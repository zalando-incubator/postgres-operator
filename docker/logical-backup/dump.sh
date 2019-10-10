#! /usr/bin/env bash

# enable unofficial bash strict mode
set -o errexit
set -o nounset
set -o pipefail
IFS=$'\n\t'

# make script trace visible via `kubectl logs`
set -o xtrace

ALL_DB_SIZE_QUERY="select sum(pg_database_size(datname)::numeric) from pg_database;"
PG_BIN=$PG_DIR/$PG_VERSION/bin
DUMP_SIZE_COEFF=5

TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
K8S_API_URL=https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/api/v1
CERT=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt

function estimate_size {
    "$PG_BIN"/psql -tqAc "${ALL_DB_SIZE_QUERY}"
}

function dump {
    # settings are taken from the environment
    "$PG_BIN"/pg_dumpall
}

function compress {
    pigz
}

function aws_upload {
    declare -r EXPECTED_SIZE="$1"

    # mimic bucket setup from Spilo
    # to keep logical backups at the same path as WAL
    # NB: $LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX already contains the leading "/" when set by the Postgres Operator
    PATH_TO_BACKUP=s3://$LOGICAL_BACKUP_S3_BUCKET"/spilo/"$SCOPE$LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX"/logical_backups/"$(date +%s).sql.gz

    if [ -z "${AWS_ENDPOINT-}" ]; then
        ENDPOINT_OPTION=""
    else
        ENDPOINT_OPTION="--endpoint-url=$AWS_ENDPOINT"
    fi

    if [ -z "$EXPECTED_SIZE" ]; then
        EXPECTED_SIZE_OPTION=""
    else
        EXPECTED_SIZE_OPTION=(--expected-size $EXPECTED_SIZE)
    fi

    aws $ENDPOINT_OPTION s3 cp - "$PATH_TO_BACKUP" --debug ${EXPECTED_SIZE_OPTION[@]} --sse="AES256"
}

function get_pods {
    declare -r SELECTOR="$1"

    curl "${K8S_API_URL}/namespaces/${POD_NAMESPACE}/pods?$SELECTOR"        \
        --cacert $CERT                          \
        -H "Authorization: Bearer ${TOKEN}" | jq .items[].status.podIP -r
}

function get_current_pod {
    curl "${K8S_API_URL}/namespaces/${POD_NAMESPACE}/pods?fieldSelector=metadata.name%3D${HOSTNAME}" \
        --cacert $CERT   \
        -H "Authorization: Bearer ${TOKEN}"
}

declare -a search_strategy=(
    list_all_replica_pods_current_node
    list_all_replica_pods_any_node
    get_master_pod
)

function list_all_replica_pods_current_node {
    get_pods "labelSelector=version%3D${SCOPE},spilo-role%3Dreplica&fieldSelector=spec.nodeName%3D${CURRENT_NODENAME}" | head -n 1
}

function list_all_replica_pods_any_node {
    get_pods "labelSelector=version%3D${SCOPE},spilo-role%3Dreplica" | head -n 1
}

function get_master_pod {
    get_pods "labelSelector=version%3D${SCOPE},spilo-role%3Dmaster" | head -n 1
}

CURRENT_NODENAME=$(get_current_pod | jq .items[].spec.nodeName --raw-output)
export CURRENT_NODENAME

for search in "${search_strategy[@]}"; do

    PGHOST=$(eval "$search")
    export PGHOST

    if [ -n "$PGHOST" ]; then
        break
    fi

done

dump | compress | aws_upload $(($(estimate_size) / DUMP_SIZE_COEFF))
