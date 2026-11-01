#! /usr/bin/env bash

# enable unofficial bash strict mode
set -o errexit
set -o nounset
set -o pipefail
IFS=$'\n\t'

ALL_DB_SIZE_QUERY="select sum(pg_database_size(datname)::numeric) from pg_database;"
PG_BIN=$PG_DIR/$PG_VERSION/bin
DUMP_SIZE_COEFF=5
ERRORCOUNT=0

TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
KUBERNETES_SERVICE_PORT=${KUBERNETES_SERVICE_PORT:-443}
if [ "$KUBERNETES_SERVICE_HOST" != "${KUBERNETES_SERVICE_HOST#*[0-9].[0-9]}" ]; then
    echo "IPv4"
    K8S_API_URL=https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/api/v1
elif [ "$KUBERNETES_SERVICE_HOST" != "${KUBERNETES_SERVICE_HOST#*:[0-9a-fA-F]}" ]; then
    echo "IPv6"
    K8S_API_URL=https://[$KUBERNETES_SERVICE_HOST]:$KUBERNETES_SERVICE_PORT/api/v1
elif [ -n "$KUBERNETES_SERVICE_HOST" ]; then
    echo "Hostname"
    K8S_API_URL=https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT/api/v1
else
  echo "KUBERNETES_SERVICE_HOST was not set"
fi
echo "API Endpoint: ${K8S_API_URL}"
CERT=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt

LOGICAL_BACKUP_PROVIDER=${LOGICAL_BACKUP_PROVIDER:="s3"}
LOGICAL_BACKUP_S3_RETENTION_TIME=${LOGICAL_BACKUP_S3_RETENTION_TIME:=""}

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

function az_upload {
    PATH_TO_BACKUP=$LOGICAL_BACKUP_S3_BUCKET"/"$LOGICAL_BACKUP_S3_BUCKET_PREFIX"/"$SCOPE$LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX"/logical_backups/"$(date +%s).sql.gz

    az storage blob upload --file "$1" --account-name "$LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_NAME" --account-key "$LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_KEY" -c "$LOGICAL_BACKUP_AZURE_STORAGE_CONTAINER" -n "$PATH_TO_BACKUP"
}

function aws_delete_objects {
    local keys=("$@")
    local batch=()
    local batch_size=1000

    for ((i=0; i<${#keys[@]}; i++)); do
        batch+=("${keys[$i]}")
        if ((${#batch[@]} >= batch_size)); then
            aws_delete_objects_batch "${batch[@]}"
            batch=()
        fi
    done
    if ((${#batch[@]} > 0)); then
        aws_delete_objects_batch "${batch[@]}"
    fi
}

function aws_delete_objects_batch {
    local keys=("$@")
    local keys_json=$(printf '%s\n' "${keys[@]}" | jq -R . | jq -s .)
    local objects_json=$(jq -n --argjson keys "$keys_json" '{Objects: [$keys[] | {Key: .}], Quiet: true}')

    local args=(
      "--bucket=$LOGICAL_BACKUP_S3_BUCKET"
    )

    [[ ! -z "$LOGICAL_BACKUP_S3_ENDPOINT" ]] && args+=("--endpoint-url=$LOGICAL_BACKUP_S3_ENDPOINT")
    [[ ! -z "$LOGICAL_BACKUP_S3_REGION" ]] && args+=("--region=$LOGICAL_BACKUP_S3_REGION")

    local result
    result=$(aws s3api delete-objects "${args[@]}" --delete "$objects_json" 2>&1)
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        echo "WARNING: failed to delete some objects: $result"
    fi
}
export -f aws_delete_objects
export -f aws_delete_objects_batch

function aws_delete_outdated {
    if [[ -z "$LOGICAL_BACKUP_S3_RETENTION_TIME" ]] ; then
        echo "no retention time configured: skip cleanup of outdated backups"
        return 0
    fi

    cutoff_timestamp=$(date -d "$LOGICAL_BACKUP_S3_RETENTION_TIME ago" +%s)

    prefix=$LOGICAL_BACKUP_S3_BUCKET_PREFIX"/"$SCOPE$LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX"/logical_backups/"

    args=(
      "--output=json"
      "--prefix=$prefix"
      "--bucket=$LOGICAL_BACKUP_S3_BUCKET"
    )

    [[ ! -z "$LOGICAL_BACKUP_S3_ENDPOINT" ]] && args+=("--endpoint-url=$LOGICAL_BACKUP_S3_ENDPOINT")
    [[ ! -z "$LOGICAL_BACKUP_S3_REGION" ]] && args+=("--region=$LOGICAL_BACKUP_S3_REGION")

    : > /tmp/outdated-backups

    local continuation_token=""
    while true; do
        local result
        if [[ -n "$continuation_token" ]]; then
            result=$(aws s3api list-objects-v2 "${args[@]}" --continuation-token "$continuation_token" 2>/dev/null)
        else
            result=$(aws s3api list-objects-v2 "${args[@]}" 2>/dev/null)
        fi

        if [[ -z "$result" ]] || [[ "$result" == "{}" ]]; then
            break
        fi

        echo "$result" | jq -r '.Contents[] | select(.LastModified != null) | "\(.Key)\n\(.LastModified)"' | \
        while read -r key && read -r last_modified; do
            set +e
            file_timestamp=$(date -d "$last_modified" +%s 2>/dev/null)
            set -e
            if [[ -n "$file_timestamp" ]] && [[ "$file_timestamp" -lt "$cutoff_timestamp" ]]; then
                echo "$key" >> /tmp/outdated-backups
            fi
        done

        local is_truncated=$(echo "$result" | jq -r '.IsTruncated')
        if [[ "$is_truncated" != "true" ]]; then
            break
        fi

        continuation_token=$(echo "$result" | jq -r '.NextContinuationToken // empty')
        if [[ -z "$continuation_token" ]]; then
            break
        fi
    done

    count=$(wc -l < /tmp/outdated-backups)
    if [[ $count == 0 ]] ; then
      echo "no outdated backups to delete"
      return 0
    fi
    echo "deleting $count outdated backups older than $LOGICAL_BACKUP_S3_RETENTION_TIME"

    mapfile -t keys_array < /tmp/outdated-backups
    aws_delete_objects "${keys_array[@]}"

    echo "cleanup completed"
}

function aws_upload {
    declare -r EXPECTED_SIZE="$1"

    # mimic bucket setup from Spilo
    # to keep logical backups at the same path as WAL
    # NB: $LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX already contains the leading "/" when set by the Postgres Operator
    PATH_TO_BACKUP=s3://$LOGICAL_BACKUP_S3_BUCKET"/"$LOGICAL_BACKUP_S3_BUCKET_PREFIX"/"$SCOPE$LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX"/logical_backups/"$(date +%s).sql.gz

    args=()

    [[ "$EXPECTED_SIZE" -gt 0 ]] && args+=("--expected-size=$EXPECTED_SIZE")
    [[ ! -z "$LOGICAL_BACKUP_S3_ENDPOINT" ]] && args+=("--endpoint-url=$LOGICAL_BACKUP_S3_ENDPOINT")
    [[ ! -z "$LOGICAL_BACKUP_S3_REGION" ]] && args+=("--region=$LOGICAL_BACKUP_S3_REGION")
    [[ ! -z "$LOGICAL_BACKUP_S3_SSE" ]] && args+=("--sse=$LOGICAL_BACKUP_S3_SSE")

    aws s3 cp - "$PATH_TO_BACKUP" "${args[@]}"
}

function gcs_upload {
    PATH_TO_BACKUP=gs://$LOGICAL_BACKUP_S3_BUCKET"/"$LOGICAL_BACKUP_S3_BUCKET_PREFIX"/"$SCOPE$LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX"/logical_backups/"$(date +%s).sql.gz

    #Set local LOGICAL_GOOGLE_APPLICATION_CREDENTIALS to nothing or
    #value of LOGICAL_GOOGLE_APPLICATION_CREDENTIALS env var. Needed
    #because `set -o nounset` is globally set
    local LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS=${LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS:-}

    GSUTIL_OPTIONS=("-o" "Credentials:gs_service_key_file=$LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS")

    #If GOOGLE_APPLICATION_CREDENTIALS is not set try to get
    #creds from metadata
    if [[ -z $LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS ]]
    then
	    GSUTIL_OPTIONS[1]="GoogleCompute:service_account=default"
    fi

    gsutil "${GSUTIL_OPTIONS[@]}" cp - "$PATH_TO_BACKUP"
}

function upload {
    case $LOGICAL_BACKUP_PROVIDER in
        "gcs")
            gcs_upload
            ;;
        "s3")
            aws_upload $(($(estimate_size) / DUMP_SIZE_COEFF))
            aws_delete_outdated
            ;;
    esac
}

function get_pods {
    declare -r SELECTOR="$1"

    curl "${K8S_API_URL}/namespaces/${POD_NAMESPACE}/pods?$SELECTOR" \
      --cacert $CERT \
      -H "Authorization: Bearer ${TOKEN}" | jq .items[].status.podIP -r
}

function get_current_pod {
    curl "${K8S_API_URL}/namespaces/${POD_NAMESPACE}/pods?fieldSelector=metadata.name%3D${HOSTNAME}" \
      --cacert $CERT \
      -H "Authorization: Bearer ${TOKEN}"
}

declare -a search_strategy=(
    list_all_replica_pods_current_node
    list_all_replica_pods_any_node
    get_master_pod
)

function list_all_replica_pods_current_node {
    get_pods "labelSelector=${CLUSTER_NAME_LABEL}%3D${SCOPE},spilo-role%3Dreplica&fieldSelector=spec.nodeName%3D${CURRENT_NODENAME}" | tee | head -n 1
}

function list_all_replica_pods_any_node {
    get_pods "labelSelector=${CLUSTER_NAME_LABEL}%3D${SCOPE},spilo-role%3Dreplica" | tee | head -n 1
}

function get_master_pod {
    get_pods "labelSelector=${CLUSTER_NAME_LABEL}%3D${SCOPE},spilo-role%3Dmaster" | tee | head -n 1
}

# Wait for TCP connectivity to the target PostgreSQL pod.
# When NetworkPolicy is enforced via iptables, a newly-created pod's IP may not
# yet be present in the destination node's ingress allow lists, causing
# cross-node connections to be rejected until the next policy sync.
function wait_for_pg {
    local retries=${LOGICAL_BACKUP_CONNECT_RETRIES:-10}
    local delay=${LOGICAL_BACKUP_CONNECT_RETRY_DELAY:-2}
    local i
    for (( i=1; i<=retries; i++ )); do
        if "$PG_BIN"/pg_isready -h "$PGHOST" -p "${PGPORT:-5432}" -q 2>/dev/null; then
            return 0
        fi
        echo "waiting for $PGHOST:${PGPORT:-5432} to become reachable (attempt $i/$retries)..."
        sleep "$delay"
    done
    echo "ERROR: $PGHOST:${PGPORT:-5432} not reachable after $((retries * delay))s"
    return 1
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

wait_for_pg

set -x
if [ "$LOGICAL_BACKUP_PROVIDER" == "az" ]; then
    dump | compress > /tmp/azure-backup.sql.gz
    az_upload /tmp/azure-backup.sql.gz
else
    dump | compress | upload
    [[ ${PIPESTATUS[0]} != 0 || ${PIPESTATUS[1]} != 0 || ${PIPESTATUS[2]} != 0 ]] && (( ERRORCOUNT += 1 ))
    set +x

    exit $ERRORCOUNT
fi
