<h1>Administrator Guide</h1>

Learn how to configure and manage the Postgres Operator in your Kubernetes (K8s)
environment.

## Minor and major version upgrade

Minor version upgrades for PostgreSQL are handled via updating the Spilo Docker
image. The operator will carry out a rolling update of Pods which includes a
switchover (planned failover) of the master to the Pod with new minor version.
The switch should usually take less than 5 seconds, still clients have to
reconnect.

### Upgrade on cloning

With [cloning](user.md#how-to-clone-an-existing-postgresql-cluster), the new
cluster manifest must have a higher `version` string than the source cluster
and will be created from a basebackup. Depending of the cluster size, downtime
in this case can be significant as writes to the database should be stopped
and all WAL files should be archived first before cloning is started.
Therefore, use cloning only to test major version upgrades and check for
compatibility of your app with to Postgres server of a higher version.

### In-place major version upgrade

Starting with Spilo 13, Postgres Operator can run an in-place major version
upgrade which is much faster than cloning. First, you need to make sure, that
the `PGVERSION` environment variable is set for the database pods. Since
`v1.6.0` the related option `enable_pgversion_env_var` is enabled by default.

In-place major version upgrades can be configured to be executed by the
operator with the `major_version_upgrade_mode` option. By default it is set
to `off` which means the cluster version will not change when increased in
the manifest. Still, a rolling update would be triggered updating the
`PGVERSION` variable. But Spilo's [`configure_spilo`](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/configure_spilo.py)
script will notice the version mismatch and start the old version again.

In this scenario the major version could then be run by a user from within the
master pod. Exec into the container and run:
```bash
python3 /scripts/inplace_upgrade.py N
```
where `N` is the number of members of your cluster (see [`numberOfInstances`](https://github.com/zalando/postgres-operator/blob/50cb5898ea715a1db7e634de928b2d16dc8cd969/manifests/minimal-postgres-manifest.yaml#L10)).
The upgrade is usually fast, well under one minute for most DBs. Note, that
changes become irrevertible once `pg_upgrade` is called. To understand the
upgrade procedure, refer to the [corresponding PR in Spilo](https://github.com/zalando/spilo/pull/488).

When `major_version_upgrade_mode` is set to `manual` the operator will run
the upgrade script for you after the manifest is updated and pods are rotated.

## CRD Validation

[CustomResourceDefinitions](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#customresourcedefinitions)
will be registered with schema validation by default when the operator is
deployed. The `OperatorConfiguration` CRD will only get created if the
`POSTGRES_OPERATOR_CONFIGURATION_OBJECT` [environment variable](../manifests/postgres-operator.yaml#L36)
in the deployment yaml is set and not empty.

When submitting manifests of [`postgresql`](../manifests/postgresql.crd.yaml) or
[`OperatorConfiguration`](../manifests/operatorconfiguration.crd.yaml) custom
resources with kubectl, validation can be bypassed with `--validate=false`. The
operator can also be configured to not register CRDs with validation on `ADD` or
`UPDATE` events. Running instances are not affected when enabling the validation
afterwards unless the manifests is not changed then. Note, that the provided CRD
manifests contain the validation for users to understand what schema is
enforced.

Once the validation is enabled it can only be disabled manually by editing or
patching the CRD manifest:

```bash
kubectl patch crd postgresqls.acid.zalan.do -p '{"spec":{"validation": null}}'
```

## Non-default cluster domain

If your cluster uses a DNS domain other than the default `cluster.local`, this
needs to be set in the operator configuration (`cluster_domain` variable). This
is used by the operator to connect to the clusters after creation.

## Namespaces

### Select the namespace to deploy to

The operator can run in a namespace other than `default`. For example, to use
the `test` namespace, run the following before deploying the operator's
manifests:

```bash
kubectl create namespace test
kubectl config set-context $(kubectl config current-context) --namespace=test
```

All subsequent `kubectl` commands will work with the `test` namespace. The
operator will run in this namespace and look up needed resources - such as its
ConfigMap - there. Please note that the namespace for service accounts and
cluster role bindings in [operator RBAC rules](../manifests/operator-service-account-rbac.yaml)
needs to be adjusted to the non-default value.

### Specify the namespace to watch

Watching a namespace for an operator means tracking requests to change Postgres
clusters in the namespace such as "increase the number of Postgres replicas to
5" and reacting to the requests, in this example by actually scaling up.

By default, the operator watches the namespace it is deployed to. You can
change this by setting the `WATCHED_NAMESPACE` var in the `env` section of the
[operator deployment](../manifests/postgres-operator.yaml) manifest or by
altering the `watched_namespace` field in the operator
[configuration](../manifests/postgresql-operator-default-configuration.yaml#L49).
In the case both are set, the env var takes the precedence. To make the
operator listen to all namespaces, explicitly set the field/env var to "`*`".

Note that for an operator to manage pods in the watched namespace, the
operator's service account (as specified in the operator deployment manifest)
has to have appropriate privileges to access the watched namespace. The
operator may not be able to function in the case it watches all namespaces but
lacks access rights to any of them (except K8s system namespaces like
`kube-system`). The reason is that for multiple namespaces operations such as
'list pods' execute at the cluster scope and fail at the first violation of
access rights.

## Operators with defined ownership of certain Postgres clusters

By default, multiple operators can only run together in one K8s cluster when
isolated into their [own namespaces](administrator.md#specify-the-namespace-to-watch).
But, it is also possible to define ownership between operator instances and
Postgres clusters running all in the same namespace or K8s cluster without
interfering.

First, define the [`CONTROLLER_ID`](../../manifests/postgres-operator.yaml#L38)
environment variable in the operator deployment manifest. Then specify the ID
in every Postgres cluster manifest you want this operator to watch using the
`"acid.zalan.do/controller"` annotation:

```yaml
apiVersion: "acid.zalan.do/v1"
kind: postgresql
metadata:
  name: demo-cluster
  annotations:
    "acid.zalan.do/controller": "second-operator"
spec:
  ...
```

Every other Postgres cluster which lacks the annotation will be ignored by this
operator. Conversely, operators without a defined `CONTROLLER_ID` will ignore
clusters with defined ownership of another operator.

## Understanding rolling update of Spilo pods

The operator logs reasons for a rolling update with the `info` level and a diff
between the old and new StatefulSet specs with the `debug` level. To benefit
from numerous escape characters in the latter log entry, view it in CLI with
`echo -e`. Note that the resultant message will contain some noise because the
`PodTemplate` used by the operator is yet to be updated with the default values
used internally in K8s.

The operator also support lazy updates of the Spilo image. That means the pod
template of a PG cluster's stateful set is updated immediately with the new
image, but no rolling update follows. This feature saves you a switchover - and
hence downtime - when you know pods are re-started later anyway, for instance
due to the node rotation. To force a rolling update, disable this mode by
setting the `enable_lazy_spilo_upgrade` to `false` in the operator configuration
and restart the operator pod. With the standard eager rolling updates the
operator checks during Sync all pods run images specified in their respective
statefulsets. The operator triggers a rolling upgrade for PG clusters that
violate this condition.

Changes in $SPILO\_CONFIGURATION under path bootstrap.dcs are ignored when
StatefulSets are being compared, if there are changes under this path, they are
applied through rest api interface and following restart of patroni instance

## Delete protection via annotations

To avoid accidental deletes of Postgres clusters the operator can check the
manifest for two existing annotations containing the cluster name and/or the
current date (in YYYY-MM-DD format). The name of the annotation keys can be
defined in the configuration. By default, they are not set which disables the
delete protection. Thus, one could choose to only go with one annotation.

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  delete_annotation_date_key: "delete-date"
  delete_annotation_name_key: "delete-clustername"
```

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  kubernetes:
    delete_annotation_date_key: "delete-date"
    delete_annotation_name_key: "delete-clustername"
```

Now, every cluster manifest must contain the configured annotation keys to
trigger the delete process when running `kubectl delete pg`. Note, that the
`Postgresql` resource would still get deleted as K8s' API server does not
block it. Only the operator logs will tell, that the delete criteria wasn't
met.

**cluster manifest**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: postgresql
metadata:
  name: demo-cluster
  annotations:
    delete-date: "2020-08-31"
    delete-clustername: "demo-cluster"
spec:
  ...
```

In case, the resource has been deleted accidentally or the annotations were
simply forgotten, it's safe to recreate the cluster with `kubectl create`.
Existing Postgres cluster are not replaced by the operator. But, as the
original cluster still exists the status will show `CreateFailed` at first.
On the next sync event it should change to `Running`. However, as it is in
fact a new resource for K8s, the UID will differ which can trigger a rolling
update of the pods because the UID is used as part of backup path to S3.

## Role-based access control for the operator

The manifest [`operator-service-account-rbac.yaml`](../manifests/operator-service-account-rbac.yaml)
defines the service account, cluster roles and bindings needed for the operator
to function under access control restrictions. The file also includes a cluster
role `postgres-pod` with privileges for Patroni to watch and manage pods and
endpoints. To deploy the operator with this RBAC policies use:

```bash
kubectl create -f manifests/configmap.yaml
kubectl create -f manifests/operator-service-account-rbac.yaml
kubectl create -f manifests/postgres-operator.yaml
kubectl create -f manifests/minimal-postgres-manifest.yaml
```

### Namespaced service account and role binding

For each namespace the operator watches it creates (or reads) a service account
and role binding to be used by the Postgres Pods. The service account is bound
to the `postgres-pod` cluster role. The name and definitions of these resources
can be [configured](reference/operator_parameters.md#kubernetes-resources).
Note, that the operator performs **no** further syncing of namespaced service
accounts and role bindings.

### Give K8s users access to create/list `postgresqls`

By default `postgresql` custom resources can only be listed and changed by
cluster admins. To allow read and/or write access to other human users apply
the `user-facing-clusterrole` manifest:

```bash
kubectl create -f manifests/user-facing-clusterroles.yaml
```

It creates zalando-postgres-operator:user:view, :edit and :admin clusterroles
that are aggregated into the K8s [default roles](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#default-roles-and-role-bindings).

## Use taints and tolerations for dedicated PostgreSQL nodes

To ensure Postgres pods are running on nodes without any other application pods,
you can use [taints and tolerations](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/)
and configure the required toleration in the operator configuration.

As an example you can set following node taint:

```bash
kubectl taint nodes <nodeName> postgres=:NoSchedule
```

And configure the toleration for the Postgres pods by adding following line
to the ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  toleration: "key:postgres,operator:Exists,effect:NoSchedule"
```

For an OperatorConfiguration resource the toleration should be defined like
this:

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-configuration
configuration:
  kubernetes:
    toleration:
      postgres: "key:postgres,operator:Exists,effect:NoSchedule"
```

Note that the K8s version 1.13 brings [taint-based eviction](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions)
to the beta stage and enables it by default. Postgres pods by default receive
tolerations for `unreachable` and `noExecute` taints with the timeout of `5m`.
Depending on your setup, you may want to adjust these parameters to prevent
master pods from being evicted by the K8s runtime. To prevent eviction
completely, specify the toleration by leaving out the `tolerationSeconds` value
(similar to how Kubernetes' own DaemonSets are configured)

## Enable pod anti affinity

To ensure Postgres pods are running on different topologies, you can use
[pod anti affinity](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/)
and configure the required topology in the operator configuration.

Enable pod anti affinity by adding following line to the operator ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  enable_pod_antiaffinity: "true"
```

Likewise, when using an OperatorConfiguration resource add:

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-configuration
configuration:
  kubernetes:
    enable_pod_antiaffinity: true
```

By default the topology key for the pod anti affinity is set to
`kubernetes.io/hostname`, you can set another topology key e.g.
`failure-domain.beta.kubernetes.io/zone`. See [built-in node labels](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#interlude-built-in-node-labels) for available topology keys.

## Pod Disruption Budget

By default the operator uses a PodDisruptionBudget (PDB) to protect the cluster
from voluntarily disruptions and hence unwanted DB downtime. The `MinAvailable`
parameter of the PDB is set to `1` which prevents killing masters in single-node
clusters and/or the last remaining running instance in a multi-node cluster.

The PDB is only relaxed in two scenarios:

* If a cluster is scaled down to `0` instances (e.g. for draining nodes)
* If the PDB is disabled in the configuration (`enable_pod_disruption_budget`)

The PDB is still in place having `MinAvailable` set to `0`. If enabled it will
be automatically set to `1` on scale up. Disabling PDBs helps avoiding blocking
Kubernetes upgrades in managed K8s environments at the cost of prolonged DB
downtime. See PR [#384](https://github.com/zalando/postgres-operator/pull/384)
for the use case.

## Add cluster-specific labels

In some cases, you might want to add `labels` that are specific to a given
Postgres cluster, in order to identify its child objects. The typical use case
is to add labels that identifies the `Pods` created by the operator, in order
to implement fine-controlled `NetworkPolicies`.

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  inherited_labels: application,environment
```

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  kubernetes:
    inherited_labels:
    - application
    - environment
```

**cluster manifest**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: postgresql
metadata:
  name: demo-cluster
  labels:
    application: my-app
    environment: demo
spec:
  ...
```

**network policy**

```yaml
kind: NetworkPolicy
apiVersion: networking.k8s.io/v1
metadata:
  name: netpol-example
spec:
  podSelector:
    matchLabels:
      application: my-app
      environment: demo
```


## Custom Pod Environment Variables

It is possible to configure a ConfigMap as well as a Secret which are used by
the Postgres pods as an additional provider for environment variables. One use
case is a customized Spilo image configured by extra environment variables.
Another case could be to provide custom cloud provider or backup settings.

In general the Operator will give preference to the globally configured
variables, to not have the custom ones interfere with core functionality.
Variables with the 'WAL_' and 'LOG_' prefix can be overwritten though, to
allow backup and log shipping to be specified differently.

### Via ConfigMap

The ConfigMap with the additional settings is referenced in the operator's
main configuration. A namespace can be specified along with the name. If left
out, the configured default namespace of your K8s client will be used and if
the ConfigMap is not found there, the Postgres cluster's namespace is taken
when different:

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  # referencing config map with custom settings
  pod_environment_configmap: default/postgres-pod-config
```

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  kubernetes:
    # referencing config map with custom settings
    pod_environment_configmap: default/postgres-pod-config
```

**referenced ConfigMap `postgres-pod-config`**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-pod-config
  namespace: default
data:
  MY_CUSTOM_VAR: value
```

The key-value pairs of the ConfigMap are then added as environment variables
to the Postgres StatefulSet/pods.

### Via Secret

The Secret with the additional variables is referenced in the operator's main
configuration. To protect the values of the secret from being exposed in the
pod spec they are each referenced as SecretKeyRef. This does not allow for the
secret to be in a different namespace as the pods though

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  # referencing secret with custom environment variables
  pod_environment_secret: postgres-pod-secrets
```

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  kubernetes:
    # referencing secret with custom environment variables
    pod_environment_secret: postgres-pod-secrets
```

**referenced Secret `postgres-pod-secrets`**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-pod-secrets
  namespace: default
data:
  MY_CUSTOM_VAR: dmFsdWU=
```

The key-value pairs of the Secret are all accessible as environment variables
to the Postgres StatefulSet/pods.

## Limiting the number of min and max instances in clusters

As a preventive measure, one can restrict the minimum and the maximum number of
instances permitted by each Postgres cluster managed by the operator. If either
`min_instances` or `max_instances` is set to a non-zero value, the operator may
adjust the number of instances specified in the cluster manifest to match
either the min or the max boundary. For instance, of a cluster manifest has 1
instance and the `min_instances` is set to 3, the cluster will be created with
3 instances. By default, both parameters are set to `-1`.

## Load balancers and allowed IP ranges

For any Postgres/Spilo cluster, the operator creates two separate K8s
services: one for the master pod and one for replica pods. To expose these
services to an outer network, one can attach load balancers to them by setting
`enableMasterLoadBalancer` and/or `enableReplicaLoadBalancer` to `true` in the
cluster manifest. In the case any of these variables are omitted from the
manifest, the operator configuration settings `enable_master_load_balancer` and
`enable_replica_load_balancer` apply. Note that the operator settings affect
all Postgresql services running in all namespaces watched by the operator.
If load balancing is enabled two default annotations will be applied to its
services:

- `external-dns.alpha.kubernetes.io/hostname` with the value defined by the
  operator configs `master_dns_name_format` and `replica_dns_name_format`.
  This value can't be overwritten. If any changing in its value is needed, it
  MUST be done changing the DNS format operator config parameters; and
- `service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout` with
  a default value of "3600". This value can be overwritten with the operator
  config parameter `custom_service_annotations` or the  cluster parameter
  `serviceAnnotations`.

To limit the range of IP addresses that can reach a load balancer, specify the
desired ranges in the `allowedSourceRanges` field (applies to both master and
replica load balancers). To prevent exposing load balancers to the entire
Internet, this field is set at cluster creation time to `127.0.0.1/32` unless
overwritten explicitly. If you want to revoke all IP ranges from an existing
cluster, please set the `allowedSourceRanges` field to `127.0.0.1/32` or to an
empty sequence `[]`. Setting the field to `null` or omitting it entirely may
lead to K8s removing this field from the manifest due to its
[handling of null fields](https://kubernetes.io/docs/concepts/overview/object-management-kubectl/declarative-config/#how-apply-calculates-differences-and-merges-changes).
Then the resultant manifest will not contain the necessary change, and the
operator will respectively do nothing with the existing source ranges.

## Running periodic 'autorepair' scans of K8s objects

The Postgres Operator periodically scans all K8s objects belonging to each
cluster and repairs all discrepancies between them and the definitions generated
from the current cluster manifest. There are two types of scans:

* `sync scan`, running every `resync_period` seconds for every cluster

* `repair scan`, coming every `repair_period` only for those clusters that
didn't report success as a result of the last operation applied to them.

## Postgres roles supported by the operator

The operator is capable of maintaining roles of multiple kinds within a
Postgres database cluster:

* **System roles** are roles necessary for the proper work of Postgres itself
such as a replication role or the initial superuser role. The operator delegates
creating such roles to Patroni and only establishes relevant secrets.

* **Infrastructure roles** are roles for processes originating from external
systems, e.g. monitoring robots. The operator creates such roles in all Postgres
clusters it manages, assuming that K8s secrets with the relevant
credentials exist beforehand.

* **Per-cluster robot users** are also roles for processes originating from
external systems but defined for an individual Postgres cluster in its manifest.
A typical example is a role for connections from an application that uses the
database.

* **Human users** originate from the [Teams API](user.md#teams-api-roles) that
returns a list of the team members given a team id. The operator differentiates
between (a) product teams that own a particular Postgres cluster and are granted
admin rights to maintain it, (b) Postgres superuser teams that get superuser
access to all Postgres databases running in a K8s cluster for the purposes of
maintaining and troubleshooting, and (c) additional teams, superuser teams or
members associated with the owning team. The latter is managed via the
[PostgresTeam CRD](user.md#additional-teams-and-members-per-cluster).

## Access to cloud resources from clusters in non-cloud environment

To access cloud resources like S3 from a cluster on bare metal you can use
`additional_secret_mount` and `additional_secret_mount_path` configuration
parameters. The cloud credentials will be provisioned in the Postgres containers
by mounting an additional volume from the given secret to database pods. They
can then be accessed over the configured mount path. Via
[Custom Pod Environment Variables](#custom-pod-environment-variables) you can
point different cloud SDK's (AWS, GCP etc.) to this mounted secret, e.g. to
access cloud resources for uploading logs etc.

A secret can be pre-provisioned in different ways:

* Generic secret created via `kubectl create secret generic some-cloud-creds --from-file=some-cloud-credentials-file.json`
* Automatically provisioned via a custom K8s controller like
  [kube-aws-iam-controller](https://github.com/mikkeloscar/kube-aws-iam-controller)

## WAL archiving and physical basebackups

Spilo is shipped with [WAL-E](https://github.com/wal-e/wal-e) and its successor
[WAL-G](https://github.com/wal-g/wal-g) to perform WAL archiving. By default,
WAL-E is used for backups because it is more battle-tested. In addition to the
continuous backup stream WAL-E/G pushes a physical base backup every night and
01:00 am UTC.

These are the pre-configured settings in the docker image:
```bash
BACKUP_NUM_TO_RETAIN: 5
BACKUP_SCHEDULE:      '00 01 * * *'
USE_WALG_BACKUP:      false (true for Azure and SSH)
USE_WALG_RESTORE:     false (true for S3, Azure and SSH)
```

Within Postgres you can check the pre-configured commands for archiving and
restoring WAL files. You can find the log files to the respective commands
under `$HOME/pgdata/pgroot/pg_log/postgres-?.log`.

```bash
archive_command:  `envdir "{WALE_ENV_DIR}" {WALE_BINARY} wal-push "%p"`
restore_command:  `envdir "{{WALE_ENV_DIR}}" /scripts/restore_command.sh "%f" "%p"`
```

You can produce a basebackup manually with the following command and check
if it ends up in your specified WAL backup path:

```bash
envdir "/run/etc/wal-e.d/env" /scripts/postgres_backup.sh "/home/postgres/pgdata/pgroot/data"
```

Depending on the cloud storage provider different [environment variables](https://github.com/zalando/spilo/blob/master/ENVIRONMENT.rst)
have to be set for Spilo. Not all of them are generated automatically by the
operator by changing its configuration. In this case you have to use an
[extra configmap or secret](#custom-pod-environment-variables).

### Using AWS S3 or compliant services

When using AWS you have to reference the S3 backup path, the IAM role and the
AWS region in the configuration.

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  aws_region: eu-central-1
  kube_iam_role: postgres-pod-role
  wal_s3_bucket: your-backup-path
```

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  aws_or_gcp:
    aws_region: eu-central-1
    kube_iam_role: postgres-pod-role
    wal_s3_bucket: your-backup-path
```

The referenced IAM role should contain the following privileges to make sure
Postgres can send compressed WAL files to the given S3 bucket:

```yaml
  PostgresPodRole:
    Type: "AWS::IAM::Role"
    Properties:
      RoleName: "postgres-pod-role"
      Path: "/"
      Policies:
        - PolicyName: "SpiloS3Access"
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Action: "s3:*"
                Effect: "Allow"
                Resource:
                  - "arn:aws:s3:::your-backup-path"
                  - "arn:aws:s3:::your-backup-path/*"
```

This should produce the following settings for the essential environment
variables:

```bash
AWS_ENDPOINT='https://s3.eu-central-1.amazonaws.com:443'
WALE_S3_ENDPOINT='https+path://s3.eu-central-1.amazonaws.com:443'
WALE_S3_PREFIX=$WAL_S3_BUCKET/spilo/{WAL_BUCKET_SCOPE_PREFIX}{SCOPE}{WAL_BUCKET_SCOPE_SUFFIX}/wal/{PGVERSION}
```

If the prefix is not specified Spilo will generate it from `WAL_S3_BUCKET`.
When the `AWS_REGION` is set `AWS_ENDPOINT` and `WALE_S3_ENDPOINT` are
generated automatically. `WALG_S3_PREFIX` is identical to `WALE_S3_PREFIX`.
`SCOPE` is the Postgres cluster name.

### Google Cloud Platform setup

To configure the operator on GCP these prerequisites that are needed:

* A service account with the proper IAM setup to access the GCS bucket for the WAL-E logs
* The credentials file for the service account.

The configuration parameters that we will be using are:

* `additional_secret_mount`
* `additional_secret_mount_path`
* `gcp_credentials`
* `wal_gs_bucket`

1. Generate the K8s secret resource that will contain your service account's
credentials. It's highly recommended to use a service account and limit its
scope to just the WAL-E bucket.
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: psql-wale-creds
  namespace: default
type: Opaque
stringData:
  key.json: |-
    <GCP .json credentials>
```

2. Setup your operator configuration values. With the `psql-wale-creds`
resource applied to your cluster, ensure that the operator's configuration
is set up like the following:
```yml
...
aws_or_gcp:
  additional_secret_mount: "pgsql-wale-creds"
  additional_secret_mount_path: "/var/secrets/google"  # or where ever you want to mount the file
  # aws_region: eu-central-1
  # kube_iam_role: ""
  # log_s3_bucket: ""
  # wal_s3_bucket: ""
  wal_gs_bucket: "postgres-backups-bucket-28302F2"  # name of bucket on where to save the WAL-E logs
  gcp_credentials: "/var/secrets/google/key.json"  # combination of the mount path & key in the K8s resource. (i.e. key.json)
...
```

3. Setup pod environment configmap that instructs the operator to use WAL-G,
instead of WAL-E, for backup and restore.
```yml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pod-env-overrides
  namespace: postgres-operator-system
data:
  # Any env variable used by spilo can be added
  USE_WALG_BACKUP: "true"
  USE_WALG_RESTORE: "true"
  CLONE_USE_WALG_RESTORE: "true"
```

4. Then provide this configmap in postgres-operator settings:
```yml
...
# namespaced name of the ConfigMap with environment variables to populate on every pod
pod_environment_configmap: "postgres-operator-system/pod-env-overrides"
...
```

### Azure Storage Account Setup

To configure phyiscal backups and WAL to be sent to an azure storage account perform the
following steps.

1. Generate the K8s secret resource that will contain your storage account
credentials.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: psql-wale-creds
  namespace: postgres-operator-system
type: Opaque
stringData:
  AZURE_STORAGE_ACCOUNT: 'mystorageaccount'
  AZURE_STORAGE_ACCESS_KEY: '....'
```

3.  Setup pod environment configmap that instructs the operator to use WAL-G,
instead of WAL-E, for backup and restore.
```yml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pod-env-overrides
  namespace: postgres-operator-system
data:
  # Any env variable used by spilo can be added
  USE_WALG_BACKUP: "true"
  USE_WALG_RESTORE: "true"
  CLONE_USE_WALG_RESTORE: "true"
  WALG_AZ_PREFIX: azure://container
```

4. Then provide this configmap in postgres-operator settings:
```yml
...
# namespaced name of the ConfigMap with environment variables to populate on every pod
pod_environment_configmap: "postgres-operator-system/pod-env-overrides"
pod_environment_secret: "postgres-operator-system/psql-wale-creds"
...
```


### Restoring physical backups

If cluster members have to be (re)initialized restoring physical backups
happens automatically either from the backup location or by running
[pg_basebackup](https://www.postgresql.org/docs/13/app-pgbasebackup.html)
on one of the other running instances (preferably replicas if they do not lag
behind). You can test restoring backups by [cloning](user.md#how-to-clone-an-existing-postgresql-cluster)
clusters.

## Logical backups

The operator can manage K8s cron jobs to run logical backups (SQL dumps) of
Postgres clusters. The cron job periodically spawns a batch job that runs a
single pod. The backup script within this pod's container can connect to a DB
for a logical backup. The operator updates cron jobs during Sync if the job
schedule changes; the job name acts as the job identifier. These jobs are to
be enabled for each individual Postgres cluster by updating the manifest:

```yaml
apiVersion: "acid.zalan.do/v1"
kind: postgresql
metadata:
  name: demo-cluster
spec:
  enableLogicalBackup: true
```

There a few things to consider when using logical backups:

1. Logical backups should not be seen as a proper alternative to basebackups
and WAL archiving which are described above. At the moment, the operator cannot
restore logical backups automatically and you do not get point-in-time recovery
but only snapshots of your data. In its current state, see logical backups as a
way to quickly create SQL dumps that you can easily restore in an empty test
cluster.

2. The [example image](../docker/logical-backup/Dockerfile) implements the backup
via `pg_dumpall` and upload of compressed and encrypted results to an S3 bucket.
`pg_dumpall` requires a `superuser` access to a DB and runs on the replica when
possible.

3. Due to the [limitation of K8s cron jobs](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-job-limitations)
it is highly advisable to set up additional monitoring for this feature; such
monitoring is outside of the scope of operator responsibilities.

4. The operator does not remove old backups.

5. You may use your own image by overwriting the relevant field in the operator
configuration. Any such image must ensure the logical backup is able to finish
[in presence of pod restarts](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#handling-pod-and-container-failures)
and [simultaneous invocations](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-job-limitations)
of the backup cron job.

6. For that feature to work, your RBAC policy must enable operations on the
`cronjobs` resource from the `batch` API group for the operator service account.
See [example RBAC](../manifests/operator-service-account-rbac.yaml)

## Sidecars for Postgres clusters

A list of sidecars is added to each cluster created by the operator. The default
is empty.

```yaml
kind: OperatorConfiguration
configuration:
  sidecars:
  - image: image:123
    name: global-sidecar
    ports:
    - containerPort: 80
      protocol: TCP
    volumeMounts:
    - mountPath: /custom-pgdata-mountpoint
      name: pgdata
  - ...
```

In addition to any environment variables you specify, the following environment
variables are always passed to sidecars:

  - `POD_NAME` - field reference to `metadata.name`
  - `POD_NAMESPACE` - field reference to `metadata.namespace`
  - `POSTGRES_USER` - the superuser that can be used to connect to the database
  - `POSTGRES_PASSWORD` - the password for the superuser

## Setting up the Postgres Operator UI

Since the v1.2 release the Postgres Operator is shipped with a browser-based
configuration user interface (UI) that simplifies managing Postgres clusters
with the operator.

### Building the UI image

The UI runs with Node.js and comes with it's own Docker
image. However, installing Node.js to build the operator UI is not required. It
is handled via Docker containers when running:

```bash
make docker
```

### Configure endpoints and options

The UI talks to the K8s API server as well as the Postgres Operator [REST API](developer.md#debugging-the-operator).
K8s API server URLs are loaded from the machine's kubeconfig environment by
default. Alternatively, a list can also be passed when starting the Python
application with the `--cluster` option.

The Operator API endpoint can be configured via the `OPERATOR_API_URL`
environment variables in the [deployment manifest](../ui/manifests/deployment.yaml#L40).
You can also expose the operator API through a [service](../manifests/api-service.yaml).
Some displayed options can be disabled from UI using simple flags under the
`OPERATOR_UI_CONFIG` field in the deployment.

### Deploy the UI on K8s

Now, apply all manifests from the `ui/manifests` folder to deploy the Postgres
Operator UI on K8s. Replace the image tag in the deployment manifest if you
want to test the image you've built with `make docker`. Make sure the pods for
the operator and the UI are both running.

```bash
sed -e "s/\(image\:.*\:\).*$/\1$TAG/" manifests/deployment.yaml | kubectl apply -f manifests/
kubectl get all -l application=postgres-operator-ui
```

### Local testing

For local testing you need to apply K8s proxying and operator pod port
forwarding so that the UI can talk to the K8s and Postgres Operator REST API.
The Ingress resource is not needed. You can use the provided `run_local.sh`
script for this. Make sure that:

* Python dependencies are installed on your machine
* the K8s API server URL is set for kubectl commands, e.g. for minikube it would usually be `https://192.168.99.100:8443`.
* the pod label selectors for port forwarding are correct

When testing with minikube you have to build the image in its docker environment
(running `make docker` doesn't do it for you). From the `ui` directory execute:

```bash
# compile and build operator UI
make docker

# build in image in minikube docker env
eval $(minikube docker-env)
docker build -t registry.opensource.zalan.do/acid/postgres-operator-ui:v1.6.3 .

# apply UI manifests next to a running Postgres Operator
kubectl apply -f manifests/

# install python dependencies to run UI locally
pip3 install -r requirements
./run_local.sh
```
