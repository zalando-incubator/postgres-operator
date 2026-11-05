<h1>Administrator Guide</h1>

Learn how to configure and manage the Postgres Operator in your Kubernetes (K8s)
environment.

## CRD registration and validation

On startup, the operator will try to register the necessary
[CustomResourceDefinitions](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#customresourcedefinitions)
`Postgresql` and `OperatorConfiguration`. The latter will only get created if
the `POSTGRES_OPERATOR_CONFIGURATION_OBJECT` [environment variable](https://github.com/zalando/postgres-operator/blob/master/manifests/postgres-operator.yaml#L36)
is set in the deployment yaml and is not empty. If the CRDs already exists they
will only be patched. If you do not wish the operator to create or update the
CRDs set `enable_crd_registration` config option to `false`.

CRDs are defined with a `openAPIV3Schema` structural schema against which new
manifests of [`postgresql`](https://github.com/zalando/postgres-operator/blob/master/manifests/postgresql.crd.yaml) or [`OperatorConfiguration`](https://github.com/zalando/postgres-operator/blob/master/manifests/operatorconfiguration.crd.yaml)
resources will be validated. On creation you can bypass the validation with
`kubectl create --validate=false`.

By default, the operator will register the CRDs in the `all` category so
that resources are listed on `kubectl get all` commands. The `crd_categories`
config option allows for customization of categories.

## Upgrading the operator

The Postgres Operator is upgraded by changing the docker image within the
deployment. Before doing so, it is recommended to check the release notes
for new configuration options or changed behavior you might want to reflect
in the ConfigMap or config CRD. E.g. a new feature might get introduced which
is enabled or disabled by default and you want to change it to the opposite
with the corresponding flag option.

When using helm, be aware that installing the new chart will not update the
`Postgresql` and `OperatorConfiguration` CRD. Make sure to update them before
with the provided manifests in the `crds` folder. Otherwise, you might face
errors about new Postgres manifest or configuration options being unknown
to the CRD schema validation.

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
operator with the `major_version_upgrade_mode` option. By default, it is
enabled (mode: `manual`). In any case, altering the version in the manifest
will update the desired `PGVERSION`. If `maintenanceWindows` are configured,
major-version-related pod rotation is deferred until the next maintenance
window. Without maintenance windows, the operator will trigger a rolling
update of pods to apply the new `PGVERSION`.
Spilo's [`configure_spilo`](https://github.com/zalando/spilo/blob/master/postgres-appliance/scripts/configure_spilo.py)
script will notice the version mismatch but start the current version again.

Next, the operator would call an updage script inside Spilo. When automatic
upgrades are disabled (mode: `off`) the upgrade could still be run by a user
from within the primary pod. This gives you full control about the point in
time when the upgrade can be started (check also maintenance windows below).
Exec into the container and run:
```bash
python3 /scripts/inplace_upgrade.py N
```
where `N` is the number of members of your cluster (see [`numberOfInstances`](https://github.com/zalando/postgres-operator/blob/50cb5898ea715a1db7e634de928b2d16dc8cd969/manifests/minimal-postgres-manifest.yaml#L10)).
The upgrade is usually fast, well under one minute for most DBs. Note, that
changes become irrevertible once `pg_upgrade` is called. To understand the
upgrade procedure, refer to the [corresponding PR in Spilo](https://github.com/zalando/spilo/pull/488).

When `major_version_upgrade_mode` is set to `full` the operator will compare
the version in the manifest with the configured `minimal_major_version`. If it
is lower the operator would start an automatic upgrade as described above. The
configured `major_target_version` will be used as the new version. This option
can be useful if you have to get rid of outdated major versions in your fleet.
Please note, that the operator does not patch the version in the manifest.
Thus, the `full` mode can create drift between desired and actual state.

### Upgrade during maintenance windows

When `maintenanceWindows` are defined in the Postgres manifest or in the global
config the operator will trigger major-version-related pod rotation and the
major version upgrade only during these periods. Make sure they are at least
twice as long as your configured `resync_period` to guarantee that operator
actions can be triggered.

### Upgrade annotations

When an upgrade is executed, the operator sets an annotation in the PostgreSQL
resource, either `last-major-upgrade-success` if the upgrade succeeds, or
`last-major-upgrade-failure` if it fails. The value of the annotation is a
timestamp indicating when the upgrade occurred.

If a PostgreSQL resource contains a failure annotation, the operator will not
attempt to retry the upgrade during a sync event. To remove the failure
annotation, you can revert the PostgreSQL version back to the current version.
This action will trigger the removal of the failure annotation.

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
cluster role bindings in [operator RBAC rules](https://github.com/zalando/postgres-operator/blob/master/manifests/operator-service-account-rbac.yaml)
needs to be adjusted to the non-default value.

### Specify the namespace to watch

Watching a namespace for an operator means tracking requests to change Postgres
clusters in the namespace such as "increase the number of Postgres replicas to
5" and reacting to the requests, in this example by actually scaling up.

By default, the operator watches the namespace it is deployed to. You can
change this by setting the `WATCHED_NAMESPACE` var in the `env` section of the
[operator deployment](https://github.com/zalando/postgres-operator/blob/master/manifests/postgres-operator.yaml) manifest or by
altering the `watched_namespace` field in the operator
[configuration](https://github.com/zalando/postgres-operator/blob/master/manifests/postgresql-operator-default-configuration.yaml#L49).
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

First, define the [`CONTROLLER_ID`](https://github.com/zalando/postgres-operator/blob/master/manifests/postgres-operator.yaml#L38)
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

The StatefulSet is replaced if the following properties change:

- annotations
- volumeClaimTemplates
- template volumes

The StatefulSet is replaced and a rolling updates is triggered if the following
properties differ between the old and new state:

- container name, ports, image, resources, env, envFrom, securityContext and volumeMounts
- template labels, annotations, service account, securityContext, affinity, priority class and termination grace period

Note that, changes in `SPILO_CONFIGURATION` env variable under `bootstrap.dcs`
path are ignored for the diff. They will be applied through Patroni's rest api
interface, following a restart of all instances.

The operator also supports lazy updates of the Spilo image. In this case the
StatefulSet is updated without immediately restarting its pods. When maintenance
windows are enabled and defined, the rolling update is delayed until the next
maintenance window. If maintenance windows are disabled or none are defined,
the pods continue running the old image until they are recreated for another
reason, such as a Kubernetes node rotation. To force an immediate rolling
update, disable this mode by setting `enable_lazy_spilo_upgrade` to `false` in
the operator configuration and restart the operator pod.

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
`Postgresql` resource would still get deleted because the operator does not
instruct K8s' API server to block it. Only the operator logs will tell, that
the delete criteria was not met.

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
Existing Postgres cluster are not replaced by the operator. But, when the
original cluster still exists the status will be `CreateFailed` at first. On
the next sync event it should change to `Running`. However, because it is in
fact a new resource for K8s, the UID and therefore, the backup path to S3,
will differ and trigger a rolling update of the pods.

## Owner References and Finalizers

The Postgres Operator can set [owner references](https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/) to most of a cluster's child resources to improve
monitoring with GitOps tools and enable cascading deletes. There are three
exceptions:

* Persistent Volume Claims, because they are handled by the [PV Reclaim Policy]https://kubernetes.io/docs/tasks/administer-cluster/change-pv-reclaim-policy/ of the Stateful Set
* Cross-namespace secrets, because owner references are not allowed across namespaces by design
* User-credential secrets when [`enable_secrets_deletion`](reference/operator_parameters.md#enable_secrets_deletion) is `false`, so Kubernetes garbage collection does not cascade-delete them after the Postgresql resource is removed (the `enable_secrets_deletion` flag alone only suppresses the operator's own delete path, not K8s GC)

The operator would clean these resources up with its regular delete loop
unless they got synced correctly. If for some reason the initial cluster sync
fails, e.g. after a cluster creation or operator restart, a deletion of the
cluster manifest might leave orphaned resources behind which the user has to
clean up manually.

Another option is to enable finalizers which first ensures the deletion of all
child resources before the cluster manifest gets removed. There is a trade-off
though: The deletion is only performed after the next two operator SYNC cycles
with the first one setting a `deletionTimestamp` and the latter reacting to it.
The final removal of the custom resource will add a DELETE event to the worker
queue but the child resources are already gone at this point. If you do not
desire this behavior consider enabling owner references instead.

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  enable_finalizers: "false"
  enable_owner_references: "true"
```

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  kubernetes:
    enable_finalizers: false
    enable_owner_references: true
```

:warning: Please note, both options are disabled by default. When enabling owner
references the operator cannot block cascading deletes, even when the [delete protection annotations](administrator.md#delete-protection-via-annotations)
are in place. You would need an K8s admission controller that blocks the actual
`kubectl delete` API call e.g. based on existing annotations.

## Role-based access control for the operator

The manifest [`operator-service-account-rbac.yaml`](https://github.com/zalando/postgres-operator/blob/master/manifests/operator-service-account-rbac.yaml)
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

When installing with the plain manifests, `postgresql` custom resources can only
be listed and changed by cluster admins. To allow read and/or write access to
other human users apply the `user-facing-clusterrole` manifest:

```bash
kubectl create -f manifests/user-facing-clusterroles.yaml
```

It creates zalando-postgres-operator:users:view, :edit and :admin clusterroles
that are aggregated into the K8s [default roles](https://kubernetes.io/docs/reference/access-authn-authz/rbac/#default-roles-and-role-bindings).
The view role grants read access to `postgresqls`, `postgresteams`,
`operatorconfigurations` and `fabriceventstreams`. The edit role can change
`postgresqls` and `fabriceventstreams`, the admin role additionally
`postgresteams` and `operatorconfigurations`, because these change
operator-wide behaviour.

The Helm chart creates these clusterroles by default. Set
`rbac.createAggregateClusterRoles: false` to disable them.

## Password rotation in K8s secrets

The operator regularly updates credentials in the K8s secrets if the
`enable_password_rotation` option is set to `true` in the configuration.
It happens only for `LOGIN` roles with an associated secret (manifest roles,
default users from `preparedDatabases`). Furthermore, there are the following
exceptions:

1. Infrastructure role secrets since rotation should happen by the infrastructure.
2. Team API roles that connect via OAuth2 and JWT token (no secrets to these roles anyway).
3. Database owners since ownership on database objects can not be inherited.
4. System users such as `postgres`, `standby` and `pooler` user.

The interval of days can be set with `password_rotation_interval` (default
`90` = 90 days, minimum 1). On each rotation the user name and password values
are replaced in the K8s secret. They belong to a newly created user named after
the original role plus rotation date in YYMMDD format. All privileges are
inherited meaning that migration scripts should still grant and revoke rights
against the original role. The timestamp of the next rotation (in RFC 3339
format, UTC timezone) is written to the secret as well. Note, if the rotation
interval is decreased it is reflected in the secrets only if the next rotation
date is more days away than the new length of the interval.

Pods still using the previous secret values which they keep in memory continue
to connect to the database since the password of the corresponding user is not
replaced. However, a retention policy can be configured for users created by
the password rotation feature with `password_rotation_user_retention`. The
operator will ensure that this period is at least twice as long as the
configured rotation interval, hence the default of `180` = 180 days. When
the creation date of a rotated user is older than the retention period it
might not get removed immediately. Only on the next user rotation it is checked
if users can get removed. Therefore, you might want to configure the retention
to be a multiple of the rotation interval.

### Password rotation for single users

From the configuration, password rotation is enabled for all secrets with the
mentioned exceptions. If you wish to first test rotation for a single user (or
just have it enabled only for a few secrets) you can specify it in the cluster
manifest. The rotation and retention intervals can only be configured globally.

```
spec:
  usersWithSecretRotation:
  - foo_user
  - bar_reader_user
```

### Password replacement without extra users

For some use cases where the secret is only used rarely - think of a `flyway`
user running a migration script on pod start - we do not need to create extra
database users but can replace only the password in the K8s secret. This type
of rotation cannot be configured globally but specified in the cluster
manifest:

```
spec:
  usersWithInPlaceSecretRotation:
  - flyway
  - bar_owner_user
```

This would be the recommended option to enable rotation in secrets of database
owners, but only if they are not used as application users for regular read
and write operations.

### Ignore rotation for certain users

If you wish to globally enable password rotation but need certain users to
opt out from it there are two ways. First, you can remove the user from the
manifest's `users` section. The corresponding secret to this user will no
longer be synced by the operator then.

Secondly, if you want the operator to continue syncing the secret (e.g. to
recreate if it got accidentally removed) but cannot allow it being rotated,
add the user to the following list in your manifest:

```
spec:
  usersIgnoringSecretRotation:
  - bar_user
```

### Turning off password rotation

When password rotation is turned off again the operator will check if the
`username` value in the secret matches the original username and replace it
with the latter. A new password is assigned and the `nextRotation` field is
cleared. A final lookup for child (rotation) users to be removed is done but
they will only be dropped if the retention policy allows for it. This is to
avoid sudden connection issues in pods which still use credentials of these
users in memory. You have to remove these child users manually or re-enable
password rotation with smaller interval so they get cleaned up.

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

## Node readiness labels

The operator can watch on certain node labels to detect e.g. the start of a
Kubernetes cluster upgrade procedure and move master pods off the nodes to be
decommissioned. Key-value pairs for these node readiness labels can be
specified in the configuration (option name is in singular form):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  node_readiness_label: "status1:ready,status2:ready"
```

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-configuration
configuration:
  kubernetes:
    node_readiness_label:
      status1: ready
      status2: ready
```

The operator will create a `nodeAffinity` on the pods. This makes the
`node_readiness_label` option the global configuration for defining node
affinities for all Postgres clusters. You can have both, cluster-specific and
global affinity, defined and they will get merged on the pods. If
`node_readiness_label_merge` is configured to `"AND"` the node readiness
affinity will end up under the same `matchExpressions` section(s) from the
manifest affinity.

```yaml
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: environment
            operator: In
            values:
            - pci
          - key: status1
            operator: In
            values:
            - ready
          - key: status2
            ...
```

If `node_readiness_label_merge` is set to `"OR"` (default) the readiness label
affinity will be appended with its own expressions block:

```yaml
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: environment
            ...
        - matchExpressions:
          - key: storage
            ...
        - matchExpressions:
          - key: status1
            ...
          - key: status2
            ...
```

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

By default the type of pod anti affinity is `requiredDuringSchedulingIgnoredDuringExecution`,
you can switch to  `preferredDuringSchedulingIgnoredDuringExecution` by setting `pod_antiaffinity_preferred_during_scheduling: true`.

By default the topology key for the pod anti affinity is set to
`kubernetes.io/hostname`, you can set another topology key e.g.
`failure-domain.beta.kubernetes.io/zone`. See [built-in node labels](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#interlude-built-in-node-labels) for available topology keys.

## Pod Disruption Budgets

By default the operator creates two PodDisruptionBudgets (PDB) to protect the cluster
from voluntarily disruptions and hence unwanted DB downtime: so-called primary PDB and
and PDB for critical operations.

### Primary PDB
The `MinAvailable` parameter of this PDB is set to `1` and, if `pdb_master_label_selector`
is enabled, label selector includes `spilo-role=master` condition, which prevents killing
masters in single-node clusters and/or the last remaining running instance in a multi-node
cluster.

## PDB for critical operations
The `MaxUnavailable` parameter of this PDB is set to `0`, while label selector includes
`critical-operation=true` condition. This blocks voluntary disruptions for all pods of a
cluster that are labeled accordingly, without leaving an unsatisfiable budget behind when
no pods carry the label (which previously kept monitoring alerts like
`KubePdbNotEnoughHealthyPods` firing permanently).
For example, Operator labels all Spilo pods with `critical-operation=true` during the major
version upgrade run. You may want to protect cluster pods during other critical operations
by assigning the label to pods yourself or using other means of automation.

The PDB is only relaxed in two scenarios:

* If a cluster is scaled down to `0` instances (e.g. for draining nodes)
* If the PDB is disabled in the configuration (`enable_pod_disruption_budget`)

The PDBs are still in place but fully relaxed: the primary PDB with `MinAvailable`
set to `0` and the critical operations PDB with `MaxUnavailable` set to `100%`.
The two PDBs intentionally use different budget fields matching their purposes:
the primary PDB guarantees a minimum count of always-present pods
(`MinAvailable`), while the critical operations PDB freezes disruptions for
whatever pods currently carry the `critical-operation=true` label - a
usually-empty set, which `MaxUnavailable: 0` expresses without producing an
unsatisfiable budget while idle. Disabling PDBs
helps avoiding blocking Kubernetes upgrades in managed K8s environments at the
cost of prolonged DB downtime. See PR [#384](https://github.com/zalando/postgres-operator/pull/384)
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

The operator will assign a set of environment variables to the database pods
that cannot be overridden to guarantee core functionality. Only variables with
'WAL_' and 'LOG_' prefixes can be customized to allow for backup and log
shipping to be specified differently. There are three ways to specify extra
environment variables (or override existing ones) for database pods:

* [Globally via ConfigMap](#via-configmap)
* [Globally via Secret](#via-secret)
* [Locally via Postgres Cluster Manifest](#via-postgres-cluster-manifest)

The first two options must be referenced from the operator configuration
making them global settings for all Postgres cluster the operator watches.
One use case is a customized Spilo image that must be configured by extra
environment variables. Another case could be to provide custom cloud
provider or backup settings.

The last options allows for specifying environment variables individual to
every cluster via the `env` or `envFrom` section in the manifest. For example,
if you use individual backup locations for each of your clusters. Or you want
to disable WAL archiving for a certain cluster by setting `WAL_S3_BUCKET`,
`WAL_GS_BUCKET` or `AZURE_STORAGE_ACCOUNT` to an empty string.

The operator will give precedence to environment variables in the following
order (e.g. a variable defined in 4. overrides a variable with the same name
in 5.):

1. Assigned by the operator
2. `env` section in cluster manifest
3. Clone section (with WAL settings from operator config when `s3_wal_path` is empty)
4. Standby section
5. Pod environment secret via operator config
6. Pod environment config map via operator config
7. WAL and logical backup settings from operator config

The `envFrom` section is treated separately and allows for a very flexible
local configuration referencing a ConfigMap or a Secret.

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

### Via Postgres Cluster Manifest

It is possible to define environment variables directly in the Postgres cluster
manifest to configure it individually. The variables must be listed under the
`env` section in the same way you would do for [containers](https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container/).
Global parameters served from a custom config map or secret will be overridden.

```yaml
apiVersion: "acid.zalan.do/v1"
kind: postgresql
metadata:
  name: acid-test-cluster
spec:
  env:
  - name: wal_s3_bucket
    value: my-custom-bucket
  - name: minio_secret_key
      valueFrom:
        secretKeyRef:
          name: my-custom-secret
          key: minio_secret_key
```

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
If load balancing is enabled the following default annotation will be applied to
its services:

- `external-dns.alpha.kubernetes.io/hostname` with the value defined by the
  operator configs `master_dns_name_format` and `replica_dns_name_format`.
  This value can't be overwritten. If any changing in its value is needed, it
  MUST be done changing the DNS format operator config parameters; and

There are multiple options to specify service annotations that will be merged
with each other and override in the following order (where latter take
precedence):

1. Default annotations if LoadBalancer is enabled
2. Globally configured `custom_service_annotations`
3. `serviceAnnotations` specified in the cluster manifest
4. `masterServiceAnnotations` and `replicaServiceAnnotations` specified in the cluster manifest

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

Load balancer services can also be enabled for the [connection pooler](user.md#connection-pooler)
pods with manifest flags `enableMasterPoolerLoadBalancer` and/or
`enableReplicaPoolerLoadBalancer` or in the operator configuration with
`enable_master_pooler_load_balancer` and/or `enable_replica_pooler_load_balancer`.
For the `external-dns.alpha.kubernetes.io/hostname` annotation the `-pooler`
suffix will be appended to the cluster name used in the template which is
defined in `master|replica_dns_name_format`.

## Node Ports

Alternatively to Load Balancers Node Ports can be used. Kubernetes services with type
`NodePort` redirect traffic from a specified port on your kubernetes nodes to your service.
To expose your services to an external network with NodePorts you can set `enableMasterNodePort` and/or `enableReplicaNodePort` to `true`
in your cluster manifest. In the case any of these variables are omitted from the manifest, the operator configuration settings `enable_master_node_port` and `enable_replica_node_port` apply.
Note that the operator settings affect all Postgresql services running in all namespaces watched
by the operator.

**Enabling a NodePort configuration will override the corresponding LoadBalancer configuration.**

There are multiple options to specify service annotations that will be merged
with each other and override in the following order (where latter take
precedence):

1. Globally configured `custom_service_annotations`
2. `serviceAnnotations` specified in the cluster manifest
3. `masterServiceAnnotations` and `replicaServiceAnnotations` specified in the cluster manifest

Load-Balancer specific annotations are not applied.

Node port services can also be configured for the [connection pooler](user.md#connection-pooler) pods
with the manifest flags `enableMasterPoolerNodePort` and/or `enableReplicaPoolerNodePort` or in the operator configuration with `enable_master_pooler_node_port`
and/or `enable_replica_pooler_node_port`.

To configure which ports Kubernetes should use for your NodePort service you can configure ports in your cluster manifest
for each type:

- masterNodePort
- masterPoolerNodePort
- replicaNodePort
- replicaPoolerNodePort

When not defined or set to 0 kubernetes will choose a port for you from [your kubernetes cluster's configured range](https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport).

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

You can also check if Spilo is able to find any backups:

```bash
envdir "/run/etc/wal-e.d/env" wal-g backup-list
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

Alternatively, if your cluster uses EKS with OIDC, you can use
[IRSA](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html)
(IAM Roles for Service Accounts) instead of kube2iam. Set `irsa_role_arn` to
the full ARN of the IAM role:

**OperatorConfiguration**

```yaml
apiVersion: "acid.zalan.do/v1"
kind: OperatorConfiguration
metadata:
  name: postgresql-operator-configuration
configuration:
  aws_or_gcp:
    aws_region: eu-central-1
    irsa_role_arn: arn:aws:iam::123456789012:role/postgres-pod-role
    wal_s3_bucket: your-backup-path
```

When `irsa_role_arn` is set the operator annotates the pod service account with
`eks.amazonaws.com/role-arn` on every reconcile. The EKS OIDC webhook then
injects an AWS web identity token into each pod, which takes precedence over
the EC2 metadata credentials used by kube2iam. Both `kube_iam_role` and
`irsa_role_arn` can coexist during a migration — existing pods retain the
kube2iam annotation until they are rotated, at which point only IRSA is used.

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

The operator sets the prefix to an empty string so that spilo will generate it
from the configured `WAL_S3_BUCKET`.

:warning: When you overwrite the configuration by defining `WAL_S3_BUCKET` in
the [pod_environment_configmap](#custom-pod-environment-variables) you have
to set `WAL_BUCKET_SCOPE_PREFIX = ""`, too. Otherwise Spilo will not find
the physical backups on restore (next chapter).

When the `AWS_REGION` is set, `AWS_ENDPOINT` and `WALE_S3_ENDPOINT` are
generated automatically. `WALG_S3_PREFIX` is identical to `WALE_S3_PREFIX`.
`SCOPE` is the Postgres cluster name.

:warning: If both `AWS_REGION` and `AWS_ENDPOINT` or `WALE_S3_ENDPOINT` are
defined backups with WAL-E will fail. You can fix it by switching to WAL-G
with `USE_WALG_BACKUP: "true"`.

### Google Cloud Platform setup

When using GCP, there are two authentication methods to allow the postgres
cluster to access buckets to write WAL-E logs: Workload Identity (recommended)
or using a GCP Service Account Key (legacy).

#### Workload Identity setup

To configure the operator on GCP using Workload Identity these prerequisites are
needed.

* [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) enabled on the GKE cluster where the operator will be deployed
* A GCP service account with the proper IAM setup to access the GCS bucket for the WAL-E logs
* An IAM policy granting the Kubernetes service account the
  `roles/iam.workloadIdentityUser` role on the GCP service account, e.g.:
```bash
gcloud iam service-accounts add-iam-policy-binding <GCP_SERVICE_ACCOUNT_NAME>@<GCP_PROJECT_ID>.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:PROJECT_ID.svc.id.goog[<POSTGRES_OPERATOR_NS>/postgres-pod-custom]"
```

The configuration parameters that we will be using are:

* `wal_gs_bucket`

1. Create a custom Kubernetes service account to be used by Patroni running on
the postgres cluster pods, this service account should include an annotation
with the email address of the Google IAM service account used to communicate
with the GCS bucket, e.g.

```yml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: postgres-pod-custom
  namespace: <POSTGRES_OPERATOR_NS>
  annotations:
    iam.gke.io/gcp-service-account: <GCP_SERVICE_ACCOUNT_NAME>@<GCP_PROJECT_ID>.iam.gserviceaccount.com
```

2. Specify the new custom service account in your [operator parameters](./reference/operator_parameters.md)

If using manual deployment or kustomize, this is done by setting
`pod_service_account_name` in your configuration file specified in the
[postgres-operator deployment](../manifests/postgres-operator.yaml#L37)

If deploying the operator [using Helm](./quickstart.md#helm-chart), this can
be specified in the chart's values file, e.g.:

```yml
...
podServiceAccount:
  name: postgres-pod-custom
```

3. Setup your operator configuration values. Ensure that the operator's configuration
is set up like the following:
```yml
...
aws_or_gcp:
  # additional_secret_mount: ""
  # additional_secret_mount_path: ""
  # aws_region: eu-central-1
  # kube_iam_role: ""
  # irsa_role_arn: ""
  # log_s3_bucket: ""
  # wal_s3_bucket: ""
  wal_gs_bucket: "postgres-backups-bucket-28302F2"  # name of bucket on where to save the WAL-E logs
  # gcp_credentials: ""
...
```

Continue to shared steps below.

#### GCP Service Account Key setup

To configure the operator on GCP using a GCP service account key these
prerequisites are needed.

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
  additional_secret_mount: "psql-wale-creds"
  additional_secret_mount_path: "/var/secrets/google"  # or where ever you want to mount the file
  # aws_region: eu-central-1
  # kube_iam_role: ""
  # irsa_role_arn: ""
  # log_s3_bucket: ""
  # wal_s3_bucket: ""
  wal_gs_bucket: "postgres-backups-bucket-28302F2"  # name of bucket on where to save the WAL-E logs
  gcp_credentials: "/var/secrets/google/key.json"  # combination of the mount path & key in the K8s resource. (i.e. key.json)
...
```

Once you have set up authentication using one of the two methods above, continue
with the remaining shared steps:

1. Setup pod environment configmap that instructs the operator to use WAL-G,
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

2. Then provide this configmap in postgres-operator settings:
```yml
...
# namespaced name of the ConfigMap with environment variables to populate on every pod
pod_environment_configmap: "postgres-operator-system/pod-env-overrides"
...
```

### Azure setup

To configure the operator on Azure these prerequisites are needed:

* A storage account in the same region as the Kubernetes cluster.

The configuration parameters that we will be using are:

* `pod_environment_secret`
* `wal_az_storage_account`

1. Generate the K8s secret resource that will contain your storage account's
access key. You will need a copy of this secret in every namespace you want to
create postgresql clusters.

The latest version of WAL-G (v1.0) supports the use of a SASS token, but you'll
have to make due with using the primary or secondary access token until the
version of WAL-G is updated in the postgres-operator.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: psql-backup-creds
  namespace: default
type: Opaque
stringData:
  AZURE_STORAGE_ACCESS_KEY: <primary or secondary access key>
```

2. Setup pod environment configmap that instructs the operator to use WAL-G,
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
  WALG_AZ_PREFIX: "azure://container-name/$(SCOPE)/$(PGVERSION)" # Enables Azure Backups (SCOPE = Cluster name) (PGVERSION = Postgres version)
```

3. Setup your operator configuration values. With the `psql-backup-creds`
and `pod-env-overrides` resources applied to your cluster, ensure that the operator's configuration
is set up like the following:
```yml
...
kubernetes:
  pod_environment_secret: "psql-backup-creds"
  pod_environment_configmap: "postgres-operator-system/pod-env-overrides"
aws_or_gcp:
  wal_az_storage_account: "postgresbackupsbucket28302F2"  # name of storage account to save the WAL-G logs
...
```

### Restoring physical backups

If cluster members have to be (re)initialized restoring physical backups
happens automatically either from the backup location or by running
[pg_basebackup](https://www.postgresql.org/docs/18/app-pgbasebackup.html)
on one of the other running instances (preferably replicas if they do not lag
behind). You can test restoring backups by [cloning](user.md#how-to-clone-an-existing-postgresql-cluster)
clusters.

If you need to provide a [custom clone environment](#custom-pod-environment-variables)
copy existing variables about your setup (backup location, prefix, access
keys etc.) and prepend the `CLONE_` prefix to get them copied to the correct
directory within Spilo.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-pod-config
data:
  AWS_REGION: "eu-west-1"
  AWS_ACCESS_KEY_ID: "****"
  AWS_SECRET_ACCESS_KEY: "****"
  ...
  CLONE_AWS_REGION: "eu-west-1"
  CLONE_AWS_ACCESS_KEY_ID: "****"
  CLONE_AWS_SECRET_ACCESS_KEY: "****"
  ...
```

### Standby clusters

The setup for [standby clusters](user.md#setting-up-a-standby-cluster) is
similar to cloning when they stream changes from a WAL archive (S3 or GCS).
If you are using [additional environment variables](#custom-pod-environment-variables)
to access your backup location you have to copy those variables and prepend
the `STANDBY_` prefix for Spilo to find the backups and WAL files to stream.

Standby clusters can also stream from a remote primary cluster.
You have to specify the host address. Port is optional and defaults to 5432.
You can combine `standby_host` with either `s3_wal_path` or `gs_wal_path`
for additional redundancy. Note that `s3_wal_path` and `gs_wal_path` are
mutually exclusive. At least one of `s3_wal_path`, `gs_wal_path`, or
`standby_host` must be specified under the `standby` top-level key.

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

2. The [example image](https://github.com/zalando/postgres-operator/blob/master/logical-backup/Dockerfile) implements the backup
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
See [example RBAC](https://github.com/zalando/postgres-operator/blob/master/manifests/operator-service-account-rbac.yaml)

7. Resources of the pod template in the cron job can be configured. When left
empty [default values of spilo pods](reference/operator_parameters.md#kubernetes-resource-requests)
will be used.

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
    env:
      - name: "ENV_VAR_NAME"
        value: "any-k8s-env-things"
    command: ['sh', '-c', 'echo "logging" > /opt/logs.txt']
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
environment variables in the [deployment manifest](https://github.com/zalando/postgres-operator/blob/master/ui/manifests/deployment.yaml#L40).
You can also expose the operator API through a [service](https://github.com/zalando/postgres-operator/blob/master/manifests/api-service.yaml).
Some displayed options can be disabled from UI using simple flags under the
`OPERATOR_UI_CONFIG` field in the deployment.

The viewing and creation of clusters within the UI is limited to the namespace specified by the `TARGET_NAMESPACE` option. To allow the creation and viewing of clusters in all namespaces, set `TARGET_NAMESPACE` to `*`.

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
docker buildx build --load -t ghcr.io/zalando/postgres-operator-ui:v2.0.3 .

# apply UI manifests next to a running Postgres Operator
kubectl apply -f manifests/

# install python dependencies to run UI locally
pip3 install -r requirements
./run_local.sh
```
