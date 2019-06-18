# How to configure PostgreSQL operator

## Select the namespace to deploy to

The operator can run in a namespace other than `default`. For example, to use
the `test` namespace, run the following before deploying the operator's
manifests:

```bash
    $ kubectl create namespace test
    $ kubectl config set-context $(kubectl config current-context) --namespace=test
```

All subsequent `kubectl` commands will work with the `test` namespace. The
operator will run in this namespace and look up needed resources - such as its
ConfigMap - there. Please note that the namespace for service accounts and
cluster role bindings in [operator RBAC rules](../manifests/operator-service-account-rbac.yaml)
needs to be adjusted to the non-default value.

## Specify the namespace to watch

Watching a namespace for an operator means tracking requests to change
Postgresql clusters in the namespace such as "increase the number of Postgresql
replicas to 5" and reacting to the requests, in this example by actually
scaling up.

By default, the operator watches the namespace it is deployed to. You can
change this by setting the `WATCHED_NAMESPACE` var in the `env` section of the
[operator deployment](../manifests/postgres-operator.yaml) manifest or by
altering the `watched_namespace` field in the operator
[ConfigMap](../manifests/configmap.yaml#L6).
In the case both are set, the env var takes the precedence. To make the
operator listen to all namespaces, explicitly set the field/env var to "`*`".

Note that for an operator to manage pods in the watched namespace, the
operator's service account (as specified in the operator deployment manifest)
has to have appropriate privileges to access the watched namespace. The
operator may not be able to function in the case it watches all namespaces but
lacks access rights to any of them (except Kubernetes system namespaces like
`kube-system`). The reason is that for multiple namespaces operations such as
'list pods' execute at the cluster scope and fail at the first violation of
access rights.

The watched namespace also needs to have a (possibly different) service account
in the case database pods need to talk to the Kubernetes API (e.g. when using
Kubernetes-native configuration of Patroni). The operator checks that the
`pod_service_account_name` exists in the target namespace, and, if not, deploys
there the `pod_service_account_definition` from the operator
[`Config`](../pkg/util/config/config.go) with the default value of:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
 name: operator
```

In this definition, the operator overwrites the account's name to match
`pod_service_account_name` and the `default` namespace to match the target
namespace. The operator performs **no** further syncing of this account.

## Non-default cluster domain

If your cluster uses a different dns domain than `cluster.local`, this needs
to be set in the operator ConfigMap. This is used by the operator to connect
to the clusters after creation.

## Role-based access control for the operator

The `manifests/operator-service-account-rbac.yaml` defines cluster roles and
bindings needed for the operator to function under access control restrictions.
To deploy the operator with this RBAC policy use:

```bash
    $ kubectl create -f manifests/configmap.yaml
    $ kubectl create -f manifests/operator-service-account-rbac.yaml
    $ kubectl create -f manifests/postgres-operator.yaml
    $ kubectl create -f manifests/minimal-postgres-manifest.yaml
```

Note that the service account in `operator-rbac.yaml` is named
`zalando-postgres-operator`. You may have to change the `service_account_name`
in the operator ConfigMap and `serviceAccountName` in the postgres-operator
deployment appropriately.

This is done intentionally to avoid breaking those setups that already work
with the default `operator` account. In the future the operator should ideally
be run under the `zalando-postgres-operator` service account.

The service account defined in `operator-rbac.yaml` acquires some privileges
not really used by the operator (i.e. we only need `list` and `watch` on
`configmaps` resources), this is also done intentionally to avoid breaking
things if someone decides to configure the same service account in the
operator's ConfigMap to run postgres clusters.

### Use taints and tolerations for dedicated PostgreSQL nodes

To ensure Postgres pods are running on nodes without any other application
pods, you can use
[taints and tolerations](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/)
and configure the required toleration in the operator ConfigMap.

As an example you can set following node taint:

```bash
    $ kubectl taint nodes <nodeName> postgres=:NoSchedule
```

And configure the toleration for the PostgreSQL pods by adding following line
to the ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  toleration: "key:postgres,operator:Exists,effect:NoSchedule"
  ...
```

Note that the Kubernetes version 1.13 brings [taint-based eviction](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/#taint-based-evictions) to the beta stage and enables it by default.
Postgres pods by default receive tolerations for `unreachable` and `noExecute` taints with the timeout of `5m`.
Depending on your setup, you may want to adjust these parameters to prevent master pods from being evicted by the Kubernetes runtime.
To prevent eviction completely, specify the toleration by leaving out the `tolerationSeconds` value (similar to how Kubernetes' own DaemonSets are configured)

### Enable pod anti affinity

To ensure Postgres pods are running on different topologies, you can use [pod anti affinity](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/)
and configure the required topology in the operator ConfigMap.

Enable pod anti affinity by adding following line to the operator ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  enable_pod_antiaffinity: "true"
```

By default the topology key for the pod anti affinity is set to `kubernetes.io/hostname`,
you can set another topology key e.g. `failure-domain.beta.kubernetes.io/zone` by adding following line
to the operator ConfigMap, see [built-in node labels](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#interlude-built-in-node-labels) for available topology keys:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  enable_pod_antiaffinity: "true"
  pod_antiaffinity_topology_key: "failure-domain.beta.kubernetes.io/zone"
```

### Pod Disruption Budget

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
downtime. See PR #384 for the use case.

### Add cluster-specific labels

In some cases, you might want to add `labels` that are specific to a given
postgres cluster, in order to identify its child objects.
The typical use case is to add labels that identifies the `Pods` created by the
operator, in order to implement fine-controlled `NetworkPolicies`.

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
...
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
...
```


## Custom Pod Environment Variables

It is possible to configure a ConfigMap which is used by the Postgres pods as
an additional provider for environment variables.

One use case is to customize the Spilo image and configure it with environment
variables. The ConfigMap with the additional settings is configured in the
operator's main ConfigMap:

**postgres-operator ConfigMap**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-operator
data:
  # referencing config map with custom settings
  pod_environment_configmap: postgres-pod-config
  ...
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

This ConfigMap is then added as a source of environment variables to the
Postgres StatefulSet/pods.

## Limiting the number of instances in clusters with `min_instances` and `max_instances`

As a preventive measure, one can restrict the minimum and the maximum number of
instances permitted by each Postgres cluster managed by the operator. If either
`min_instances` or `max_instances` is set to a non-zero value, the operator may
adjust the number of instances specified in the cluster manifest to match
either the min or the max boundary. For instance, of a cluster manifest has 1
instance and the `min_instances` is set to 3, the cluster will be created with 3
instances. By default, both parameters are set to `-1`.

## Load balancers

For any Postgresql/Spilo cluster, the operator creates two separate Kubernetes
services: one for the master pod and one for replica pods. To expose these
services to an outer network, one can attach load balancers to them by setting
`enableMasterLoadBalancer` and/or `enableReplicaLoadBalancer` to `true` in the
cluster manifest. In the case any of these variables are omitted from the
manifest, the operator configmap's settings `enable_master_load_balancer` and
`enable_replica_load_balancer` apply. Note that the operator settings affect
all Postgresql services running in all namespaces watched by the operator.

To limit the range of IP adresses that can reach a load balancer, specify the
desired ranges in the `allowedSourceRanges` field (applies to both master and
replica load balancers). To prevent exposing load balancers to the entire
Internet, this field is set at cluster creation time to `127.0.0.1/32` unless
overwritten explicitly. If you want to revoke all IP ranges from an existing
cluster, please set the `allowedSourceRanges` field to `127.0.0.1/32` or to an
empty sequence `[]`. Setting the field to `null` or omitting it entirely may
lead to Kubernetes removing this field from the manifest due to its
[handling of null fields](https://kubernetes.io/docs/concepts/overview/object-management-kubectl/declarative-config/#how-apply-calculates-differences-and-merges-changes).
Then the resultant manifest will not contain the necessary change, and the
operator will respectively do noting with the existing source ranges.

## Running periodic 'autorepair' scans of Kubernetes objects

The Postgres operator periodically scans all Kubernetes objects belonging to
each cluster and repairs all discrepancies between them and the definitions
generated from the current cluster manifest. There are two types of scans:

* `sync scan`, running every `resync_period` seconds for every cluster

* `repair scan`, coming every `repair_period` only for those clusters that didn't
report success as a result of the last operation applied to them.

## Postgres roles supported by the operator

The operator is capable of maintaining roles of multiple kinds within a
Postgres database cluster:

* **System roles** are roles necessary for the proper work of Postgres itself such as a replication role or the initial superuser role. The operator delegates creating such roles to Patroni and only establishes relevant secrets.

* **Infrastructure roles** are roles for processes originating from external systems, e.g. monitoring robots. The operator creates such roles in all Postgres clusters it manages assuming that Kubernetes secrets with the relevant credentials exist beforehand.

* **Per-cluster robot users** are also roles for processes originating from external systems but defined for an individual Postgres cluster in its manifest. A typical example is a role for connections from an application that uses the database.

* **Human users** originate from the Teams API that returns a list of the team members given a team id. The operator differentiates between (a) product teams that own a particular Postgres cluster and are granted admin rights to maintain it, and (b) Postgres superuser teams that get the superuser access to all Postgres databases running in a Kubernetes cluster for the purposes of maintaining and troubleshooting.

## Understanding rolling update of Spilo pods

The operator logs reasons for a rolling update with the `info` level and a diff between the old and new StatefulSet specs with the `debug` level. To benefit from numerous escape characters in the latter log entry, view it in CLI with `echo -e`. Note that the resultant message will contain some noise because the `PodTemplate` used by the operator is yet to be updated with the default values used internally in Kubernetes.

## Logical backups

The operator can manage k8s cron jobs to run logical backups of Postgres clusters. The cron job periodically spawns a batch job that runs a single pod. The backup script within this pod's container can connect to a DB for a logical backup. The operator updates cron jobs during Sync if the job schedule changes; the job name acts as the job identifier. These jobs are to be enabled for each indvidual Postgres cluster by setting `enableLogicalBackup: true` in its manifest. Notes:

1. The [example image](../docker/logical-backup/Dockerfile) implements the backup via `pg_dumpall` and upload of compressed and encrypted results to an S3 bucket; the default image ``registry.opensource.zalan.do/acid/logical-backup`` is the same image built with the Zalando-internal CI pipeline. `pg_dumpall` requires a `superuser` access to a DB and runs on the replica when possible.  

2. Due to the [limitation of Kubernetes cron jobs](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-job-limitations) it is highly advisable to set up additional monitoring for this feature; such monitoring is outside of the scope of operator responsibilities.

3. The operator does not remove old backups.

4. You may use your own image by overwriting the relevant field in the operator configuration. Any such image must ensure the logical backup is able to finish [in presence of pod restarts](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#handling-pod-and-container-failures) and [simultaneous invocations](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-job-limitations) of the backup cron job.

5. For that feature to work, your RBAC policy must enable operations on the `cronjobs` resource from the `batch` API group for the operator service account. See [example RBAC](../manifests/operator-service-account-rbac.yaml)

## Access to cloud resources from clusters in non cloud environment

To access cloud resources like S3 from a cluster in a bare metal setup you can use
`additional_secret_mount` and `additional_secret_mount_path` config parameters.
With this you can provision cloud credentials to the containers in the pods of the StatefulSet.
This works this way that it mounts a volume from the given secret in the pod and this can
then accessed in the container over the configured mount path. Via [Custum Pod Environment Variables](## Custom Pod Environment Variables)
you can then point the different cloud sdk's (aws, google etc.) to this mounted secret.
With this credentials the cloud sdk can then access cloud resources to upload logs etc.

This secrets can be pre provisioned in different ways:

* Generic secret created via `kubectl create secret generic some-cloud-creds --from-file=some-cloud-credentials-file.json`

* Automaticly provisioned via a Controller like [kube-aws-iam-controller](https://github.com/mikkeloscar/kube-aws-iam-controller). This controller would then also rotate the credentials. Please visit the documention for more information.
