<h1>Administrator Guide</h1>

Learn how to configure and manage the Postgres Operator in your Kubernetes (K8s)
environment.

## Minor and major version upgrade

Minor version upgrades for PostgreSQL are handled via updating the Spilo Docker
image. The operator will carry out a rolling update of Pods which includes a
switchover (planned failover) of the master to the Pod with new minor version.
The switch should usually take less than 5 seconds, still clients have to
reconnect.

Major version upgrades are supported via [cloning](user.md#how-to-clone-an-existing-postgresql-cluster).
The new cluster manifest must have a higher `version` string than the source
cluster and will be created from a basebackup. Depending of the cluster size,
downtime in this case can be significant as writes to the database should be
stopped and all WAL files should be archived first before cloning is started.

Note, that simply changing the version string in the `postgresql` manifest does
not work at present and leads to errors. Neither Patroni nor Postgres Operator
can do in place `pg_upgrade`. Still, it can be executed manually in the Postgres
container, which is tricky (i.e. systems need to be stopped, replicas have to be
synced) but of course faster than cloning.

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
zk8 patch crd postgresqls.acid.zalan.do -p '{"spec":{"validation": null}}'
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
It is possible to configure a ConfigMap as well as a Secret which are used by the Postgres pods as
an additional provider for environment variables. One use case is to customize
the Spilo image and configure it with environment variables. Another case could be to provide custom
cloud provider or backup settings.

In general the Operator will give preference to the globally configured variables, to not have the custom
ones interfere with core functionality. Variables with the 'WAL_' and 'LOG_' prefix can be overwritten though, to allow
backup and logshipping to be specified differently.


### Via ConfigMap
The ConfigMap with the additional settings is referenced in the operator's main configuration.
A namespace can be specified along with the name. If left out, the configured
default namespace of your K8s client will be used and if the ConfigMap is not
found there, the Postgres cluster's namespace is taken when different:

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

The key-value pairs of the ConfigMap are then added as environment variables to the
Postgres StatefulSet/pods.


### Via Secret
The Secret with the additional variables is referenced in the operator's main configuration.
To protect the values of the secret from being exposed in the pod spec they are each referenced
as SecretKeyRef.
This does not allow for the secret to be in a different namespace as the pods though

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

The key-value pairs of the Secret are all accessible as environment variables to the
Postgres StatefulSet/pods.

## Limiting the number of min and max instances in clusters

As a preventive measure, one can restrict the minimum and the maximum number of
instances permitted by each Postgres cluster managed by the operator. If either
`min_instances` or `max_instances` is set to a non-zero value, the operator may
adjust the number of instances specified in the cluster manifest to match
either the min or the max boundary. For instance, of a cluster manifest has 1
instance and the `min_instances` is set to 3, the cluster will be created with 3
instances. By default, both parameters are set to `-1`.

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
admin rights to maintain it, and (b) Postgres superuser teams that get the
superuser access to all Postgres databases running in a K8s cluster for the
purposes of maintaining and troubleshooting.

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

## Logical backups

The operator can manage K8s cron jobs to run logical backups of Postgres
clusters. The cron job periodically spawns a batch job that runs a single pod.
The backup script within this pod's container can connect to a DB for a logical
backup. The operator updates cron jobs during Sync if the job schedule changes;
the job name acts as the job identifier. These jobs are to be enabled for each
individual Postgres cluster by setting `enableLogicalBackup: true` in its
manifest. Notes:

1. The [example image](../docker/logical-backup/Dockerfile) implements the
backup via `pg_dumpall` and upload of compressed and encrypted results to an S3
bucket; the default image ``registry.opensource.zalan.do/acid/logical-backup``
is the same image built with the Zalando-internal CI pipeline. `pg_dumpall`
requires a `superuser` access to a DB and runs on the replica when possible.

2. Due to the [limitation of K8s cron jobs](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-job-limitations)
it is highly advisable to set up additional monitoring for this feature; such
monitoring is outside of the scope of operator responsibilities.

3. The operator does not remove old backups.

4. You may use your own image by overwriting the relevant field in the operator
configuration. Any such image must ensure the logical backup is able to finish
[in presence of pod restarts](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#handling-pod-and-container-failures)
and [simultaneous invocations](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/#cron-job-limitations)
of the backup cron job.

5. For that feature to work, your RBAC policy must enable operations on the
`cronjobs` resource from the `batch` API group for the operator service account.
See [example RBAC](../manifests/operator-service-account-rbac.yaml)

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

## Google Cloud Platform setup

To configure the operator on GCP there are some prerequisites that are needed:

* A service account with the proper IAM setup to access the GCS bucket for the WAL-E logs
* The credentials file for the service account.

The configuration paramaters that we will be using are:

* `additional_secret_mount`
* `additional_secret_mount_path`
* `gcp_credentials`
* `wal_gs_bucket`

### Generate a K8 secret resource

Generate the K8 secret resource that will contain your service account's 
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

### Setup your operator configuration values

With the `psql-wale-creds` resource applied to your cluster, ensure that
the operator's configuration is set up like the following:

```yml
...
aws_or_gcp:
  additional_secret_mount: "pgsql-wale-creds"
  additional_secret_mount_path: "/var/secrets/google" # or where ever you want to mount the file
  # aws_region: eu-central-1
  # kube_iam_role: ""
  # log_s3_bucket: ""
  # wal_s3_bucket: ""
  wal_gs_bucket: "postgres-backups-bucket-28302F2" # name of bucket on where to save the WAL-E logs
  gcp_credentials: "/var/secrets/google/key.json" # combination of the mount path & key in the K8 resource. (i.e. key.json)
...
```

## Retain state of Postgres cluster when the resource is deleted
By default, deleting the Postgresql custom resource will also explicitly delete its PersistentVolumeClaims. If your environment allows Postgresql custom resources to be deleted occasionally, and you don't want to lose your users and database contents, you can set the `enable_pvc_deletion` operator configuration to false. This will make sure that the secrets and PVCs linked to this cluster are retained, so when you redeploy your cluster, the users and data are not lost. With this config, you're still able to delete specific Postgres clusters by setting `volume.deletePVC` in your manifest to the name of the cluster.

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
docker build -t registry.opensource.zalan.do/acid/postgres-operator-ui:v1.3.0 .

# apply UI manifests next to a running Postgres Operator
kubectl apply -f manifests/

# install python dependencies to run UI locally
pip3 install -r requirements
./run_local.sh
```
