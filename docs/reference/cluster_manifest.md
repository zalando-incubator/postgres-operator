Individual postgres clusters are described by the Kubernetes *cluster manifest*
that has the structure defined by the `postgres CRD` (custom resource
definition). The following section describes the structure of the manifest and
the purpose of individual keys. You can take a look at the examples of the
[minimal](https://github.com/zalando/postgres-operator/blob/master/manifests/minimal-postgres-manifest.yaml)
and the
[complete](https://github.com/zalando/postgres-operator/blob/master/manifests/complete-postgres-manifest.yaml)
cluster manifests.

When Kubernetes resources, such as memory, CPU or volumes, are configured,
their amount is usually described as a string together with the units of
measurements. Please, refer to the [Kubernetes
documentation](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/)
for the possible values of those.

:exclamation: If both operator configmap/CRD and a Postgres cluster manifest
define the same parameter, the value from the Postgres cluster manifest is
applied.

## Manifest structure

A postgres manifest is a `YAML` document. On the top level both individual
parameters and parameter groups can be defined. Parameter names are written
in camelCase.

## Cluster metadata

Those parameters are grouped under the `metadata` top-level key.

* **name**
  the name of the cluster. Must start with the `teamId` followed by a dash.
  Changing it after the cluster creation is not supported. Required field.

* **namespace**
  the namespace where the operator creates Kubernetes objects (i.e. pods,
  services, secrets) for the cluster. Changing it after the cluster creation
  results in deploying or updating a completely separate cluster in the target
  namespace. Optional (if present, should match the namespace where the
  manifest is applied).

* **labels**
  if labels are matching one of the `inherited_labels` [configured in the
  operator parameters](operator_parameters.md#kubernetes-resources),
  they will automatically be added to all the objects (StatefulSet, Service,
  Endpoints, etc.) that are created by the operator.
  Labels that are set here but not listed as `inherited_labels` in the operator
  parameters are ignored.

## Top-level parameters

These parameters are grouped directly under  the `spec` key in the manifest.

* **teamId**
  name of the team the cluster belongs to. Changing it after the cluster
  creation is not supported. Required field.

* **dockerImage**
  custom docker image that overrides the **docker_image** operator parameter.
  It should be a [Spilo](https://github.com/zalando/spilo) image.  Optional.

* **spiloFSGroup**
  the Persistent Volumes for the spilo pods in the StatefulSet will be owned
  and writable by the group ID specified. This will override the **spilo_fsgroup**
  operator parameter. This is required to run Spilo as a non-root process, but
  requires a custom spilo image. Note the FSGroup of a Pod cannot be changed
  without recreating a new Pod.

* **enableMasterLoadBalancer**
  boolean flag to override the operator defaults (set by the
  `enable_master_load_balancer` parameter) to define whether to enable the load
  balancer pointing to the postgres primary. Optional.

* **enableReplicaLoadBalancer**
  boolean flag to override the operator defaults (set by the
  `enable_replica_load_balancer` parameter) to define whether to enable the
  load balancer pointing to the postgres standby instances. Optional.

* **allowedSourceRanges**
  when one or more load balancers are enabled for the cluster, this parameter
  defines the comma-separated range of IP networks (in CIDR-notation). The
  corresponding load balancer is accessible only to the networks defined by
  this parameter. Optional, when empty the load balancer service becomes
  inaccessible from outside of the Kubernetes cluster.

* **numberOfInstances**
  total number of  instances for a given cluster. The operator parameters
  `max_instances` and `min_instances` may also adjust this number.  Required
  field.

* **users**
  a map of usernames to user flags for the users that should be created in the
  cluster by the operator. User flags are a list, allowed elements are
  `SUPERUSER`, `REPLICATION`, `INHERIT`, `LOGIN`, `NOLOGIN`, `CREATEROLE`,
  `CREATEDB`, `BYPASSURL`. A login user is created by default unless NOLOGIN is
  specified, in which case the operator creates a role. One can specify empty
  flags by providing a JSON empty array '*[]*'. Optional.

* **databases**
  a map of database names to database owners for the databases that should be
  created by the operator. The owner users should already exist on the cluster
  (i.e. mentioned in the `user` parameter). Optional.

* **tolerations**
  a list of tolerations that apply to the cluster pods. Each element of that
  list is a dictionary with the following fields: `key`, `operator`, `value`,
  `effect` and `tolerationSeconds`. Each field is optional. See [Kubernetes
  examples](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/)
  for details on tolerations and possible values of those keys. When set, this
  value overrides the `pod_toleration` setting from the operator. Optional.

* **podPriorityClassName**
   a name of the [priority
   class](https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/#priorityclass)
   that should be assigned to the cluster pods. When not specified, the value
   is taken from the `pod_priority_class_name` operator parameter, if not set
   then the default priority class is taken. The priority class itself must be
   defined in advance.

* **enableShmVolume**
  Start a database pod without limitations on shm memory. By default docker
  limit `/dev/shm` to `64M` (see e.g. the [docker
  issue](https://github.com/docker-library/postgres/issues/416), which could be
  not enough if PostgreSQL uses parallel workers heavily. If this option is
  present and value is `true`, to the target database pod will be mounted a new
  tmpfs volume to remove this limitation. If it's not present, the decision
  about mounting a volume will be made based on operator configuration
  (`enable_shm_volume`, which is `true` by default). It it's present and value
  is `false`, then no volume will be mounted no matter how operator was
  configured (so you can override the operator configuration).

* **enableLogicalBackup**
  Determines if the logical backup of this cluster should be taken and uploaded
  to S3. Default: false.

* **logicalBackupSchedule**
  Schedule for the logical backup k8s cron job. Please take [the reference schedule format](https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#schedule)
  into account. Default: "30 00 \* \* \*"

## Postgres parameters

Those parameters are grouped under the `postgresql` top-level key.

* **version**
  the postgres major version of the cluster. Looks at the [Spilo
  project](https://github.com/zalando/spilo/releases) for the list of supported
  versions. Changing the cluster version once the cluster has been bootstrapped
  is not supported. Required field.

* **parameters**
  a dictionary of postgres parameter names and values to apply to the resulting
  cluster. Optional (Spilo automatically sets reasonable defaults for
  parameters like work_mem or max_connections).


## Patroni parameters

Those parameters are grouped under the `patroni` top-level key. See the [patroni
documentation](https://patroni.readthedocs.io/en/latest/SETTINGS.html) for the
explanation of `ttl` and `loop_wait` parameters.

* **initdb**
  a map of key-value pairs describing initdb parameters. For `data-checksum`,
  `debug`, `no-locale`, `noclean`, `nosync` and `sync-only` parameters use
  `true` as the value if you want to set them. Changes to this option do not
  affect the already initialized clusters. Optional.

* **pg_hba**
  list of custom `pg_hba` lines to replace default ones. Note that the default
  ones include

  ```
  hostsll all +pamrole all pam
  ```
  , where pamrole is the name of the role for the pam authentication; any
    custom `pg_hba` should include the pam line to avoid breaking pam
    authentication. Optional.

* **ttl**
  patroni `ttl` parameter value, optional. The default is set by the Spilo
  docker image. Optional.

* **loop_wait**
  patroni `loop_wait` parameter value, optional. The default is set by the
  Spilo docker image. Optional.

* **retry_timeout**
  patroni `retry_timeout` parameter value, optional. The default is set by the
  Spilo docker image. Optional.

* **maximum_lag_on_failover**
  patroni `maximum_lag_on_failover` parameter value, optional. The default is
  set by the Spilo docker image. Optional.

* **slots**
  permanent replication slots that Patroni preserves after failover by
  re-creating them on the new primary immediately after doing a promote. Slots
  could be reconfigured with the help of `patronictl edit-config`. It is the
  responsibility of a user to avoid clashes in names between replication slots
  automatically created by Patroni for cluster members and permanent replication
  slots. Optional.

* **standby_cluster**
  initialises cluster as a standby creating a cascading replication, where elected master is streaming from specified remote location

## Postgres container resources

Those parameters define [CPU and memory requests and
limits](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/)
for the postgres container. They are grouped under the `resources` top-level
key. There are two subgroups, `requests` and `limits`.

### Requests

CPU and memory requests for the postgres container.

* **cpu**
  CPU requests for the postgres container. Optional, overrides the
  `default_cpu_requests` operator configuration parameter. Optional.

* **memory**
  memory requests for the postgres container. Optional, overrides the
  `default_memory_request` operator configuration parameter. Optional.

#### Limits

CPU and memory limits for the postgres container.

* **cpu**
  CPU limits for the postgres container. Optional, overrides the
  `default_cpu_limits` operator configuration parameter. Optional.

* **memory**
  memory limits for the postgres container. Optional, overrides the
  `default_memory_limits` operator configuration parameter. Optional.

## Parameters defining how to clone the cluster from another one

Those parameters are applied when the cluster should be a clone of another one
that is either already running or has a basebackup on S3. They are grouped
under the `clone` top-level key and do not affect the already running cluster.

* **cluster**
  name of the cluster to clone from. Translated to either the service name or
  the key inside the S3 bucket containing base backups. Required when the
  `clone` section is present.

* **uid**
  Kubernetes UID of the cluster to clone from. Since cluster name is not a
  unique identifier of the cluster (as identically named clusters may exist in
  different namespaces) , the operator uses UID in the S3 bucket name in order
  to guarantee uniqueness. Has no effect when cloning from the running
  clusters. Optional.

* **timestamp**
  the timestamp up to which the recovery should proceed. The operator always
  configures non-inclusive recovery target, stopping right before the given
  timestamp. When this parameter is set the operator will not consider cloning
  from the live cluster, even if it is running, and instead goes to S3. Optional.

* **s3_endpoint**
  the url of the S3-compatible service should be set when cloning from non AWS S3. Optional.

* **s3_access_key_id**
  the access key id, used for authentication on S3 service. Optional.

* **s3_secret_access_key**
  the secret access key, used for authentication on S3 service. Optional.

* **s3_force_path_style**
  to enable path-style addressing(i.e., http://s3.amazonaws.com/BUCKET/KEY) when connecting to an S3-compatible service
  that lack of support for sub-domain style bucket URLs (i.e., http://BUCKET.s3.amazonaws.com/KEY). Optional.

### EBS volume resizing

Those parameters are grouped under the `volume` top-level key and define the
properties of the persistent storage that stores postgres data.

* **size**
  the size of the target EBS volume. Usual Kubernetes size modifiers, i.e. `Gi`
  or `Mi`, apply. Required.

* **storageClass**
  the name of the Kubernetes storage class to draw the persistent volume from.
  See [Kubernetes
  documentation](https://kubernetes.io/docs/concepts/storage/storage-classes/)
  for the details on storage classes. Optional.

### Sidecar definitions

Those parameters are defined under the `sidecars` key. They consist of a list
of dictionaries, each defining one sidecar (an extra container running
along the main postgres container on the same pod). The following keys can be
defined in the sidecar dictionary:

* **name**
  name of the sidecar. Required.

* **image**
  docker image of the sidecar. Required.

* **env**
  a dictionary of environment variables. Use usual Kubernetes definition
  (https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)
  for environment variables. Optional.

* **resources** see below. Optional.

#### Sidecar container resources

Those parameters define [CPU and memory requests and
limits](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/)
for the sidecar container. They are grouped under the `resources` key for each sidecar.
There are two subgroups, `requests` and `limits`.

##### Requests

CPU and memory requests for the sidecar container.

* **cpu**
  CPU requests for the sidecar container. Optional, overrides the
  `default_cpu_requests` operator configuration parameter. Optional.

* **memory**
  memory requests for the sidecar container. Optional, overrides the
  `default_memory_request` operator configuration parameter. Optional.

##### Limits

CPU and memory limits for the sidecar container.

* **cpu**
  CPU limits for the sidecar container. Optional, overrides the
  `default_cpu_limits` operator configuration parameter. Optional.

* **memory**
  memory limits for the sidecar container. Optional, overrides the
  `default_memory_limits` operator configuration parameter. Optional.
