<h1>Cluster manifest reference</h1>

Individual Postgres clusters are described by the Kubernetes *cluster manifest*
that has the structure defined by the `postgresql` CRD (custom resource
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

A Postgres manifest is a `YAML` document. On the top level both individual
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
  name of the team the cluster belongs to. Required field.

* **numberOfInstances**
  total number of  instances for a given cluster. The operator parameters
  `max_instances` and `min_instances` may also adjust this number. Required
  field.

* **dockerImage**
  custom Docker image that overrides the **docker_image** operator parameter.
  It should be a [Spilo](https://github.com/zalando/spilo) image. Optional.

* **schedulerName**
  specifies the scheduling profile for database pods. If no value is provided
  K8s' `default-scheduler` will be used. Optional.

* **spiloRunAsUser**
  sets the user ID which should be used in the container to run the process.
  This must be set to run the container without root. By default the container
  runs with root. This option only works for Spilo versions >= 1.6-p3.

* **spiloRunAsGroup**
  sets the group ID which should be used in the container to run the process.
  This must be set to run the container without root. By default the container
  runs with root. This option only works for Spilo versions >= 1.6-p3.

* **spiloFSGroup**
  the Persistent Volumes for the Spilo pods in the StatefulSet will be owned and
  writable by the group ID specified. This will override the **spilo_fsgroup**
  operator parameter. This is required to run Spilo as a non-root process, but
  requires a custom Spilo image. Note the FSGroup of a Pod cannot be changed
  without recreating a new Pod. Optional.

* **livenessProbe**
  Allows for adding a liveness probe to the Spilo container to detect if it's
  running properly.

* **enableMasterLoadBalancer**
  boolean flag to override the operator defaults (set by the
  `enable_master_load_balancer` parameter) to define whether to enable the load
  balancer pointing to the Postgres primary. Optional.

* **enableMasterPoolerLoadBalancer**
  boolean flag to override the operator defaults (set by the
  `enable_master_pooler_load_balancer` parameter) to define whether to enable
  the load balancer for master pooler pods pointing to the Postgres primary.
  Optional.

* **enableReplicaLoadBalancer**
  boolean flag to override the operator defaults (set by the
  `enable_replica_load_balancer` parameter) to define whether to enable the
  load balancer pointing to the Postgres standby instances. Optional.

* **enableReplicaPoolerLoadBalancer**
  boolean flag to override the operator defaults (set by the
  `enable_replica_pooler_load_balancer` parameter) to define whether to enable
  the load balancer for replica pooler pods pointing to the Postgres standby
  instances. Optional.

* **allowedSourceRanges**
  when one or more load balancers are enabled for the cluster, this parameter
  defines the comma-separated range of IP networks (in CIDR-notation). Both
  IPv4 (e.g. `192.168.1.0/24`) and IPv6 (e.g. `fd01::/48`) CIDR ranges are
  supported. The corresponding load balancer is accessible only to the networks
  defined by this parameter. Optional, when empty the load balancer service
  becomes inaccessible from outside of the Kubernetes cluster.

* **enableMasterNodePort**
  boolean flag to override the operator defaults (set by the
  `enable_master_node_port` parameter) to define whether to enable the node
  port pointing to the Postgres primary. Optional. Overrides `enableMasterLoadBalancer`.

* **enableMasterPoolerNodePort**
  boolean flag to override the operator defaults (set by the
  `enable_master_pooler_node_port` parameter) to define whether to enable
  the node port for master pooler pods pointing to the Postgres primary.
  Optional. Overrides `enableMasterPoolerLoadBalancer`.

* **enableReplicaNodePort**
  boolean flag to override the operator defaults (set by the
  `enable_replica_node_port` parameter) to define whether to enable the node
  port pointing to the Postgres standby instances. Optional. Overrides `enableReplicaLoadBalancer`.

* **enableReplicaPoolerNodePort**
  boolean flag to override the operator defaults (set by the
  `enable_replica_pooler_node_port` parameter) to define whether to enable
  the node port for replica pooler pods pointing to the Postgres standby
  instances. Optional. Overrides `enableReplicaPoolerLoadBalancer`.

* **masterNodePort**
  integer flag to specify a port number for the node port to the Postgres primary.
  Only used when `enableMasterNodePort` or `enable_master_node_port` are enabled.
  Optional. Kubernetes will provide a port number for you if not specified.

* **masterPoolerNodePort**
  integer flag to specify a port number for the node port for the master pooler pods pointing to the Postgres primary.
  Only used when `enableMasterPoolerNodePort` or `enable_master_pooler_node_port` are enabled.
  Optional. Kubernetes will provide a port number for you if not specified.

* **replicaNodePort**
  integer flag to specify a port number for the node port pointing to the Postgres standby instances.
  Only used when `enableReplicaNodePort` or `enable_replica_node_port` are enabled.
  Optional. Kubernetes will provide a port number for you if not specified.

* **replicaPoolerNodePort**
  integer flag to specify a port number for the node port for the replica pooler pods pointing to the Postgres standby instances
  Only used when `enableReplicaPoolerNodePort` or `enable_replica_pooler_node_port` are enabled.
  Optional. Kubernetes will provide a port number for you if not specified.

* **maintenanceWindows**
  a list which defines specific time frames when certain maintenance operations
  such as automatic major upgrades or master pod migration are allowed to happen.
  Accepted formats are "01:00-06:00" for daily maintenance windows or
  "Sat:00:00-04:00" for specific days, with all times in UTC. Note, when the
  global config option `enable_maintenance_windows` is false, the specified
  windows will be ignored.

* **users**
  a map of usernames to user flags for the users that should be created in the
  cluster by the operator. User flags are a list, allowed elements are
  `SUPERUSER`, `REPLICATION`, `INHERIT`, `LOGIN`, `NOLOGIN`, `CREATEROLE`,
  `CREATEDB`, `BYPASSRLS`. A login user is created by default unless NOLOGIN is
  specified, in which case the operator creates a role. One can specify empty
  flags by providing a JSON empty array '*[]*'. If the config option
  `enable_cross_namespace_secret` is enabled you can specify the namespace in
  the user name in the form `{namespace}.{username}` and the operator will
  create the K8s secret in that namespace. The part after the first `.` is
  considered to be the user name. Optional.

* **usersWithSecretRotation**
  list of users to enable credential rotation in K8s secrets. The rotation
  interval can only be configured globally. On each rotation a new user will
  be added in the database replacing the `username` value in the secret of
  the listed user. Although, rotation users inherit all rights from the
  original role, keep in mind that ownership is not transferred. See more
  details in the [administrator docs](https://github.com/zalando/postgres-operator/blob/master/docs/administrator.md#password-rotation-in-k8s-secrets).

* **usersWithInPlaceSecretRotation**
  list of users to enable in-place password rotation in K8s secrets. The
  rotation interval can only be configured globally. On each rotation the
  password value will be replaced in the secrets which the operator reflects
  in the database, too. List only users here that rarely connect to the
  database, like a flyway user running a migration on Pod start. See more
  details in the [administrator docs](https://github.com/zalando/postgres-operator/blob/master/docs/administrator.md#password-replacement-without-extra-users).

* **usersIgnoringSecretRotation**
  if you have secret rotation enabled globally you can define a list of
  of users that should opt out from it, for example if you store credentials
  outside of K8s, too, and corresponding deployments cannot dynamically
  reference secrets. Note, you can also opt out from the rotation by removing
  users from the manifest's `users` section. The operator will not drop them
  from the database. Optional.

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
  defined in advance. Optional.

* **podAnnotations**
  A map of key value pairs that gets attached as [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
  to each pod created for the database.

* **serviceAnnotations**
  A map of key value pairs that gets attached as [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
  to the services created for the database cluster. Check the
  [administrator docs](https://github.com/zalando/postgres-operator/blob/master/docs/administrator.md#load-balancers-and-allowed-ip-ranges)
  for more information regarding default values and overwrite rules.

* **masterServiceAnnotations**
  A map of key value pairs that gets attached as [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
  to the master service created for the database cluster. Check the
  [administrator docs](https://github.com/zalando/postgres-operator/blob/master/docs/administrator.md#load-balancers-and-allowed-ip-ranges)
  for more information regarding default values and overwrite rules.
  This field overrides `serviceAnnotations` with the same key for the master
  service if not empty.

* **replicaServiceAnnotations**
  A map of key value pairs that gets attached as [annotations](https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/)
  to the replica service created for the database cluster. Check the
  [administrator docs](https://github.com/zalando/postgres-operator/blob/master/docs/administrator.md#load-balancers-and-allowed-ip-ranges)
  for more information regarding default values and overwrite rules.
  This field overrides `serviceAnnotations` with the same key for the replica
  service if not empty.

* **enableShmVolume**
  Start a database pod without limitations on shm memory. By default Docker
  limit `/dev/shm` to `64M` (see e.g. the [docker
  issue](https://github.com/docker-library/postgres/issues/416), which could be
  not enough if PostgreSQL uses parallel workers heavily. If this option is
  present and value is `true`, to the target database pod will be mounted a new
  tmpfs volume to remove this limitation. If it's not present, the decision
  about mounting a volume will be made based on operator configuration
  (`enable_shm_volume`, which is `true` by default). It it's present and value
  is `false`, then no volume will be mounted no matter how operator was
  configured (so you can override the operator configuration). Optional.

* **enableConnectionPooler**
  Tells the operator to create a connection pooler with a database for the master
  service. If this field is true, a connection pooler deployment will be created even if
  `connectionPooler` section is empty. Optional, not set by default.

* **enableReplicaConnectionPooler**
  Tells the operator to create a connection pooler with a database for the replica
  service. If this field is true, a connection pooler deployment for replica
  will be created even if `connectionPooler` section is empty. Optional, not set by default.

* **enableLogicalBackup**
  Determines if the logical backup of this cluster should be taken and uploaded
  to S3. Default: false. Optional.

* **logicalBackupRetention**
  You can set a retention time for the logical backup cron job to remove old backup
  files after a new backup has been uploaded. Example values are "3 days", "2 weeks", or
  "1 month". It takes precedence over the global `logical_backup_s3_retention_time`
  configuration. Currently only supported for AWS. Optional.

* **logicalBackupSchedule**
  Schedule for the logical backup K8s cron job. Please take
  [the reference schedule format](https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#schedule)
  into account. It takes precedence over the global `logical_backup_schedule`
  configuration. Optional.

* **additionalVolumes**
  List of additional volumes to mount in each container of the statefulset pod.
  Each item must contain a `name`, `mountPath`, and `volumeSource` which is a
  [kubernetes volumeSource](https://godoc.org/k8s.io/api/core/v1#VolumeSource).
  It allows you to mount existing PersistentVolumeClaims, ConfigMaps and Secrets inside the StatefulSet.
  Also an `emptyDir` volume can be shared between initContainer and statefulSet.
  Additionally, you can provide a `SubPath` for volume mount (a file in a configMap source volume, for example).
  Set `isSubPathExpr` to true if you want to include [API environment variables](https://kubernetes.io/docs/concepts/storage/volumes/#using-subpath-expanded-environment).
  You can also specify in which container the additional Volumes will be mounted with the `targetContainers` array option.
  If `targetContainers` is empty, additional volumes will be mounted only in the `postgres` container.
  If you set the `all` special item, it will be mounted in all containers (postgres + sidecars).
  Else you can set the list of target containers in which the additional volumes will be mounted (eg : postgres, telegraf)

## Prepared Databases

The operator can create databases with default owner, reader and writer roles
without the need to specify them under `users` or `databases` sections. Those
parameters are grouped under the `preparedDatabases` top-level key. For more
information, see [user docs](../user.md#prepared-databases-with-roles-and-default-privileges).

* **defaultUsers**
  The operator will always create default `NOLOGIN` roles for defined prepared
  databases, but if `defaultUsers` is set to `true` three additional `LOGIN`
  roles with `_user` suffix will get created. Default is `false`.

* **extensions**
  map of extensions with target database schema that the operator will install
  in the database. Optional.

* **schemas**
  map of schemas that the operator will create. Optional - if no schema is
  listed, the operator will create a schema called `data`. Under each schema
  key, it can be defined if `defaultRoles` (NOLOGIN) and `defaultUsers` (LOGIN)
  roles shall be created that have schema-exclusive privileges.
  By default, `defaultRoles` is `true` and `defaultUsers` is false.

* **secretNamespace**
  for each default LOGIN role the operator will create a secret. You can
  specify the namespace in which these secrets will get created, if
  `enable_cross_namespace_secret` is set to `true` in the config. Otherwise,
  the cluster namespace is used.

## Postgres parameters

Those parameters are grouped under the `postgresql` top-level key, which is
required in the manifest.

* **version**
  the Postgres major version of the cluster. Looks at the [Spilo
  project](https://github.com/zalando/spilo/releases) for the list of supported
  versions. Changing the cluster version once the cluster has been bootstrapped
  is not supported. Required field.

* **parameters**
  a dictionary of Postgres parameter names and values to apply to the resulting
  cluster. Optional (Spilo automatically sets reasonable defaults for parameters
  like `work_mem` or `max_connections`).

## Patroni parameters

Those parameters are grouped under the `patroni` top-level key. See the [Patroni
documentation](https://patroni.readthedocs.io/en/latest/SETTINGS.html) for the
explanation of `ttl` and `loop_wait` parameters.

* **initdb**
  a map of key-value pairs describing initdb parameters. For `data-checksums`,
  `debug`, `no-locale`, `noclean`, `nosync` and `sync-only` parameters use
  `true` as the value if you want to set them. Changes to this option do not
  affect the already initialized clusters. Optional.

* **pg_hba**
  list of custom `pg_hba` lines to replace default ones. Note that the default
  ones include

  ```
  hostssl all +pamrole all pam
  ```
  where pamrole is the name of the role for the pam authentication; any
  custom `pg_hba` should include the pam line to avoid breaking pam
  authentication. Optional.

* **pg_ident**
  list of custom `pg_ident` lines defining user name maps for external
  authentication methods (e.g. cert, gss, peer). Each line is of the form
  `mapname system-username pg-username`. Patroni manages `pg_ident.conf`
  and reloads PostgreSQL when this list changes. Optional.

* **ttl**
  Patroni `ttl` parameter value, optional. The default is set by the Spilo
  Docker image. Optional.

* **loop_wait**
  Patroni `loop_wait` parameter value, optional. The default is set by the
  Spilo Docker image. Optional.

* **retry_timeout**
  Patroni `retry_timeout` parameter value, optional. The default is set by the
  Spilo Docker image. Optional.

* **maximum_lag_on_failover**
  Patroni `maximum_lag_on_failover` parameter value, optional. The default is
  set by the Spilo Docker image. Optional.

* **slots**
  permanent replication slots that Patroni preserves after failover by
  re-creating them on the new primary immediately after doing a promote. Slots
  could be reconfigured with the help of `patronictl edit-config`. It is the
  responsibility of a user to avoid clashes in names between replication slots
  automatically created by Patroni for cluster members and permanent replication
  slots. Optional.

* **synchronous_mode**
  Patroni `synchronous_mode` parameter value. The default is set to `false`. Optional.

* **synchronous_mode_strict**
  Patroni `synchronous_mode_strict` parameter value. Can be used in addition to `synchronous_mode`. The default is set to `false`. Optional.

* **synchronous_node_count**
  Patroni `synchronous_node_count` parameter value. Note, this option is only available for Spilo images with Patroni 2.0+. The default is set to `1`. Optional.

* **failsafe_mode**
  Patroni `failsafe_mode` parameter value. If enabled, Patroni will cope
  with DCS outages by avoiding leader demotion. See the Patroni documentation
  [here](https://patroni.readthedocs.io/en/master/dcs_failsafe_mode.html) for more details.
  This feature is included since Patroni 3.0.0. Hence, check the container
  image in use if this feature is included in the used Patroni version. The
  default is set to `false`. Optional. 
  
## Postgres container resources

Those parameters define [CPU and memory requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/)
for the Postgres container. They are grouped under the `resources` top-level
key with subgroups `requests` and `limits`. 

### Requests

CPU and memory requests for the Postgres container.

* **cpu**
  CPU requests for the Postgres container. Optional, overrides the
  `default_cpu_requests` operator configuration parameter.

* **memory**
  memory requests for the Postgres container. Optional, overrides the
  `default_memory_request` operator configuration parameter.

* **hugepages-2Mi**
  hugepages-2Mi requests for the sidecar container.
  Optional, defaults to not set.

* **hugepages-1Gi**
  1Gi hugepages requests for the sidecar container.
  Optional, defaults to not set.

### Limits

CPU and memory limits for the Postgres container.

* **cpu**
  CPU limits for the Postgres container. Optional, overrides the
  `default_cpu_limits` operator configuration parameter.

* **memory**
  memory limits for the Postgres container. Optional, overrides the
  `default_memory_limits` operator configuration parameter.

* **hugepages-2Mi**
  hugepages-2Mi requests for the sidecar container.
  Optional, defaults to not set.

* **hugepages-1Gi**
  1Gi hugepages requests for the sidecar container.
  Optional, defaults to not set.

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

* **s3_wal_path**
  the url to S3 bucket containing the WAL archive of the cluster to be cloned.
  Optional.

* **s3_endpoint**
  the url of the S3-compatible service should be set when cloning from non AWS
  S3. Optional.

* **s3_access_key_id**
  the access key id, used for authentication on S3 service. Optional.

* **s3_secret_access_key**
  the secret access key, used for authentication on S3 service. Optional.

* **s3_force_path_style**
  to enable path-style addressing(i.e., http://s3.amazonaws.com/BUCKET/KEY)
  when connecting to an S3-compatible service that lack of support for
  sub-domain style bucket URLs (i.e., http://BUCKET.s3.amazonaws.com/KEY).
  Optional.

## Standby cluster

On startup, an existing `standby` top-level key creates a standby Postgres
cluster streaming from a remote location - either from a S3 or GCS WAL
archive, a remote primary, or a combination of both. At least one of
`s3_wal_path`, `gs_wal_path`, or `standby_host` must be specified.
Note that `s3_wal_path` and `gs_wal_path` are mutually exclusive.

* **s3_wal_path**
  the url to S3 bucket containing the WAL archive of the remote primary.
  Can be combined with `standby_host` for additional redundancy.

* **gs_wal_path**
  the url to GS bucket containing the WAL archive of the remote primary.
  Can be combined with `standby_host` for additional redundancy.

* **standby_host**
  hostname or IP address of the primary to stream from.
  Can be specified alone or combined with either `s3_wal_path` or `gs_wal_path`.

* **standby_port**
  TCP port on which the primary is listening for connections. Patroni will
  use `"5432"` if not set.

* **standby_primary_slot_name**
  name of the replication slot to use on the primary server when streaming
  from a remote primary. See the Patroni documentation
  [here](https://patroni.readthedocs.io/en/latest/standby_cluster.html) for more details. Optional.

## Volume properties

Those parameters are grouped under the `volume` top-level key and define the
properties of the persistent storage that stores Postgres data.

* **size**
  the size of the target volume. Usual Kubernetes size modifiers, i.e. `Gi`
  or `Mi`, apply. Required.

* **storageClass**
  the name of the Kubernetes storage class to draw the persistent volume from.
  See [Kubernetes
  documentation](https://kubernetes.io/docs/concepts/storage/storage-classes/)
  for the details on storage classes. Optional.

* **subPath**
  Subpath to use when mounting volume into Spilo container. Optional.

* **isSubPathExpr**
  Set it to true if the specified subPath is an expression. Optional.

* **iops**
  When running the operator on AWS the latest generation of EBS volumes (`gp3`)
  allows for configuring the number of IOPS. Maximum is 80000. Optional.

* **throughput**
  When running the operator on AWS the latest generation of EBS volumes (`gp3`)
  allows for configuring the throughput in MB/s. Maximum is 2000. Optional.

* **selector**
  A label query over PVs to consider for binding. See the [Kubernetes 
  documentation](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/)
  for details on using `matchLabels` and `matchExpressions`. Optional

## Sidecar definitions

Those parameters are defined under the `sidecars` key. They consist of a list
of dictionaries, each defining one sidecar (an extra container running
along the main Postgres container on the same pod). The following keys can be
defined in the sidecar dictionary:

* **name**
  name of the sidecar. Required.

* **image**
  Docker image of the sidecar. Required.

* **env**
  a dictionary of environment variables. Use usual Kubernetes definition
  (https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/)
  for environment variables. Optional.

* **resources**
  [CPU and memory requests and limits](https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container)
  for each sidecar container. Optional.

### Requests

CPU and memory requests for the sidecar container.

* **cpu**
  CPU requests for the sidecar container. Optional, overrides the
  `default_cpu_requests` operator configuration parameter. Optional.

* **memory**
  memory requests for the sidecar container. Optional, overrides the
  `default_memory_request` operator configuration parameter. Optional.

* **hugepages-2Mi**
  hugepages-2Mi requests for the sidecar container.
  Optional, defaults to not set.

* **hugepages-1Gi**
  1Gi hugepages requests for the sidecar container.
  Optional, defaults to not set.

### Limits

CPU and memory limits for the sidecar container.

* **cpu**
  CPU limits for the sidecar container. Optional, overrides the
  `default_cpu_limits` operator configuration parameter. Optional.

* **memory**
  memory limits for the sidecar container. Optional, overrides the
  `default_memory_limits` operator configuration parameter. Optional.

* **hugepages-2Mi**
  hugepages-2Mi requests for the sidecar container.
  Optional, defaults to not set.

* **hugepages-1Gi**
  1Gi hugepages requests for the sidecar container.
  Optional, defaults to not set.

## Connection pooler

Parameters are grouped under the `connectionPooler` top-level key and specify
configuration for connection pooler. If this section is not empty, a connection
pooler will be created for master service only even if `enableConnectionPooler`
is not present. But if this section is present then it defines the configuration
for both master and replica pooler services (if `enableReplicaConnectionPooler`
 is enabled).

* **numberOfInstances**
  How many instances of connection pooler to create.

* **schema**
  Database schema to create for credentials lookup function.

* **user**
  User to create for connection pooler to be able to connect to a database.
  You can also choose a role from the `users` section or a system user role.

* **dockerImage**
  Which docker image to use for connection pooler deployment.

* **maxDBConnections**
  How many connections the pooler can max hold. This value is divided among the
  pooler pods.

* **mode**
  In which mode to run connection pooler, transaction or session.

* **resources**
  Resource configuration for connection pooler deployment.

## Custom TLS certificates

Those parameters are grouped under the `tls` top-level key. Note, you have to
define `spiloFSGroup` in the Postgres cluster manifest or `spilo_fsgroup` in
the global configuration before adding the `tls` section'.

* **secretName**
  By setting the `secretName` value, the cluster will switch to load the given
  Kubernetes Secret into the container as a volume and uses that as the
  certificate instead. It is up to the user to create and manage the
  Kubernetes Secret either by hand or using a tool like the CertManager
  operator.

* **certificateFile**
  Filename of the certificate. Defaults to "tls.crt".

* **privateKeyFile**
  Filename of the private key. Defaults to "tls.key".

* **caFile**
  Optional filename to the CA certificate (e.g. "ca.crt"). Useful when the
  client connects with `sslmode=verify-ca` or `sslmode=verify-full`.
  Default is empty.

* **caSecretName**
  By setting the `caSecretName` value, the ca certificate file defined by the
  `caFile` will be fetched from this secret instead of `secretName` above.
  This secret has to hold a file with that name in its root.

  Optionally one can provide full path for any of them. By default it is
  relative to the "/tls/", which is mount path of the tls secret.
  If `caSecretName` is defined, the ca.crt path is relative to "/tlsca/",
  otherwise to the same "/tls/".

## Change data capture streams

This sections enables change data capture (CDC) streams via Postgres' 
[logical decoding](https://www.postgresql.org/docs/18/logicaldecoding.html)
feature and `pgoutput` plugin. While the Postgres operator takes responsibility
for providing the setup to publish change events, it relies on external tools
to consume them. At Zalando, we are using a workflow based on
[Debezium Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
which can feed streams into Zalando’s distributed event broker [Nakadi](https://nakadi.io/)
among others.

The Postgres Operator creates custom resources for Zalando's internal CDC
operator which will be used to set up the consumer part. Each stream object
can have the following properties:

* **applicationId**
  The application name to which the database and CDC belongs to. For each
  set of streams with a distinct `applicationId` a separate stream resource as
  well as a separate logical replication slot will be created. This means there
  can be different streams in the same database and streams with the same
  `applicationId` are bundled in one stream resource. The stream resource will
  be called like the Postgres cluster plus "-<applicationId>" suffix. Required.

* **database**
  Name of the database from where events will be published via Postgres'
  logical decoding feature. The operator will take care of updating the
  database configuration (setting `wal_level: logical`, creating logical
  replication slots, using output plugin `pgoutput` and creating a dedicated
  replication user). Required.

* **tables**
  Defines a map of table names and their properties (`eventType`, `idColumn`
  and `payloadColumn`). Required.
  The CDC operator is following the [outbox pattern](https://debezium.io/blog/2019/02/19/reliable-microservices-data-exchange-with-the-outbox-pattern/).
  The application is responsible for putting events into a (JSON/B or VARCHAR)
  payload column of the outbox table in the structure of the specified target
  event type. The operator will create a [PUBLICATION](https://www.postgresql.org/docs/18/logical-replication-publication.html)
  in Postgres for all tables specified for one `database` and `applicationId`.
  The CDC operator will consume from it shortly after transactions are
  committed to the outbox table. The `idColumn` will be used in telemetry for
  the CDC operator. The names for `idColumn` and `payloadColumn` can be
  configured. Defaults are `id` and `payload`. The target `eventType` has to
  be defined. One can also specify a `recoveryEventType` that will be used
  for a dead letter queue. By enabling `ignoreRecovery`, you can choose to
  ignore failing events.

* **filter**
  Streamed events can be filtered by a jsonpath expression for each table.
  Optional.

* **enableRecovery**
  Flag to enable a dead letter queue recovery for all streams tables.
  Alternatively, recovery can also be enable for single outbox tables by only
  specifying a `recoveryEventType` and no `enableRecovery` flag. When set to
  false or missing, events will be retried until consuming succeeded. You can
  use a `filter` expression to get rid of poison pills. Optional.

* **batchSize**
  Defines the size of batches in which events are consumed. Optional.
  Defaults to 1.

* **cpu**
  CPU requests to be set as an annotation on the stream resource. Optional.

* **memory**
  memory requests to be set as an annotation on the stream resource. Optional.
