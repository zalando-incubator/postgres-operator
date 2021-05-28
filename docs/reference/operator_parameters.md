<h1>Configuration parameters</h1>

There are two mutually-exclusive methods to set the Postgres Operator
configuration.

* ConfigMaps-based, the legacy one.  The configuration is supplied in a
  key-value configmap, defined by the `CONFIG_MAP_NAME` environment variable.
  Non-scalar values, i.e. lists or maps, are encoded in the value strings using
  the comma-based syntax for lists and coma-separated `key:value` syntax for
  maps. String values containing ':' should be enclosed in quotes. The
  configuration is flat, parameter group names below are not reflected in the
  configuration structure. There is an
  [example](../../manifests/configmap.yaml)

* CRD-based configuration. The configuration is stored in a custom YAML
  manifest. The manifest is an instance of the custom resource definition (CRD)
  called `OperatorConfiguration`. The operator registers this CRD during the
  start and uses it for configuration if the [operator deployment manifest](../../manifests/postgres-operator.yaml#L36)
  sets the `POSTGRES_OPERATOR_CONFIGURATION_OBJECT` env variable to a non-empty
  value. The variable should point to the `postgresql-operator-configuration`
  object in the operator's namespace.

  The CRD-based configuration is a regular YAML document; non-scalar keys are
  simply represented in the usual YAML way. There are no default values built-in
  in the operator, each parameter that is not supplied in the configuration
  receives an empty value. In order to create your own configuration just copy
  the [default one](../../manifests/postgresql-operator-default-configuration.yaml)
  and change it.

  To test the CRD-based configuration locally, use the following
  ```bash
  kubectl create -f manifests/operatorconfiguration.crd.yaml # registers the CRD
  kubectl create -f manifests/postgresql-operator-default-configuration.yaml

  kubectl create -f manifests/operator-service-account-rbac.yaml
  kubectl create -f manifests/postgres-operator.yaml # set the env var as mentioned above

  kubectl get operatorconfigurations postgresql-operator-default-configuration -o yaml
  ```

The CRD-based configuration is more powerful than the one based on ConfigMaps
and should be used unless there is a compatibility requirement to use an already
existing configuration. Even in that case, it should be rather straightforward
to convert the ConfigMap-based configuration into the CRD-based one and restart
the operator. The ConfigMap-based configuration will be deprecated and
subsequently removed in future releases.

Note that for the CRD-based configuration groups of configuration options below
correspond to the non-leaf keys in the target YAML (i.e. for the Kubernetes
resources the key is `kubernetes`). The key is mentioned alongside the group
description. The ConfigMap-based configuration is flat and does not allow
non-leaf keys.

Since in the CRD-based case the operator needs to create a CRD first, which is
controlled by the `resource_check_interval` and `resource_check_timeout`
parameters, those parameters have no effect and are replaced by the
`CRD_READY_WAIT_INTERVAL` and `CRD_READY_WAIT_TIMEOUT` environment variables.
They will be deprecated and removed in the future.

For the configmap configuration, the [default parameter values](../../pkg/util/config/config.go#L14)
mentioned here are likely to be overwritten in your local operator installation
via your local version of the operator configmap. In the case you use the
operator CRD, all the CRD defaults are provided in the
[operator's default configuration manifest](../../manifests/postgresql-operator-default-configuration.yaml)

Variable names are underscore-separated words.


## General

Those are top-level keys, containing both leaf keys and groups.

* **enable_crd_validation**
  toggles if the operator will create or update CRDs with
  [OpenAPI v3 schema validation](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#validation)
  The default is `true`.

* **enable_lazy_spilo_upgrade**
  Instruct operator to update only the statefulsets with new images (Spilo and InitContainers) without immediately doing the rolling update. The assumption is pods will be re-started later with new images, for example due to the node rotation.
  The default is `false`.

* **enable_pgversion_env_var**
  With newer versions of Spilo, it is preferable to use `PGVERSION` pod environment variable instead of the setting `postgresql.bin_dir` in the `SPILO_CONFIGURATION` env variable. When this option is true, the operator sets `PGVERSION` and omits `postgresql.bin_dir` from  `SPILO_CONFIGURATION`. When false, the `postgresql.bin_dir` is set. This setting takes precedence over `PGVERSION`; see PR 222 in Spilo. The default is `true`.

* **enable_spilo_wal_path_compat**
  enables backwards compatible path between Spilo 12 and Spilo 13 images. The default is `false`.

* **etcd_host**
  Etcd connection string for Patroni defined as `host:port`. Not required when
  Patroni native Kubernetes support is used. The default is empty (use
  Kubernetes-native DCS).

* **kubernetes_use_configmaps**
  Select if setup uses endpoints (default), or configmaps to manage leader when
  DCS is kubernetes (not etcd or similar). In OpenShift it is not possible to
  use endpoints option, and configmaps is required. By default,
  `kubernetes_use_configmaps: false`, meaning endpoints will be used.

* **docker_image**
  Spilo Docker image for Postgres instances. For production, don't rely on the
  default image, as it might be not the most up-to-date one. Instead, build
  your own Spilo image from the [github
  repository](https://github.com/zalando/spilo).

* **sidecar_docker_images**
  *deprecated*: use **sidecars** instead. A map of sidecar names to Docker
  images to run with Spilo. In case of the name conflict with the definition in
  the cluster manifest the cluster-specific one is preferred.

* **sidecars**
  a list of sidecars to run with Spilo, for any cluster (i.e. globally defined
  sidecars). Each item in the list is of type
  [Container](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#container-v1-core).
  Globally defined sidecars can be overwritten by specifying a sidecar in the
  Postgres manifest with the same name.
  Note: This field is not part of the schema validation. If the container
  specification is invalid, then the operator fails to create the statefulset.

* **enable_shm_volume**
  Instruct operator to start any new database pod without limitations on shm
  memory. If this option is enabled, to the target database pod will be mounted
  a new tmpfs volume to remove shm memory limitation (see e.g. the
  [docker issue](https://github.com/docker-library/postgres/issues/416)).
  This option is global for an operator object, and can be overwritten by
  `enableShmVolume` parameter from Postgres manifest. The default is `true`.

* **workers**
  number of working routines the operator spawns to process requests to
  create/update/delete/sync clusters concurrently. The default is `8`.

* **max_instances**
  operator will cap the number of instances in any managed Postgres cluster up
  to the value of this parameter. When `-1` is specified, no limits are applied.
  The default is `-1`.

* **min_instances**
  operator will run at least the number of instances for any given Postgres
  cluster equal to the value of this parameter. Standby clusters can still run
  with `numberOfInstances: 1` as this is the [recommended setup](../user.md#setting-up-a-standby-cluster).
  When `-1` is specified for `min_instances`, no limits are applied. The default
  is `-1`.

* **resync_period**
  period between consecutive sync requests. The default is `30m`.

* **repair_period**
  period between consecutive repair requests. The default is `5m`.

* **set_memory_request_to_limit**
  Set `memory_request` to `memory_limit` for all Postgres clusters (the default
  value is also increased). This prevents certain cases of memory overcommitment
  at the cost of overprovisioning memory and potential scheduling problems for
  containers with high memory limits due to the lack of memory on Kubernetes
  cluster nodes. This affects all containers created by the operator (Postgres,
  Scalyr sidecar, and other sidecars except **sidecars** defined in the operator
  configuration); to set resources for the operator's own container, change the
  [operator deployment manually](../../manifests/postgres-operator.yaml#L20).
  The default is `false`.

## Postgres users

Parameters describing Postgres users. In a CRD-configuration, they are grouped
under the `users` key.

* **super_username**
  Postgres `superuser` name to be created by `initdb`. The default is
  `postgres`.

* **replication_username**
  Postgres username used for replication between instances. The default is
  `standby`.

## Major version upgrades

Parameters configuring automatic major version upgrades. In a 
CRD-configuration, they are grouped under the `major_version_upgrade` key.

* **major_version_upgrade_mode**
  Postgres Operator supports [in-place major version upgrade](../administrator.md#in-place-major-version-upgrade) 
  with three different modes:
  `"off"` = no upgrade by the operator,
  `"manual"` = manifest triggers action,
  `"full"` = manifest and minimal version violation trigger upgrade.
  Note, that with all three modes increasing the version in the manifest will
  trigger a rolling update of the pods. The default is `"off"`.

* **minimal_major_version**
  The minimal Postgres major version that will not automatically be upgraded
  when `major_version_upgrade_mode` is set to `"full"`. The default is `"9.5"`.

* **target_major_version**
  The target Postgres major version when upgrading clusters automatically
  which violate the configured allowed `minimal_major_version` when
  `major_version_upgrade_mode` is set to `"full"`. The default is `"13"`.

## Kubernetes resources

Parameters to configure cluster-related Kubernetes objects created by the
operator, as well as some timeouts associated with them. In a CRD-based
configuration they are grouped under the `kubernetes` key.

* **pod_service_account_name**
  service account used by Patroni running on individual Pods to communicate
  with the operator. Required even if native Kubernetes support in Patroni is
  not used, because Patroni keeps pod labels in sync with the instance role.
  The default is `postgres-pod`.

* **pod_service_account_definition**
  On Postgres cluster creation the operator tries to create the service account
  for the Postgres pods if it does not exist in the namespace. The internal
  default service account definition (defines only the name) can be overwritten
  with this parameter. Make sure to provide a valid YAML or JSON string. The
  default is empty.

* **pod_service_account_role_binding_definition**
  This definition must bind the pod service account to a role with permission
  sufficient for the pods to start and for Patroni to access K8s endpoints;
  service account on its own lacks any such rights starting with K8s v1.8. If
  not explicitly defined by the user, a simple definition that binds the
  account to the 'postgres-pod' [cluster role](../../manifests/operator-service-account-rbac.yaml#L198)
  will be used. The default is empty.

* **pod_terminate_grace_period**
  Postgres pods are [terminated forcefully](https://kubernetes.io/docs/concepts/workloads/pods/pod/#termination-of-pods)
  after this timeout. The default is `5m`.

* **custom_pod_annotations**
  This key/value map provides a list of annotations that get attached to each pod
  of a database created by the operator. If the annotation key is also provided
  by the database definition, the database definition value is used.

* **delete_annotation_date_key**
  key name for annotation that compares manifest value with current date in the
  YYYY-MM-DD format. Allowed pattern: `'([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]'`.
  The default is empty which also disables this delete protection check.

* **delete_annotation_name_key**
  key name for annotation that compares manifest value with Postgres cluster name.
  Allowed pattern: `'([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]'`. The default is
  empty which also disables this delete protection check.

* **downscaler_annotations**
  An array of annotations that should be passed from Postgres CRD on to the
  statefulset and, if exists, to the connection pooler deployment as well.
  Regular expressions like `downscaler/*` etc. are also accepted. Can be used
  with [kube-downscaler](https://github.com/hjacobs/kube-downscaler).

* **watched_namespace**
  The operator watches for Postgres objects in the given namespace. If not
  specified, the value is taken from the operator namespace. A special `*`
  value makes it watch all namespaces. The default is empty (watch the operator
  pod namespace).

* **pdb_name_format**
  defines the template for PDB (Pod Disruption Budget) names created by the
  operator. The default is `postgres-{cluster}-pdb`, where `{cluster}` is
  replaced by the cluster name. Only the `{cluster}` placeholders is allowed in
  the template.

* **enable_pod_disruption_budget**
  PDB is enabled by default to protect the cluster from voluntarily disruptions
  and hence unwanted DB downtime. However, on some cloud providers it could be
  necessary to temporarily disabled it, e.g. for node updates. See
  [admin docs](../administrator.md#pod-disruption-budget) for more information.
  Default is true.

* **enable_init_containers**
  global option to allow for creating init containers in the cluster manifest to
  run actions before Spilo is started. Default is true.

* **enable_sidecars**
  global option to allow for creating sidecar containers in the cluster manifest
  to run alongside Spilo on the same pod. Globally defined sidecars are always
  enabled. Default is true.

* **secret_name_template**
  a template for the name of the database user secrets generated by the
  operator. `{username}` is replaced with name of the secret, `{cluster}` with
  the name of the cluster, `{tprkind}` with the kind of CRD (formerly known as
  TPR) and `{tprgroup}` with the group of the CRD. No other placeholders are
  allowed. The default is
  `{username}.{cluster}.credentials.{tprkind}.{tprgroup}`.

* **cluster_domain**
  defines the default DNS domain for the kubernetes cluster the operator is
  running in. The default is `cluster.local`. Used by the operator to connect
  to the Postgres clusters after creation.

* **oauth_token_secret_name**
  namespaced name of the secret containing the `OAuth2` token to pass to the
  teams API. The default is `postgresql-operator`.

* **infrastructure_roles_secret_name**
  *deprecated*: namespaced name of the secret containing infrastructure roles
  with user names, passwords and role membership.

* **infrastructure_roles_secrets**
  array of infrastructure role definitions which reference existing secrets
  and specify the key names from which user name, password and role membership
  are extracted. For the ConfigMap this has to be a string which allows
  referencing only one infrastructure roles secret. The default is empty.

* **inherited_annotations**
  list of annotation keys that can be inherited from the cluster manifest, and
  added to each child objects  (`Deployment`, `StatefulSet`, `Pod`, `PDB` and
  `Services`) created by the operator incl. the ones from the connection
  pooler deployment. The default is empty.

* **pod_role_label**
  name of the label assigned to the Postgres pods (and services/endpoints) by
  the operator. The default is `spilo-role`.

* **cluster_labels**
  list of `name:value` pairs for additional labels assigned to the cluster
  objects. The default is `application:spilo`.

* **inherited_labels**
  list of label keys that can be inherited from the cluster manifest, and
  added to each child objects (`Deployment`, `StatefulSet`, `Pod`, `PVCs`,
  `PDB`, `Service`, `Endpoints` and `Secrets`) created by the operator.
  Typical use case is to dynamically pass labels that are specific to a
  given Postgres cluster, in order to implement `NetworkPolicy`. The default
  is empty.

* **cluster_name_label**
  name of the label assigned to Kubernetes objects created by the operator
  that indicates which cluster a given object belongs to. The default is
  `cluster-name`.

* **node_readiness_label**
  a set of labels that a running and active node should possess to be
  considered `ready`. The operator uses values of those labels to detect the
  start of the Kubernetes cluster upgrade procedure and move master pods off
  the nodes to be decommissioned. When the set is not empty, the operator also
  assigns the `Affinity` clause to the Postgres pods to be scheduled only on
  `ready` nodes. The default is empty.

* **toleration**
  a dictionary that should contain `key`, `operator`, `value` and
  `effect` keys. In that case, the operator defines a pod toleration
  according to the values of those keys. See [kubernetes documentation](https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/)
  for details on taints and tolerations. The default is empty.

* **pod_environment_configmap**
  namespaced name of the ConfigMap with environment variables to populate on
  every pod. Right now this ConfigMap is searched in the namespace of the
  Postgres cluster. All variables from that ConfigMap are injected to the pod's
  environment, on conflicts they are overridden by the environment variables
  generated by the operator. The default is empty.

* **pod_priority_class_name**
  a name of the [priority class](https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/#priorityclass)
  that should be assigned to the Postgres pods. The priority class itself must
  be defined in advance. Default is empty (use the default priority class).

* **spilo_runasuser**
  sets the user ID which should be used in the container to run the process.
  This must be set to run the container without root. By default the container
  runs with root. This option only works for Spilo versions >= 1.6-p3.

* **spilo_runasgroup**
  sets the group ID which should be used in the container to run the process.
  This must be set to run the container without root. By default the container
  runs with root. This option only works for Spilo versions >= 1.6-p3.

* **spilo_fsgroup**
  the Persistent Volumes for the Spilo pods in the StatefulSet will be owned and
  writable by the group ID specified. This is required to run Spilo as a
  non-root process, but requires a custom Spilo image. Note the FSGroup of a Pod
  cannot be changed without recreating a new Pod.

* **spilo_privileged**
  whether the Spilo container should run in privileged mode. Privileged mode is
  used for AWS volume resizing and not required if you don't need that
  capability. The default is `false`.

* **spilo_allow_privilege_escalation**
  Controls whether a process can gain more privileges than its parent
  process. Required by cron which needs setuid. Without this parameter,
  certification rotation & backups will not be done. The default is `true`.

* **additional_pod_capabilities**
  list of additional capabilities to be added to the postgres container's
  SecurityContext (e.g. SYS_NICE etc.). Please, make sure first that the
  PodSecruityPolicy allows the capabilities listed here. Otherwise, the
  container will not start. The default is empty.

* **master_pod_move_timeout**
  The period of time to wait for the success of migration of master pods from
  an unschedulable node. The migration includes Patroni switchovers to
  respective replicas on healthy nodes. The situation where master pods still
  exist on the old node after this timeout expires has to be fixed manually.
  The default is 20 minutes.

* **enable_pod_antiaffinity**
  toggles [pod anti affinity](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/)
  on the Postgres pods, to avoid multiple pods of the same Postgres cluster in
  the same topology , e.g. node. The default is `false`.

* **pod_antiaffinity_topology_key**
  override [topology key](https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#built-in-node-labels)
  for pod anti affinity. The default is `kubernetes.io/hostname`.

* **pod_management_policy**
  specify the [pod management policy](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-management-policies)
  of stateful sets of PG clusters. The default is `ordered_ready`, the second
  possible value is `parallel`.

* **storage_resize_mode**
  defines how operator handles the difference between the requested volume size and
    the actual size. Available options are:
    1. `ebs` : operator resizes EBS volumes directly and executes `resizefs` within a pod
    2. `pvc` : operator only changes PVC definition
    3. `off` : disables resize of the volumes.
    4. `mixed` :operator  uses AWS API to adjust size, throughput, and IOPS, and calls pvc change for file system resize
    Default is "pvc".

## Kubernetes resource requests

This group allows you to configure resource requests for the Postgres pods.
Those parameters are grouped under the `postgres_pod_resources` key in a
CRD-based configuration.

* **default_cpu_request**
  CPU request value for the Postgres containers, unless overridden by
  cluster-specific settings. The default is `100m`.

* **default_memory_request**
  memory request value for the Postgres containers, unless overridden by
  cluster-specific settings. The default is `100Mi`.

* **default_cpu_limit**
  CPU limits for the Postgres containers, unless overridden by cluster-specific
  settings. The default is `1`.

* **default_memory_limit**
  memory limits for the Postgres containers, unless overridden by cluster-specific
  settings. The default is `500Mi`.

* **min_cpu_limit**
  hard CPU minimum what we consider to be required to properly run Postgres
  clusters with Patroni on Kubernetes. The default is `250m`.

* **min_memory_limit**
  hard memory minimum what we consider to be required to properly run Postgres
  clusters with Patroni on Kubernetes. The default is `250Mi`.

## Operator timeouts

This set of parameters define various timeouts related to some operator
actions, affecting pod operations and CRD creation. In the CRD-based
configuration `resource_check_interval` and `resource_check_timeout` have no
effect, and the parameters are grouped under the `timeouts` key in the
CRD-based configuration.

* **resource_check_interval**
  interval to wait between consecutive attempts to check for the presence of
  some Kubernetes resource (i.e. `StatefulSet` or `PodDisruptionBudget`). The
  default is `3s`.

* **resource_check_timeout**
  timeout when waiting for the presence of a certain Kubernetes resource (i.e.
  `StatefulSet` or `PodDisruptionBudget`) before declaring the operation
  unsuccessful. The default is `10m`.

* **pod_label_wait_timeout**
  timeout when waiting for the pod role and cluster labels. Bigger value gives
  Patroni more time to start the instance; smaller makes the operator detect
  possible issues faster. The default is `10m`.

* **pod_deletion_wait_timeout**
  timeout when waiting for the Postgres pods to be deleted when removing the
  cluster or recreating pods. The default is `10m`.

* **ready_wait_interval**
  the interval between consecutive attempts waiting for the `postgresql` CRD to
  be created. The default is `5s`.

* **ready_wait_timeout**
  the timeout for the complete `postgresql` CRD creation. The default is `30s`.

## Load balancer related options

Those options affect the behavior of load balancers created by the operator.
In the CRD-based configuration they are grouped under the `load_balancer` key.

* **custom_service_annotations**
  This key/value map provides a list of annotations that get attached to each
  service of a cluster created by the operator. If the annotation key is also
  provided by the cluster definition, the manifest value is used.
  Optional.

* **db_hosted_zone**
  DNS zone for the cluster DNS name when the load balancer is configured for
  the cluster. Only used when combined with
  [external-dns](https://github.com/kubernetes-incubator/external-dns) and with
  the cluster that has the load balancer enabled. The default is
  `db.example.com`.

* **enable_master_load_balancer**
  toggles service type load balancer pointing to the master pod of the cluster.
  Can be overridden by individual cluster settings. The default is `true`.

* **enable_replica_load_balancer**
  toggles service type load balancer pointing to the replica pod of the
  cluster.  Can be overridden by individual cluster settings. The default is
  `false`.

* **external_traffic_policy** defines external traffic policy for load
  balancers. Allowed values are `Cluster` (default) and `Local`.

* **master_dns_name_format** defines the DNS name string template for the
  master load balancer cluster.  The default is
  `{cluster}.{team}.{hostedzone}`, where `{cluster}` is replaced by the cluster
  name, `{team}` is replaced with the team name and `{hostedzone}` is replaced
  with the hosted zone (the value of the `db_hosted_zone` parameter). No other
  placeholders are allowed.

* **replica_dns_name_format** defines the DNS name string template for the
  replica load balancer cluster.  The default is
  `{cluster}-repl.{team}.{hostedzone}`, where `{cluster}` is replaced by the
  cluster name, `{team}` is replaced with the team name and `{hostedzone}` is
  replaced with the hosted zone (the value of the `db_hosted_zone` parameter).
  No other placeholders are allowed.

## AWS or GCP interaction

The options in this group configure operator interactions with non-Kubernetes
objects from Amazon Web Services (AWS) or Google Cloud Platform (GCP). They have
no effect unless you are using either. In the CRD-based configuration those
options are grouped under the `aws_or_gcp` key. Note the GCP integration is not
yet officially supported.

* **wal_s3_bucket**
  S3 bucket to use for shipping WAL segments with WAL-E. A bucket has to be
  present and accessible by Postgres pods. At the moment, supported services by
  Spilo are S3 and GCS. The default is empty.

* **wal_gs_bucket**
  GCS bucket to use for shipping WAL segments with WAL-E. A bucket has to be
  present and accessible by Postgres pods. Note, only the name of the bucket is
  required. At the moment, supported services by Spilo are S3 and GCS.
  The default is empty.

* **gcp_credentials**
  Used to set the GOOGLE_APPLICATION_CREDENTIALS environment variable for the pods.
  This is used in with conjunction with the `additional_secret_mount` and
  `additional_secret_mount_path` to properly set the credentials for the spilo
  containers. This will allow users to use specific
  [service accounts](https://cloud.google.com/kubernetes-engine/docs/tutorials/authenticating-to-cloud-platform).
  The default is empty

* **log_s3_bucket**
  S3 bucket to use for shipping Postgres daily logs. Works only with S3 on AWS.
  The bucket has to be present and accessible by Postgres pods. The default is
  empty.

* **kube_iam_role**
  AWS IAM role to supply in the `iam.amazonaws.com/role` annotation of Postgres
  pods. Only used when combined with
  [kube2iam](https://github.com/jtblin/kube2iam) project on AWS. The default is
  empty.

* **aws_region**
  AWS region used to store EBS volumes. The default is `eu-central-1`.

* **additional_secret_mount**
  Additional Secret (aws or gcp credentials) to mount in the pod.
  The default is empty.

* **additional_secret_mount_path**
  Path to mount the above Secret in the filesystem of the container(s).
  The default is empty.

* **enable_ebs_gp3_migration**
  enable automatic migration on AWS from gp2 to gp3 volumes, that are smaller
  than the configured max size (see below). This ignores that EBS gp3 is by
  default only 125 MB/sec vs 250 MB/sec for gp2 >= 333GB.
  The default is `false`.

* **enable_ebs_gp3_migration_max_size**
  defines the maximum volume size in GB until which auto migration happens.
  Default is 1000 (1TB) which matches 3000 IOPS.

## Logical backup

These parameters configure a K8s cron job managed by the operator to produce
Postgres logical backups. In the CRD-based configuration those parameters are
grouped under the `logical_backup` key.

* **logical_backup_docker_image**
  An image for pods of the logical backup job. The [example image](../../docker/logical-backup/Dockerfile)
  runs `pg_dumpall` on a replica if possible and uploads compressed results to
  an S3 bucket under the key `/spilo/pg_cluster_name/cluster_k8s_uuid/logical_backups`.
  The default image is the same image built with the Zalando-internal CI
  pipeline. Default: "registry.opensource.zalan.do/acid/logical-backup:v1.6.3"

* **logical_backup_google_application_credentials**
  Specifies the path of the google cloud service account json file. Default is empty.

* **logical_backup_job_prefix**
  The prefix to be prepended to the name of a k8s CronJob running the backups. Beware the prefix counts towards the name length restrictions imposed by k8s. Empty string is a legitimate value. Operator does not do the actual renaming: It simply creates the job with the new prefix. You will have to delete the old cron job manually. Default: "logical-backup-".

* **logical_backup_provider**
  Specifies the storage provider to which the backup should be uploaded (`s3` or `gcs`).
  Default: "s3"

* **logical_backup_s3_access_key_id**
  When set, value will be in AWS_ACCESS_KEY_ID env variable. The Default is empty.

* **logical_backup_s3_bucket**
  S3 bucket to store backup results. The bucket has to be present and
  accessible by Postgres pods. Default: empty.

* **logical_backup_s3_endpoint**
  When using non-AWS S3 storage, endpoint can be set as a ENV variable. The default is empty.

* **logical_backup_s3_region**
  Specifies the region of the bucket which is required with some non-AWS S3 storage services. The default is empty.

* **logical_backup_s3_secret_access_key**
  When set, value will be in AWS_SECRET_ACCESS_KEY env variable. The Default is empty.

* **logical_backup_s3_sse**
  Specify server side encryption that S3 storage is using. If empty string
  is specified, no argument will be passed to `aws s3` command. Default: "AES256".

* **logical_backup_s3_retention_time**
  Specify a retention time for logical backups stored in S3. Backups older than the specified retention 
  time will be deleted after a new backup was uploaded. If empty, all backups will be kept. Example values are
  "3 days", "2 weeks", or "1 month". The default is empty.

* **logical_backup_schedule**
  Backup schedule in the cron format. Please take the
  [reference schedule format](https://kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#schedule)
  into account. Default: "30 00 \* \* \*"

## Debugging the operator

Options to aid debugging of the operator itself. Grouped under the `debug` key.

* **debug_logging**
  boolean parameter that toggles verbose debug logs from the operator. The
  default is `true`.

* **enable_database_access**
  boolean parameter that toggles the functionality of the operator that require
  access to the Postgres database, i.e. creating databases and users. The
  default is `true`.

## Automatic creation of human users in the database

Options to automate creation of human users with the aid of the teams API
service. In the CRD-based configuration those are grouped under the `teams_api`
key.

* **enable_teams_api**
  boolean parameter that toggles usage of the Teams API by the operator.
  The default is `true`.

* **teams_api_url**
  contains the URL of the Teams API service. There is a [demo
  implementation](https://github.com/ikitiki/fake-teams-api). The default is
  `https://teams.example.com/api/`.

* **team_api_role_configuration**
  Postgres parameters to apply to each team member role. The default is
  '*log_statement:all*'. It is possible to supply multiple options, separating
  them by commas. Options containing commas within the value are not supported,
  with the exception of the `search_path`. For instance:

  ```yaml
  teams_api_role_configuration: "log_statement:all,search_path:'data,public'"
  ```
  The default is `"log_statement:all"`

* **enable_team_superuser**
  whether to grant superuser to members of the cluster's owning team created
  from the Teams API. The default is `false`.

* **team_admin_role**
  role name to grant to team members created from the Teams API. The default is
  `admin`, that role is created by Spilo as a `NOLOGIN` role.

* **enable_admin_role_for_users**
   if `true`, the `team_admin_role` will have the rights to grant roles coming
   from PG manifests. Such roles will be created as in
   "CREATE ROLE 'role_from_manifest' ... ADMIN 'team_admin_role'".
   The default is `true`.

* **pam_role_name**
  when set, the operator will add all team member roles to this group and add a
  `pg_hba` line to authenticate members of that role via `pam`. The default is
  `zalandos`.

* **pam_configuration**
  when set, should contain a URL to use for authentication against the username
  and the token supplied as the password.  Used in conjunction with
  [pam_oauth2](https://github.com/CyberDem0n/pam-oauth2) module. The default is
  `https://info.example.com/oauth2/tokeninfo?access_token= uid
  realm=/employees`.

* **protected_role_names**
  List of roles that cannot be overwritten by an application, team or
  infrastructure role. The default is `admin`.

* **postgres_superuser_teams**
  List of teams which members need the superuser role in each PG database
  cluster to administer Postgres and maintain infrastructure built around it.
  The default is empty.

* **role_deletion_suffix**
  defines a suffix that - when `enable_team_member_deprecation` is set to
  `true` - will be appended to database role names of team members that were
  removed from either the team in the Teams API or a `PostgresTeam` custom
  resource (additionalMembers). When re-added, the operator will rename roles
  with the defined suffix back to the original role name.
  The default is `_deleted`.

* **enable_team_member_deprecation**
  if `true` database roles of former team members will be renamed by appending
  the configured `role_deletion_suffix` and `LOGIN` privilege will be revoked.
  The default is `false`.

* **enable_postgres_team_crd**
  toggle to make the operator watch for created or updated `PostgresTeam` CRDs
  and create roles for specified additional teams and members.
  The default is `false`.

* **enable_postgres_team_crd_superusers**
  in a `PostgresTeam` CRD additional superuser teams can assigned to teams that
  own clusters. With this flag set to `false`, it will be ignored.
  The default is `false`.

## Logging and REST API

Parameters affecting logging and REST API listener. In the CRD-based
configuration they are grouped under the `logging_rest_api` key.

* **api_port**
  REST API listener listens to this port. The default is `8080`.

* **ring_log_lines**
  number of lines in the ring buffer used to store cluster logs. The default is `100`.

* **cluster_history_entries**
  number of entries in the cluster history ring buffer. The default is `1000`.

## Scalyr options (*deprecated*)

Those parameters define the resource requests/limits and properties of the
scalyr sidecar. In the CRD-based configuration they are grouped under the
`scalyr` key. Note, that this section is deprecated. Instead, define Scalyr as
a global sidecar under the `sidecars` key in the configuration.

* **scalyr_api_key**
  API key for the Scalyr sidecar. The default is empty.

* **scalyr_image**
  Docker image for the Scalyr sidecar. The default is empty.

* **scalyr_server_url**
  server URL for the Scalyr sidecar. The default is `https://upload.eu.scalyr.com`.

* **scalyr_cpu_request**
  CPU request value for the Scalyr sidecar. The default is `100m`.

* **scalyr_memory_request**
  Memory request value for the Scalyr sidecar. The default is `50Mi`.

* **scalyr_cpu_limit**
  CPU limit value for the Scalyr sidecar. The default is `1`.

* **scalyr_memory_limit**
  Memory limit value for the Scalyr sidecar. The default is `500Mi`.

## Connection pooler configuration

Parameters are grouped under the `connection_pooler` top-level key and specify
default configuration for connection pooler, if a postgres manifest requests it
but do not specify some of the parameters. All of them are optional with the
operator being able to provide some reasonable defaults.

* **connection_pooler_number_of_instances**
  How many instances of connection pooler to create. Default is 2 which is also
  the required minimum.

* **connection_pooler_schema**
  Database schema to create for credentials lookup function to be used by the
  connection pooler. Is is created in every database of the Postgres cluster.
  You can also choose an existing schema. Default schema is `pooler`.

* **connection_pooler_user**
  User to create for connection pooler to be able to connect to a database.
  You can also choose an existing role, but make sure it has the `LOGIN`
  privilege. Default role is `pooler`.

* **connection_pooler_image**
  Docker image to use for connection pooler deployment.
  Default: "registry.opensource.zalan.do/acid/pgbouncer"

* **connection_pooler_max_db_connections**
  How many connections the pooler can max hold. This value is divided among the
  pooler pods. Default is 60 which will make up 30 connections per pod for the
  default setup with two instances.

* **connection_pooler_mode**
  Default pooler mode, `session` or `transaction`. Default is `transaction`.

* **connection_pooler_default_cpu_request**
  **connection_pooler_default_memory_reques**
  **connection_pooler_default_cpu_limit**
  **connection_pooler_default_memory_limit**
  Default resource configuration for connection pooler deployment. The internal
  default for memory request and limit is `100Mi`, for CPU it is `500m` and `1`.
