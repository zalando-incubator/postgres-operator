package v1

// Operator configuration CRD definition, please use snake_case for field names.

import (
	"github.com/zalando/postgres-operator/v2/pkg/util/config"

	"github.com/zalando/postgres-operator/v2/pkg/spec"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OperatorConfiguration defines the specification for the OperatorConfiguration.
// +k8s:deepcopy-gen=true
// +kubebuilder:resource:categories=all,shortName=opconfig,scope=Namespaced
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.configuration.docker_image`,description="Spilo image to be used for Pods"
// +kubebuilder:printcolumn:name="Cluster-Label",type=string,JSONPath=`.configuration.kubernetes.cluster_name_label`,description="Label for K8s resources created by operator"
// +kubebuilder:printcolumn:name="Service-Account",type=string,JSONPath=`.configuration.kubernetes.pod_service_account_name`,description="Name of service account to be used"
// +kubebuilder:printcolumn:name="Min-Instances",type=integer,JSONPath=`.configuration.min_instances`,description="Minimum number of instances per Postgres cluster"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`,description="Age of the OperatorConfiguration resource"
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels=app.kubernetes.io/name=postgres-operator
type OperatorConfiguration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Configuration OperatorConfigurationData `json:"configuration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OperatorConfigurationList is used in the k8s API calls
type OperatorConfigurationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []OperatorConfiguration `json:"items"`
}

// PostgresUsersConfiguration defines the system users of Postgres.
type PostgresUsersConfiguration struct {
	// +kubebuilder:default=postgres
	SuperUsername string `json:"super_username,omitempty"`
	// +kubebuilder:default=standby
	ReplicationUsername    string   `json:"replication_username,omitempty"`
	AdditionalOwnerRoles   []string `json:"additional_owner_roles,omitempty"`
	EnablePasswordRotation bool     `json:"enable_password_rotation,omitempty"`
	// +kubebuilder:default=90
	PasswordRotationInterval uint32 `json:"password_rotation_interval,omitempty"`
	// +kubebuilder:default=120
	PasswordRotationUserRetention uint32 `json:"password_rotation_user_retention,omitempty"`
}

// MajorVersionUpgradeConfiguration defines how to execute major version upgrades of Postgres.
type MajorVersionUpgradeConfiguration struct {
	// +kubebuilder:validation:Enum=off;manual;full
	// +kubebuilder:default=manual
	MajorVersionUpgradeMode          string   `json:"major_version_upgrade_mode,omitempty"` // off - no actions, manual - manifest triggers action, full - manifest and minimal version violation trigger upgrade
	MajorVersionUpgradeTeamAllowList []string `json:"major_version_upgrade_team_allow_list,omitempty"`
	// +kubebuilder:default="14"
	MinimalMajorVersion string `json:"minimal_major_version,omitempty"`
	// +kubebuilder:default="18"
	TargetMajorVersion string `json:"target_major_version,omitempty"`
}

// KubernetesMetaConfiguration defines k8s conf required for all Postgres clusters and the operator itself
type KubernetesMetaConfiguration struct {
	EnableOwnerReferences *bool `json:"enable_owner_references,omitempty"`
	// +kubebuilder:default=postgres-pod
	PodServiceAccountName string `json:"pod_service_account_name,omitempty"`
	// TODO: change it to the proper json
	PodServiceAccountDefinition            string `json:"pod_service_account_definition,omitempty"`
	PodServiceAccountRoleBindingDefinition string `json:"pod_service_account_role_binding_definition,omitempty"`
	// +kubebuilder:default="5m"
	// Postgres pods are terminated forcefully after this timeout
	PodTerminateGracePeriod *metav1.Duration `json:"pod_terminate_grace_period,omitempty"`
	// +optional
	LivenessProbe   *v1.Probe `json:"liveness_probe"`
	SpiloPrivileged bool      `json:"spilo_privileged,omitempty"`
	// +kubebuilder:default=true
	SpiloAllowPrivilegeEscalation *bool    `json:"spilo_allow_privilege_escalation,omitempty"`
	SpiloRunAsUser                *int64   `json:"spilo_runasuser,omitempty"`
	SpiloRunAsGroup               *int64   `json:"spilo_runasgroup,omitempty"`
	SpiloFSGroup                  *int64   `json:"spilo_fsgroup,omitempty"`
	SpiloRunAsNonRoot             *bool    `json:"spilo_run_as_non_root,omitempty"`
	AdditionalPodCapabilities     []string `json:"additional_pod_capabilities,omitempty"`
	WatchedNamespace              string   `json:"watched_namespace,omitempty"`
	// +kubebuilder:default="postgres-{cluster}-pdb"
	// defines the template for PDB names
	PDBNameFormat config.StringTemplate `json:"pdb_name_format,omitempty"`
	// +kubebuilder:default=true
	PDBMasterLabelSelector *bool `json:"pdb_master_label_selector,omitempty"`
	// +kubebuilder:default=true
	EnablePodDisruptionBudget *bool `json:"enable_pod_disruption_budget,omitempty"`
	// +kubebuilder:validation:Enum=ebs;mixed;pvc;off
	// +kubebuilder:default=pvc
	StorageResizeMode string `json:"storage_resize_mode,omitempty"`
	// +kubebuilder:default=true
	EnableInitContainers *bool `json:"enable_init_containers,omitempty"`
	// +kubebuilder:default=true
	EnableSidecars            *bool `json:"enable_sidecars,omitempty"`
	SharePgSocketWithSidecars *bool `json:"share_pgsocket_with_sidecars,omitempty"`
	// +kubebuilder:default="{username}.{cluster}.credentials.{tprkind}.{tprgroup}"
	// template for database user secrets generated by the operator,
	// here username contains the namespace in the format namespace.username
	// if the user is in different namespace than cluster and cross namespace secrets
	// are enabled via `enable_cross_namespace_secret` flag in the configuration.
	SecretNameTemplate config.StringTemplate `json:"secret_name_template,omitempty"`
	// +kubebuilder:default="cluster.local"
	ClusterDomain string `json:"cluster_domain,omitempty"`
	// +kubebuilder:default=postgres-operator
	// namespaced name of the secret containing the OAuth2 token to pass to the teams API
	OAuthTokenSecretName          spec.NamespacedName `json:"oauth_token_secret_name,omitempty"`
	InfrastructureRolesSecretName spec.NamespacedName `json:"infrastructure_roles_secret_name,omitempty"`
	// +kubebuilder:validation:Type=array
	// namespaced name of the secret containing infrastructure roles names and passwords
	InfrastructureRolesDefs []*config.InfrastructureRole `json:"infrastructure_roles_secrets,omitempty"`
	// +kubebuilder:default=spilo-role
	PodRoleLabel string `json:"pod_role_label,omitempty"`
	// +kubebuilder:default={application: spilo}
	ClusterLabels         map[string]string `json:"cluster_labels,omitempty"`
	InheritedLabels       []string          `json:"inherited_labels,omitempty"`
	InheritedAnnotations  []string          `json:"inherited_annotations,omitempty"`
	DownscalerAnnotations []string          `json:"downscaler_annotations,omitempty"`
	IgnoredAnnotations    []string          `json:"ignored_annotations,omitempty"`
	// +kubebuilder:default=cluster-name
	ClusterNameLabel        string            `json:"cluster_name_label,omitempty"`
	DeleteAnnotationDateKey string            `json:"delete_annotation_date_key,omitempty"`
	DeleteAnnotationNameKey string            `json:"delete_annotation_name_key,omitempty"`
	NodeReadinessLabel      map[string]string `json:"node_readiness_label,omitempty"`
	// +kubebuilder:validation:Enum=AND;OR
	NodeReadinessLabelMerge string            `json:"node_readiness_label_merge,omitempty"`
	CustomPodAnnotations    map[string]string `json:"custom_pod_annotations,omitempty"`
	// TODO: use a proper toleration structure?
	PodToleration map[string]string `json:"toleration,omitempty"`
	// namespaced name of the ConfigMap with environment variables to populate on every pod
	PodEnvironmentConfigMap spec.NamespacedName `json:"pod_environment_configmap,omitempty"`
	PodEnvironmentSecret    string              `json:"pod_environment_secret,omitempty"`
	PodPriorityClassName    string              `json:"pod_priority_class_name,omitempty"`
	// +kubebuilder:default="20m"
	// timeout for successful migration of master pods from unschedulable node
	MasterPodMoveTimeout                     *metav1.Duration `json:"master_pod_move_timeout,omitempty"`
	EnablePodAntiAffinity                    bool             `json:"enable_pod_antiaffinity,omitempty"`
	PodAntiAffinityPreferredDuringScheduling bool             `json:"pod_antiaffinity_preferred_during_scheduling,omitempty"`
	// +kubebuilder:default="kubernetes.io/hostname"
	PodAntiAffinityTopologyKey string `json:"pod_antiaffinity_topology_key,omitempty"`
	// +kubebuilder:validation:Enum=ordered_ready;parallel
	// +kubebuilder:default=ordered_ready
	PodManagementPolicy                  string            `json:"pod_management_policy,omitempty"`
	PersistentVolumeClaimRetentionPolicy map[string]string `json:"persistent_volume_claim_retention_policy,omitempty"`

	// +kubebuilder:default=true
	EnableSecretsDeletion *bool `json:"enable_secrets_deletion,omitempty"`
	// +kubebuilder:default=true
	EnablePersistentVolumeClaimDeletion *bool `json:"enable_persistent_volume_claim_deletion,omitempty"`
	EnableReadinessProbe                bool  `json:"enable_readiness_probe,omitempty"`
	EnableCrossNamespaceSecret          bool  `json:"enable_cross_namespace_secret,omitempty"`
	EnableFinalizers                    *bool `json:"enable_finalizers,omitempty"`
}

// PostgresPodResourcesDefaults defines the spec of default resources
type PostgresPodResourcesDefaults struct {
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	DefaultCPURequest string `json:"default_cpu_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	DefaultMemoryRequest string `json:"default_memory_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	DefaultCPULimit string `json:"default_cpu_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	DefaultMemoryLimit string `json:"default_memory_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	MinCPULimit string `json:"min_cpu_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	MinMemoryLimit string `json:"min_memory_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	MaxCPURequest string `json:"max_cpu_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	MaxMemoryRequest string `json:"max_memory_request,omitempty"`
}

// OperatorTimeouts defines the timeout of ResourceCheck, PodWait, ReadyWait
type OperatorTimeouts struct {
	// +kubebuilder:default="3s"
	// interval to wait between consecutive attempts to check for some K8s resources
	ResourceCheckInterval *metav1.Duration `json:"resource_check_interval,omitempty"`
	// +kubebuilder:default="10m"
	// timeout when waiting for the presence of a certain K8s resource
	ResourceCheckTimeout *metav1.Duration `json:"resource_check_timeout,omitempty"`
	// +kubebuilder:default="10m"
	// timeout when waiting for pod role and cluster labels
	PodLabelWaitTimeout *metav1.Duration `json:"pod_label_wait_timeout,omitempty"`
	// +kubebuilder:default="10m"
	// timeout when waiting for the Postgres pods to be deleted
	PodDeletionWaitTimeout *metav1.Duration `json:"pod_deletion_wait_timeout,omitempty"`
	// +kubebuilder:default="4s"
	// interval between consecutive attempts waiting for postgresql CRD to be created
	ReadyWaitInterval *metav1.Duration `json:"ready_wait_interval,omitempty"`
	// +kubebuilder:default="30s"
	// timeout for the complete postgres CRD creation
	ReadyWaitTimeout *metav1.Duration `json:"ready_wait_timeout,omitempty"`
	// +kubebuilder:default="1s"
	// interval between consecutive attempts of operator calling the Patroni API
	PatroniAPICheckInterval *metav1.Duration `json:"patroni_api_check_interval,omitempty"`
	// +kubebuilder:default="5s"
	// timeout when waiting for successful response from Patroni API
	PatroniAPICheckTimeout *metav1.Duration `json:"patroni_api_check_timeout,omitempty"`
}

// LoadBalancerConfiguration defines the LB configuration
type LoadBalancerConfiguration struct {
	DbHostedZone                    string `json:"db_hosted_zone,omitempty"`
	EnableMasterLoadBalancer        bool   `json:"enable_master_load_balancer,omitempty"`
	EnableMasterPoolerLoadBalancer  bool   `json:"enable_master_pooler_load_balancer,omitempty"`
	EnableReplicaLoadBalancer       bool   `json:"enable_replica_load_balancer,omitempty"`
	EnableReplicaPoolerLoadBalancer bool   `json:"enable_replica_pooler_load_balancer,omitempty"`

	// NodePort flags kept in LoadBalancerConfiguration because all the other parameters apply here too

	EnableMasterNodePort        bool `json:"enable_master_node_port,omitempty"`
	EnableMasterPoolerNodePort  bool `json:"enable_master_pooler_node_port,omitempty"`
	EnableReplicaNodePort       bool `json:"enable_replica_node_port,omitempty"`
	EnableReplicaPoolerNodePort bool `json:"enable_replica_pooler_node_port,omitempty"`

	CustomServiceAnnotations map[string]string `json:"custom_service_annotations,omitempty"`
	// +kubebuilder:default="{cluster}.{namespace}.{hostedzone}"
	// defines the DNS name string template for the master load balancer cluster
	MasterDNSNameFormat config.StringTemplate `json:"master_dns_name_format,omitempty"`
	// +kubebuilder:default="{cluster}.{team}.{hostedzone}"
	// deprecated DNS template for master load balancer using team name
	MasterLegacyDNSNameFormat config.StringTemplate `json:"master_legacy_dns_name_format,omitempty"`
	// +kubebuilder:default="{cluster}-repl.{namespace}.{hostedzone}"
	// defines the DNS name string template for the replica load balancer cluster
	ReplicaDNSNameFormat config.StringTemplate `json:"replica_dns_name_format,omitempty"`
	// +kubebuilder:default="{cluster}-repl.{team}.{hostedzone}"
	// deprecated DNS template for replica load balancer using team name
	ReplicaLegacyDNSNameFormat config.StringTemplate `json:"replica_legacy_dns_name_format,omitempty"`
	// +kubebuilder:validation:Enum=Cluster;Local
	// +kubebuilder:default=Cluster
	ExternalTrafficPolicy string `json:"external_traffic_policy,omitempty"`
}

// AWSGCPConfiguration defines the configuration for AWS
// TODO complete Google Cloud Platform (GCP) configuration
type AWSGCPConfiguration struct {
	WALES3Bucket string `json:"wal_s3_bucket,omitempty"`
	// +kubebuilder:default=eu-central-1
	AWSRegion                 string `json:"aws_region,omitempty"`
	WALGSBucket               string `json:"wal_gs_bucket,omitempty"`
	GCPCredentials            string `json:"gcp_credentials,omitempty"`
	WALAZStorageAccount       string `json:"wal_az_storage_account,omitempty"`
	LogS3Bucket               string `json:"log_s3_bucket,omitempty"`
	KubeIAMRole               string `json:"kube_iam_role,omitempty"`
	IRSARoleARN               string `json:"irsa_role_arn,omitempty"`
	AdditionalSecretMount     string `json:"additional_secret_mount,omitempty"`
	AdditionalSecretMountPath string `json:"additional_secret_mount_path,omitempty"`
}

// OperatorDebugConfiguration defines options for the debug mode
type OperatorDebugConfiguration struct {
	// +kubebuilder:default=true
	DebugLogging *bool `json:"debug_logging,omitempty"`
	// +kubebuilder:default=true
	EnableDBAccess *bool `json:"enable_database_access,omitempty"`
}

// TeamsAPIConfiguration defines the configuration of TeamsAPI
type TeamsAPIConfiguration struct {
	EnableTeamsAPI bool `json:"enable_teams_api,omitempty"`
	// +kubebuilder:default="https://teams.example.com/api/"
	TeamsAPIUrl string `json:"teams_api_url,omitempty"`
	// +kubebuilder:default={log_statement: all}
	TeamAPIRoleConfiguration map[string]string `json:"team_api_role_configuration,omitempty"`
	EnableTeamSuperuser      bool              `json:"enable_team_superuser,omitempty"`
	// +kubebuilder:default=true
	EnableAdminRoleForUsers bool `json:"enable_admin_role_for_users,omitempty"`
	// +kubebuilder:default=admin
	TeamAdminRole string `json:"team_admin_role,omitempty"`
	// +kubebuilder:default=zalandos
	PamRoleName string `json:"pam_role_name,omitempty"`
	// +kubebuilder:default="https://info.example.com/oauth2/tokeninfo?access_token= uid realm=/employees"
	PamConfiguration string `json:"pam_configuration,omitempty"`
	// +kubebuilder:default={"admin", "cron_admin"}
	ProtectedRoles         []string `json:"protected_role_names,omitempty"`
	PostgresSuperuserTeams []string `json:"postgres_superuser_teams,omitempty"`
	// +kubebuilder:default=true
	EnablePostgresTeamCRD           bool `json:"enable_postgres_team_crd,omitempty"`
	EnablePostgresTeamCRDSuperusers bool `json:"enable_postgres_team_crd_superusers,omitempty"`
	EnableTeamMemberDeprecation     bool `json:"enable_team_member_deprecation,omitempty"`
	// +kubebuilder:default=_deleted
	RoleDeletionSuffix string `json:"role_deletion_suffix,omitempty"`
}

// LoggingRESTAPIConfiguration defines Logging API conf
type LoggingRESTAPIConfiguration struct {
	// +kubebuilder:default=8080
	APIPort int `json:"api_port,omitempty"`
	// +kubebuilder:default=100
	RingLogLines int `json:"ring_log_lines,omitempty"`
	// +kubebuilder:default=1000
	ClusterHistoryEntries int `json:"cluster_history_entries,omitempty"`
}

// ScalyrConfiguration defines the configuration for ScalyrAPI
type ScalyrConfiguration struct {
	ScalyrAPIKey string `json:"scalyr_api_key,omitempty"`
	ScalyrImage  string `json:"scalyr_image,omitempty"`
	// +kubebuilder:default="https://upload.eu.scalyr.com"
	ScalyrServerURL string `json:"scalyr_server_url,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	// +kubebuilder:default="100m"
	ScalyrCPURequest string `json:"scalyr_cpu_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	// +kubebuilder:default="50Mi"
	ScalyrMemoryRequest string `json:"scalyr_memory_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	// +kubebuilder:default="1"
	ScalyrCPULimit string `json:"scalyr_cpu_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	// +kubebuilder:default="500Mi"
	ScalyrMemoryLimit string `json:"scalyr_memory_limit,omitempty"`
}

// ConnectionPoolerConfiguration defines default configuration for connection pooler
type ConnectionPoolerConfiguration struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	NumberOfInstances *int32 `json:"connection_pooler_number_of_instances,omitempty"`
	// +kubebuilder:default=pooler
	Schema string `json:"connection_pooler_schema,omitempty"`
	// +kubebuilder:default=pooler
	User string `json:"connection_pooler_user,omitempty"`
	// +kubebuilder:default="ghcr.io/zalando/postgres-operator/pgbouncer:v2.0.3"
	Image string `json:"connection_pooler_image,omitempty"`
	// +kubebuilder:validation:Enum=session;transaction
	// +kubebuilder:default=transaction
	Mode             string `json:"connection_pooler_mode,omitempty"`
	MaxDBConnections *int32 `json:"connection_pooler_max_db_connections,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	DefaultCPURequest string `json:"connection_pooler_default_cpu_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	DefaultMemoryRequest string `json:"connection_pooler_default_memory_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	DefaultCPULimit string `json:"connection_pooler_default_cpu_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	DefaultMemoryLimit string `json:"connection_pooler_default_memory_limit,omitempty"`
}

// OperatorLogicalBackupConfiguration defines configuration for logical backup
type OperatorLogicalBackupConfiguration struct {
	// +kubebuilder:validation:Pattern=`^(\d+|\*)(/\d+)?(\s+(\d+|\*)(/\d+)?){4}$`
	// +kubebuilder:default="30 00 * * *"
	Schedule string `json:"logical_backup_schedule,omitempty"`
	// +kubebuilder:default="ghcr.io/zalando/postgres-operator/logical-backup:v2.0.3"
	DockerImage string `json:"logical_backup_docker_image,omitempty"`
	// +kubebuilder:validation:Enum=az;gcs;s3
	// +kubebuilder:default=s3
	BackupProvider               string `json:"logical_backup_provider,omitempty"`
	AzureStorageAccountName      string `json:"logical_backup_azure_storage_account_name,omitempty"`
	AzureStorageContainer        string `json:"logical_backup_azure_storage_container,omitempty"`
	AzureStorageAccountKey       string `json:"logical_backup_azure_storage_account_key,omitempty"`
	S3Bucket                     string `json:"logical_backup_s3_bucket,omitempty"`
	S3BucketPrefix               string `json:"logical_backup_s3_bucket_prefix,omitempty"`
	S3Region                     string `json:"logical_backup_s3_region,omitempty"`
	S3Endpoint                   string `json:"logical_backup_s3_endpoint,omitempty"`
	S3AccessKeyID                string `json:"logical_backup_s3_access_key_id,omitempty"`
	S3SecretAccessKey            string `json:"logical_backup_s3_secret_access_key,omitempty"`
	S3SSE                        string `json:"logical_backup_s3_sse,omitempty"`
	RetentionTime                string `json:"logical_backup_s3_retention_time,omitempty"`
	GoogleApplicationCredentials string `json:"logical_backup_google_application_credentials,omitempty"`
	// +kubebuilder:default=logical-backup-
	JobPrefix                string `json:"logical_backup_job_prefix,omitempty"`
	CronjobEnvironmentSecret string `json:"logical_backup_cronjob_environment_secret,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	CPURequest string `json:"logical_backup_cpu_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	MemoryRequest string `json:"logical_backup_memory_request,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	CPULimit string `json:"logical_backup_cpu_limit,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	MemoryLimit string `json:"logical_backup_memory_limit,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	SuccessfulJobsHistoryLimit *int32 `json:"logical_backup_successful_jobs_history_limit,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	FailedJobsHistoryLimit *int32 `json:"logical_backup_failed_jobs_history_limit,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=86400
	TTLSecondsAfterFinished *int32 `json:"logical_backup_ttl_seconds_after_finished,omitempty"`
}

// PatroniConfiguration defines configuration for Patroni
type PatroniConfiguration struct {
	FailsafeMode *bool `json:"enable_patroni_failsafe_mode,omitempty"`
}

// OperatorConfigurationData defines the operation config
type OperatorConfigurationData struct {
	// +kubebuilder:default=true
	EnableCRDRegistration  *bool    `json:"enable_crd_registration,omitempty"`
	CRDCategories          []string `json:"crd_categories,omitempty"`
	EnableLazySpiloUpgrade bool     `json:"enable_lazy_spilo_upgrade,omitempty"`
	// +kubebuilder:default=true
	EnablePgVersionEnvVar         bool `json:"enable_pgversion_env_var,omitempty"`
	EnableSpiloWalPathCompat      bool `json:"enable_spilo_wal_path_compat,omitempty"`
	EnableTeamIdClusternamePrefix bool `json:"enable_team_id_clustername_prefix,omitempty"`
	// +kubebuilder:default=""
	EtcdHost string `json:"etcd_host,omitempty"`
	// +kubebuilder:default=true
	KubernetesUseConfigMaps *bool `json:"kubernetes_use_configmaps,omitempty"`
	// +kubebuilder:default="ghcr.io/zalando/spilo-18:4.1-p2"
	DockerImage string `json:"docker_image,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=8
	Workers uint32 `json:"workers,omitempty"`
	// +kubebuilder:default="30m"
	// period between consecutive sync requests
	ResyncPeriod *metav1.Duration `json:"resync_period,omitempty"`
	// +kubebuilder:default="5m"
	// period between consecutive repair requests
	RepairPeriod *metav1.Duration `json:"repair_period,omitempty"`
	// +kubebuilder:default=true
	EnableMaintenanceWindows *bool `json:"enable_maintenance_windows,omitempty"`
	// +kubebuilder:validation:Type=array
	// +kubebuilder:validation:items:Pattern=`^\ *((Mon|Tue|Wed|Thu|Fri|Sat|Sun):(2[0-3]|[01]?\d):([0-5]?\d)|(2[0-3]|[01]?\d):([0-5]?\d))-((2[0-3]|[01]?\d):([0-5]?\d)|(2[0-3]|[01]?\d):([0-5]?\d))\ *$`
	MaintenanceWindows      []string `json:"maintenance_windows,omitempty"`
	SetMemoryRequestToLimit bool     `json:"set_memory_request_to_limit,omitempty"`
	// +kubebuilder:default=true
	ShmVolume     *bool             `json:"enable_shm_volume,omitempty"`
	SidecarImages map[string]string `json:"sidecar_docker_images,omitempty"` // deprecated in favour of SidecarContainers
	// +nullable
	SidecarContainers []v1.Container `json:"sidecars,omitempty"`
	// +optional
	PostgresUsersConfiguration PostgresUsersConfiguration `json:"users"`
	// +optional
	MajorVersionUpgrade MajorVersionUpgradeConfiguration `json:"major_version_upgrade"`
	// +optional
	Kubernetes KubernetesMetaConfiguration `json:"kubernetes"`
	// +optional
	PostgresPodResources PostgresPodResourcesDefaults `json:"postgres_pod_resources"`
	// +optional
	Timeouts OperatorTimeouts `json:"timeouts"`
	// +optional
	LoadBalancer LoadBalancerConfiguration `json:"load_balancer"`
	// +optional
	AWSGCP AWSGCPConfiguration `json:"aws_or_gcp"`
	// +optional
	OperatorDebug OperatorDebugConfiguration `json:"debug"`
	// +optional
	TeamsAPI TeamsAPIConfiguration `json:"teams_api"`
	// +optional
	LoggingRESTAPI LoggingRESTAPIConfiguration `json:"logging_rest_api"`
	// +optional
	Scalyr ScalyrConfiguration `json:"scalyr"`
	// +optional
	LogicalBackup OperatorLogicalBackupConfiguration `json:"logical_backup"`
	// +optional
	ConnectionPooler ConnectionPoolerConfiguration `json:"connection_pooler"`
	// +optional
	Patroni PatroniConfiguration `json:"patroni"`

	// +kubebuilder:validation:Minimum=-1
	// +kubebuilder:default=-1
	// -1 = disabled
	MinInstances int32 `json:"min_instances,omitempty"`
	// +kubebuilder:validation:Minimum=-1
	// +kubebuilder:default=-1
	// -1 = disabled
	MaxInstances int32 `json:"max_instances,omitempty"`

	IgnoreInstanceLimitsAnnotationKey  string `json:"ignore_instance_limits_annotation_key,omitempty"`
	IgnoreResourcesLimitsAnnotationKey string `json:"ignore_resources_limits_annotation_key,omitempty"`
}
