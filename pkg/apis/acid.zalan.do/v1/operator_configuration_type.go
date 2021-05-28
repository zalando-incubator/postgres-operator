package v1

// Operator configuration CRD definition, please use snake_case for field names.

import (
	"github.com/zalando/postgres-operator/pkg/util/config"

	"time"

	"github.com/zalando/postgres-operator/pkg/spec"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:onlyVerbs=get
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// OperatorConfiguration defines the specification for the OperatorConfiguration.
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
	SuperUsername       string `json:"super_username,omitempty"`
	ReplicationUsername string `json:"replication_username,omitempty"`
}

// MajorVersionUpgradeConfiguration defines how to execute major version upgrades of Postgres.
type MajorVersionUpgradeConfiguration struct {
	MajorVersionUpgradeMode string `json:"major_version_upgrade_mode" default:"off"` // off - no actions, manual - manifest triggers action, full - manifest and minimal version violation trigger upgrade
	MinimalMajorVersion     string `json:"minimal_major_version" default:"9.5"`
	TargetMajorVersion      string `json:"target_major_version" default:"13"`
}

// KubernetesMetaConfiguration defines k8s conf required for all Postgres clusters and the operator itself
type KubernetesMetaConfiguration struct {
	PodServiceAccountName string `json:"pod_service_account_name,omitempty"`
	// TODO: change it to the proper json
	PodServiceAccountDefinition            string                       `json:"pod_service_account_definition,omitempty"`
	PodServiceAccountRoleBindingDefinition string                       `json:"pod_service_account_role_binding_definition,omitempty"`
	PodTerminateGracePeriod                Duration                     `json:"pod_terminate_grace_period,omitempty"`
	SpiloPrivileged                        bool                         `json:"spilo_privileged,omitempty"`
	SpiloAllowPrivilegeEscalation          *bool                        `json:"spilo_allow_privilege_escalation,omitempty"`
	SpiloRunAsUser                         *int64                       `json:"spilo_runasuser,omitempty"`
	SpiloRunAsGroup                        *int64                       `json:"spilo_runasgroup,omitempty"`
	SpiloFSGroup                           *int64                       `json:"spilo_fsgroup,omitempty"`
	AdditionalPodCapabilities              []string                     `json:"additional_pod_capabilities,omitempty"`
	WatchedNamespace                       string                       `json:"watched_namespace,omitempty"`
	PDBNameFormat                          config.StringTemplate        `json:"pdb_name_format,omitempty"`
	EnablePodDisruptionBudget              *bool                        `json:"enable_pod_disruption_budget,omitempty"`
	StorageResizeMode                      string                       `json:"storage_resize_mode,omitempty"`
	EnableInitContainers                   *bool                        `json:"enable_init_containers,omitempty"`
	EnableSidecars                         *bool                        `json:"enable_sidecars,omitempty"`
	SecretNameTemplate                     config.StringTemplate        `json:"secret_name_template,omitempty"`
	ClusterDomain                          string                       `json:"cluster_domain,omitempty"`
	OAuthTokenSecretName                   spec.NamespacedName          `json:"oauth_token_secret_name,omitempty"`
	InfrastructureRolesSecretName          spec.NamespacedName          `json:"infrastructure_roles_secret_name,omitempty"`
	InfrastructureRolesDefs                []*config.InfrastructureRole `json:"infrastructure_roles_secrets,omitempty"`
	PodRoleLabel                           string                       `json:"pod_role_label,omitempty"`
	ClusterLabels                          map[string]string            `json:"cluster_labels,omitempty"`
	InheritedLabels                        []string                     `json:"inherited_labels,omitempty"`
	InheritedAnnotations                   []string                     `json:"inherited_annotations,omitempty"`
	DownscalerAnnotations                  []string                     `json:"downscaler_annotations,omitempty"`
	ClusterNameLabel                       string                       `json:"cluster_name_label,omitempty"`
	DeleteAnnotationDateKey                string                       `json:"delete_annotation_date_key,omitempty"`
	DeleteAnnotationNameKey                string                       `json:"delete_annotation_name_key,omitempty"`
	NodeReadinessLabel                     map[string]string            `json:"node_readiness_label,omitempty"`
	CustomPodAnnotations                   map[string]string            `json:"custom_pod_annotations,omitempty"`
	// TODO: use a proper toleration structure?
	PodToleration              map[string]string   `json:"toleration,omitempty"`
	PodEnvironmentConfigMap    spec.NamespacedName `json:"pod_environment_configmap,omitempty"`
	PodEnvironmentSecret       string              `json:"pod_environment_secret,omitempty"`
	PodPriorityClassName       string              `json:"pod_priority_class_name,omitempty"`
	MasterPodMoveTimeout       Duration            `json:"master_pod_move_timeout,omitempty"`
	EnablePodAntiAffinity      bool                `json:"enable_pod_antiaffinity,omitempty"`
	PodAntiAffinityTopologyKey string              `json:"pod_antiaffinity_topology_key,omitempty"`
	PodManagementPolicy        string              `json:"pod_management_policy,omitempty"`
}

// PostgresPodResourcesDefaults defines the spec of default resources
type PostgresPodResourcesDefaults struct {
	DefaultCPURequest    string `json:"default_cpu_request,omitempty"`
	DefaultMemoryRequest string `json:"default_memory_request,omitempty"`
	DefaultCPULimit      string `json:"default_cpu_limit,omitempty"`
	DefaultMemoryLimit   string `json:"default_memory_limit,omitempty"`
	MinCPULimit          string `json:"min_cpu_limit,omitempty"`
	MinMemoryLimit       string `json:"min_memory_limit,omitempty"`
}

// OperatorTimeouts defines the timeout of ResourceCheck, PodWait, ReadyWait
type OperatorTimeouts struct {
	ResourceCheckInterval  Duration `json:"resource_check_interval,omitempty"`
	ResourceCheckTimeout   Duration `json:"resource_check_timeout,omitempty"`
	PodLabelWaitTimeout    Duration `json:"pod_label_wait_timeout,omitempty"`
	PodDeletionWaitTimeout Duration `json:"pod_deletion_wait_timeout,omitempty"`
	ReadyWaitInterval      Duration `json:"ready_wait_interval,omitempty"`
	ReadyWaitTimeout       Duration `json:"ready_wait_timeout,omitempty"`
}

// LoadBalancerConfiguration defines the LB configuration
type LoadBalancerConfiguration struct {
	DbHostedZone              string                `json:"db_hosted_zone,omitempty"`
	EnableMasterLoadBalancer  bool                  `json:"enable_master_load_balancer,omitempty"`
	EnableReplicaLoadBalancer bool                  `json:"enable_replica_load_balancer,omitempty"`
	CustomServiceAnnotations  map[string]string     `json:"custom_service_annotations,omitempty"`
	IgnoredServiceAnnotations []string              `json:"ignored_service_annotations,omitempty"`
	MasterDNSNameFormat       config.StringTemplate `json:"master_dns_name_format,omitempty"`
	ReplicaDNSNameFormat      config.StringTemplate `json:"replica_dns_name_format,omitempty"`
	ExternalTrafficPolicy     string                `json:"external_traffic_policy" default:"Cluster"`
}

// AWSGCPConfiguration defines the configuration for AWS
// TODO complete Google Cloud Platform (GCP) configuration
type AWSGCPConfiguration struct {
	WALES3Bucket                 string `json:"wal_s3_bucket,omitempty"`
	AWSRegion                    string `json:"aws_region,omitempty"`
	WALGSBucket                  string `json:"wal_gs_bucket,omitempty"`
	GCPCredentials               string `json:"gcp_credentials,omitempty"`
	LogS3Bucket                  string `json:"log_s3_bucket,omitempty"`
	KubeIAMRole                  string `json:"kube_iam_role,omitempty"`
	AdditionalSecretMount        string `json:"additional_secret_mount,omitempty"`
	AdditionalSecretMountPath    string `json:"additional_secret_mount_path" default:"/meta/credentials"`
	EnableEBSGp3Migration        bool   `json:"enable_ebs_gp3_migration" default:"false"`
	EnableEBSGp3MigrationMaxSize int64  `json:"enable_ebs_gp3_migration_max_size" default:"1000"`
}

// OperatorDebugConfiguration defines options for the debug mode
type OperatorDebugConfiguration struct {
	DebugLogging   bool `json:"debug_logging,omitempty"`
	EnableDBAccess bool `json:"enable_database_access,omitempty"`
}

// TeamsAPIConfiguration defines the configuration of TeamsAPI
type TeamsAPIConfiguration struct {
	EnableTeamsAPI                  bool              `json:"enable_teams_api,omitempty"`
	TeamsAPIUrl                     string            `json:"teams_api_url,omitempty"`
	TeamAPIRoleConfiguration        map[string]string `json:"team_api_role_configuration,omitempty"`
	EnableTeamSuperuser             bool              `json:"enable_team_superuser,omitempty"`
	EnableAdminRoleForUsers         bool              `json:"enable_admin_role_for_users,omitempty"`
	TeamAdminRole                   string            `json:"team_admin_role,omitempty"`
	PamRoleName                     string            `json:"pam_role_name,omitempty"`
	PamConfiguration                string            `json:"pam_configuration,omitempty"`
	ProtectedRoles                  []string          `json:"protected_role_names,omitempty"`
	PostgresSuperuserTeams          []string          `json:"postgres_superuser_teams,omitempty"`
	EnablePostgresTeamCRD           bool              `json:"enable_postgres_team_crd,omitempty"`
	EnablePostgresTeamCRDSuperusers bool              `json:"enable_postgres_team_crd_superusers,omitempty"`
	EnableTeamMemberDeprecation     bool              `json:"enable_team_member_deprecation,omitempty"`
	RoleDeletionSuffix              string            `json:"role_deletion_suffix,omitempty"`
}

// LoggingRESTAPIConfiguration defines Logging API conf
type LoggingRESTAPIConfiguration struct {
	APIPort               int `json:"api_port,omitempty"`
	RingLogLines          int `json:"ring_log_lines,omitempty"`
	ClusterHistoryEntries int `json:"cluster_history_entries,omitempty"`
}

// ScalyrConfiguration defines the configuration for ScalyrAPI
type ScalyrConfiguration struct {
	ScalyrAPIKey        string `json:"scalyr_api_key,omitempty"`
	ScalyrImage         string `json:"scalyr_image,omitempty"`
	ScalyrServerURL     string `json:"scalyr_server_url,omitempty"`
	ScalyrCPURequest    string `json:"scalyr_cpu_request,omitempty"`
	ScalyrMemoryRequest string `json:"scalyr_memory_request,omitempty"`
	ScalyrCPULimit      string `json:"scalyr_cpu_limit,omitempty"`
	ScalyrMemoryLimit   string `json:"scalyr_memory_limit,omitempty"`
}

// ConnectionPoolerConfiguration defines default configuration for connection pooler
type ConnectionPoolerConfiguration struct {
	NumberOfInstances    *int32 `json:"connection_pooler_number_of_instances,omitempty"`
	Schema               string `json:"connection_pooler_schema,omitempty"`
	User                 string `json:"connection_pooler_user,omitempty"`
	Image                string `json:"connection_pooler_image,omitempty"`
	Mode                 string `json:"connection_pooler_mode,omitempty"`
	MaxDBConnections     *int32 `json:"connection_pooler_max_db_connections,omitempty"`
	DefaultCPURequest    string `json:"connection_pooler_default_cpu_request,omitempty"`
	DefaultMemoryRequest string `json:"connection_pooler_default_memory_request,omitempty"`
	DefaultCPULimit      string `json:"connection_pooler_default_cpu_limit,omitempty"`
	DefaultMemoryLimit   string `json:"connection_pooler_default_memory_limit,omitempty"`
}

// OperatorLogicalBackupConfiguration defines configuration for logical backup
type OperatorLogicalBackupConfiguration struct {
	Schedule                     string `json:"logical_backup_schedule,omitempty"`
	DockerImage                  string `json:"logical_backup_docker_image,omitempty"`
	BackupProvider               string `json:"logical_backup_provider,omitempty"`
	S3Bucket                     string `json:"logical_backup_s3_bucket,omitempty"`
	S3Region                     string `json:"logical_backup_s3_region,omitempty"`
	S3Endpoint                   string `json:"logical_backup_s3_endpoint,omitempty"`
	S3AccessKeyID                string `json:"logical_backup_s3_access_key_id,omitempty"`
	S3SecretAccessKey            string `json:"logical_backup_s3_secret_access_key,omitempty"`
	S3SSE                        string `json:"logical_backup_s3_sse,omitempty"`
	GoogleApplicationCredentials string `json:"logical_backup_google_application_credentials,omitempty"`
	JobPrefix                    string `json:"logical_backup_job_prefix,omitempty"`
}

// OperatorConfigurationData defines the operation config
type OperatorConfigurationData struct {
	EnableCRDValidation        *bool                              `json:"enable_crd_validation,omitempty"`
	EnableLazySpiloUpgrade     bool                               `json:"enable_lazy_spilo_upgrade,omitempty"`
	EnablePgVersionEnvVar      bool                               `json:"enable_pgversion_env_var,omitempty"`
	EnableSpiloWalPathCompat   bool                               `json:"enable_spilo_wal_path_compat,omitempty"`
	EtcdHost                   string                             `json:"etcd_host,omitempty"`
	KubernetesUseConfigMaps    bool                               `json:"kubernetes_use_configmaps,omitempty"`
	DockerImage                string                             `json:"docker_image,omitempty"`
	Workers                    uint32                             `json:"workers,omitempty"`
	MinInstances               int32                              `json:"min_instances,omitempty"`
	MaxInstances               int32                              `json:"max_instances,omitempty"`
	ResyncPeriod               Duration                           `json:"resync_period,omitempty"`
	RepairPeriod               Duration                           `json:"repair_period,omitempty"`
	SetMemoryRequestToLimit    bool                               `json:"set_memory_request_to_limit,omitempty"`
	ShmVolume                  *bool                              `json:"enable_shm_volume,omitempty"`
	SidecarImages              map[string]string                  `json:"sidecar_docker_images,omitempty"` // deprecated in favour of SidecarContainers
	SidecarContainers          []v1.Container                     `json:"sidecars,omitempty"`
	PostgresUsersConfiguration PostgresUsersConfiguration         `json:"users"`
	MajorVersionUpgrade        MajorVersionUpgradeConfiguration   `json:"major_version_upgrade"`
	Kubernetes                 KubernetesMetaConfiguration        `json:"kubernetes"`
	PostgresPodResources       PostgresPodResourcesDefaults       `json:"postgres_pod_resources"`
	Timeouts                   OperatorTimeouts                   `json:"timeouts"`
	LoadBalancer               LoadBalancerConfiguration          `json:"load_balancer"`
	AWSGCP                     AWSGCPConfiguration                `json:"aws_or_gcp"`
	OperatorDebug              OperatorDebugConfiguration         `json:"debug"`
	TeamsAPI                   TeamsAPIConfiguration              `json:"teams_api"`
	LoggingRESTAPI             LoggingRESTAPIConfiguration        `json:"logging_rest_api"`
	Scalyr                     ScalyrConfiguration                `json:"scalyr"`
	LogicalBackup              OperatorLogicalBackupConfiguration `json:"logical_backup"`
	ConnectionPooler           ConnectionPoolerConfiguration      `json:"connection_pooler"`
}

//Duration shortens this frequently used name
type Duration time.Duration
