package config

import (
	"encoding/json"
	"strings"

	"fmt"

	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CRD describes CustomResourceDefinition specific configuration parameters
type CRD struct {
	ReadyWaitInterval     *metav1.Duration `name:"ready_wait_interval" default:"4s"`
	ReadyWaitTimeout      *metav1.Duration `name:"ready_wait_timeout" default:"30s"`
	ResyncPeriod          *metav1.Duration `name:"resync_period" default:"30m"`
	RepairPeriod          *metav1.Duration `name:"repair_period" default:"5m"`
	EnableCRDRegistration *bool            `name:"enable_crd_registration" default:"true"`
	CRDCategories         []string         `name:"crd_categories" default:"all"`
}

// Resources describes kubernetes resource specific configuration parameters
type Resources struct {
	EnableOwnerReferences         *bool               `name:"enable_owner_references" default:"false"`
	ResourceCheckInterval         *metav1.Duration    `name:"resource_check_interval" default:"3s"`
	ResourceCheckTimeout          *metav1.Duration    `name:"resource_check_timeout" default:"10m"`
	PodLabelWaitTimeout           *metav1.Duration    `name:"pod_label_wait_timeout" default:"10m"`
	PodDeletionWaitTimeout        *metav1.Duration    `name:"pod_deletion_wait_timeout" default:"10m"`
	PodTerminateGracePeriod       *metav1.Duration    `name:"pod_terminate_grace_period" default:"5m"`
	LivenessProbe                 *v1.Probe           `name:"-"`
	SpiloRunAsUser                *int64              `name:"spilo_runasuser"`
	SpiloRunAsGroup               *int64              `name:"spilo_runasgroup"`
	SpiloFSGroup                  *int64              `name:"spilo_fsgroup"`
	PodPriorityClassName          string              `name:"pod_priority_class_name"`
	ClusterDomain                 string              `name:"cluster_domain" default:"cluster.local"`
	SpiloPrivileged               bool                `name:"spilo_privileged" default:"false"`
	SpiloAllowPrivilegeEscalation *bool               `name:"spilo_allow_privilege_escalation" default:"true"`
	SpiloRunAsNonRoot             *bool               `name:"spilo_run_as_non_root"`
	AdditionalPodCapabilities     []string            `name:"additional_pod_capabilities" default:""`
	ClusterLabels                 map[string]string   `name:"cluster_labels" default:"application:spilo"`
	InheritedLabels               []string            `name:"inherited_labels" default:""`
	InheritedAnnotations          []string            `name:"inherited_annotations" default:""`
	DownscalerAnnotations         []string            `name:"downscaler_annotations"`
	IgnoredAnnotations            []string            `name:"ignored_annotations"`
	ClusterNameLabel              string              `name:"cluster_name_label" default:"cluster-name"`
	DeleteAnnotationDateKey       string              `name:"delete_annotation_date_key"`
	DeleteAnnotationNameKey       string              `name:"delete_annotation_name_key"`
	PodRoleLabel                  string              `name:"pod_role_label" default:"spilo-role"`
	PodToleration                 map[string]string   `name:"toleration" default:""`
	DefaultCPURequest             string              `name:"default_cpu_request"`
	DefaultMemoryRequest          string              `name:"default_memory_request"`
	DefaultCPULimit               string              `name:"default_cpu_limit"`
	DefaultMemoryLimit            string              `name:"default_memory_limit"`
	MinCPULimit                   string              `name:"min_cpu_limit"`
	MinMemoryLimit                string              `name:"min_memory_limit"`
	MaxCPURequest                 string              `name:"max_cpu_request"`
	MaxMemoryRequest              string              `name:"max_memory_request"`
	PodEnvironmentConfigMap       spec.NamespacedName `name:"pod_environment_configmap"`
	PodEnvironmentSecret          string              `name:"pod_environment_secret"`
	NodeReadinessLabel            map[string]string   `name:"node_readiness_label" default:""`
	NodeReadinessLabelMerge       string              `name:"node_readiness_label_merge" default:"OR"`
	ShmVolume                     *bool               `name:"enable_shm_volume" default:"true"`

	MaxInstances                       int32  `name:"max_instances" default:"-1"`
	MinInstances                       int32  `name:"min_instances" default:"-1"`
	IgnoreInstanceLimitsAnnotationKey  string `name:"ignore_instance_limits_annotation_key"`
	IgnoreResourcesLimitsAnnotationKey string `name:"ignore_resources_limits_annotation_key"`
}

type InfrastructureRole struct {
	// Name of a secret which describes the role, and optionally name of a
	// configmap with an extra information
	SecretName spec.NamespacedName `json:"secretname,omitempty"`

	UserKey     string `json:"userkey,omitempty"`
	PasswordKey string `json:"passwordkey,omitempty"`
	RoleKey     string `json:"rolekey,omitempty"`

	DefaultUserValue string `json:"defaultuservalue,omitempty"`
	DefaultRoleValue string `json:"defaultrolevalue,omitempty"`

	// This field point out the detailed yaml definition of the role, if exists
	Details string `json:"details,omitempty"`

	// Specify if a secret contains multiple fields in the following format:
	//
	// 	%(userkey)idx: ...
	// 	%(passwordkey)idx: ...
	// 	%(rolekey)idx: ...
	//
	// If it does, Name/Password/Role are interpreted not as unique field
	// names, but as a template.

	Template bool `json:"template,omitempty"`
}

// Auth describes authentication specific configuration parameters
type Auth struct {
	SecretNameTemplate            StringTemplate        `name:"secret_name_template" default:"{username}.{cluster}.credentials.{tprkind}.{tprgroup}"`
	PamRoleName                   string                `name:"pam_role_name" default:"zalandos"`
	PamConfiguration              string                `name:"pam_configuration" default:"https://info.example.com/oauth2/tokeninfo?access_token= uid realm=/employees"`
	TeamsAPIUrl                   string                `name:"teams_api_url" default:"https://teams.example.com/api/"`
	OAuthTokenSecretName          spec.NamespacedName   `name:"oauth_token_secret_name" default:"postgresql-operator"`
	InfrastructureRolesSecretName spec.NamespacedName   `name:"infrastructure_roles_secret_name"`
	InfrastructureRoles           []*InfrastructureRole `name:"-"`
	InfrastructureRolesDefs       string                `name:"infrastructure_roles_secrets"`
	SuperUsername                 string                `name:"super_username" default:"postgres"`
	ReplicationUsername           string                `name:"replication_username" default:"standby"`
	AdditionalOwnerRoles          []string              `name:"additional_owner_roles" default:""`
	EnablePasswordRotation        bool                  `name:"enable_password_rotation" default:"false"`
	PasswordRotationInterval      uint32                `name:"password_rotation_interval" default:"90"`
	PasswordRotationUserRetention uint32                `name:"password_rotation_user_retention" default:"180"`
}

// Scalyr holds the configuration for the Scalyr Agent sidecar for log shipping:
type Scalyr struct {
	ScalyrAPIKey        string `name:"scalyr_api_key" default:""`
	ScalyrImage         string `name:"scalyr_image" default:""`
	ScalyrServerURL     string `name:"scalyr_server_url" default:"https://upload.eu.scalyr.com"`
	ScalyrCPURequest    string `name:"scalyr_cpu_request" default:"100m"`
	ScalyrMemoryRequest string `name:"scalyr_memory_request" default:"50Mi"`
	ScalyrCPULimit      string `name:"scalyr_cpu_limit" default:"1"`
	ScalyrMemoryLimit   string `name:"scalyr_memory_limit" default:"500Mi"`
}

// LogicalBackup defines configuration for logical backup
type LogicalBackup struct {
	LogicalBackupSchedule                     string `name:"logical_backup_schedule" default:"30 00 * * *"`
	LogicalBackupDockerImage                  string `name:"logical_backup_docker_image" default:"ghcr.io/zalando/postgres-operator/logical-backup:v2.0.3"`
	LogicalBackupProvider                     string `name:"logical_backup_provider" default:"s3"`
	LogicalBackupAzureStorageAccountName      string `name:"logical_backup_azure_storage_account_name" default:""`
	LogicalBackupAzureStorageContainer        string `name:"logical_backup_azure_storage_container" default:""`
	LogicalBackupAzureStorageAccountKey       string `name:"logical_backup_azure_storage_account_key" default:""`
	LogicalBackupS3Bucket                     string `name:"logical_backup_s3_bucket" default:""`
	LogicalBackupS3BucketPrefix               string `name:"logical_backup_s3_bucket_prefix" default:"spilo"`
	LogicalBackupS3Region                     string `name:"logical_backup_s3_region" default:""`
	LogicalBackupS3Endpoint                   string `name:"logical_backup_s3_endpoint" default:""`
	LogicalBackupS3AccessKeyID                string `name:"logical_backup_s3_access_key_id" default:""`
	LogicalBackupS3SecretAccessKey            string `name:"logical_backup_s3_secret_access_key" default:""`
	LogicalBackupS3SSE                        string `name:"logical_backup_s3_sse" default:""`
	LogicalBackupS3RetentionTime              string `name:"logical_backup_s3_retention_time" default:""`
	LogicalBackupGoogleApplicationCredentials string `name:"logical_backup_google_application_credentials" default:""`
	LogicalBackupJobPrefix                    string `name:"logical_backup_job_prefix" default:"logical-backup-"`
	LogicalBackupCronjobEnvironmentSecret     string `name:"logical_backup_cronjob_environment_secret" default:""`
	LogicalBackupCPURequest                   string `name:"logical_backup_cpu_request"`
	LogicalBackupMemoryRequest                string `name:"logical_backup_memory_request"`
	LogicalBackupCPULimit                     string `name:"logical_backup_cpu_limit"`
	LogicalBackupMemoryLimit                  string `name:"logical_backup_memory_limit"`
	LogicalBackupSuccessfulJobsHistoryLimit   *int32 `name:"logical_backup_successful_jobs_history_limit" default:"3"`
	LogicalBackupFailedJobsHistoryLimit       *int32 `name:"logical_backup_failed_jobs_history_limit" default:"3"`
	LogicalBackupTTLSecondsAfterFinished      *int32 `name:"logical_backup_ttl_seconds_after_finished" default:"86400"`
}

// Operator options for connection pooler
type ConnectionPooler struct {
	NumberOfInstances                    *int32 `name:"connection_pooler_number_of_instances" default:"2"`
	Schema                               string `name:"connection_pooler_schema" default:"pooler"`
	User                                 string `name:"connection_pooler_user" default:"pooler"`
	Image                                string `name:"connection_pooler_image" default:"ghcr.io/zalando/postgres-operator/pgbouncer:v2.0.3"`
	Mode                                 string `name:"connection_pooler_mode" default:"transaction"`
	MaxDBConnections                     *int32 `name:"connection_pooler_max_db_connections" default:"60"`
	ConnectionPoolerDefaultCPURequest    string `name:"connection_pooler_default_cpu_request"`
	ConnectionPoolerDefaultMemoryRequest string `name:"connection_pooler_default_memory_request"`
	ConnectionPoolerDefaultCPULimit      string `name:"connection_pooler_default_cpu_limit"`
	ConnectionPoolerDefaultMemoryLimit   string `name:"connection_pooler_default_memory_limit"`
}

// Config describes operator config
type Config struct {
	CRD
	Resources
	Auth
	Scalyr
	LogicalBackup
	ConnectionPooler

	WatchedNamespace         string            `name:"watched_namespace"` // special values: "*" means 'watch all namespaces', the empty string "" means 'watch a namespace where operator is deployed to'
	KubernetesUseConfigMaps  *bool             `name:"kubernetes_use_configmaps" default:"true"`
	EtcdHost                 string            `name:"etcd_host" default:""` // special values: the empty string "" means Patroni will use K8s as a DCS
	EnableMaintenanceWindows *bool             `name:"enable_maintenance_windows" default:"true"`
	MaintenanceWindows       []string          `name:"maintenance_windows"`
	DockerImage              string            `name:"docker_image" default:"ghcr.io/zalando/spilo-18:4.1-p2"`
	SidecarImages            map[string]string `name:"sidecar_docker_images"` // deprecated in favour of SidecarContainers
	SidecarContainers        []v1.Container    `name:"sidecars"`
	PodServiceAccountName    string            `name:"pod_service_account_name" default:"postgres-pod"`
	// value of this string must be valid JSON or YAML; see initPodServiceAccount
	PodServiceAccountDefinition              string            `name:"pod_service_account_definition" default:""`
	PodServiceAccountRoleBindingDefinition   string            `name:"pod_service_account_role_binding_definition" default:""`
	MasterPodMoveTimeout                     *metav1.Duration  `name:"master_pod_move_timeout" default:"20m"`
	DbHostedZone                             string            `name:"db_hosted_zone" default:"db.example.com"`
	AWSRegion                                string            `name:"aws_region" default:"eu-central-1"`
	WALES3Bucket                             string            `name:"wal_s3_bucket"`
	LogS3Bucket                              string            `name:"log_s3_bucket"`
	KubeIAMRole                              string            `name:"kube_iam_role"`
	IRSARoleARN                              string            `name:"irsa_role_arn"`
	WALGSBucket                              string            `name:"wal_gs_bucket"`
	GCPCredentials                           string            `name:"gcp_credentials"`
	WALAZStorageAccount                      string            `name:"wal_az_storage_account"`
	AdditionalSecretMount                    string            `name:"additional_secret_mount"`
	AdditionalSecretMountPath                string            `name:"additional_secret_mount_path"`
	DebugLogging                             bool              `name:"debug_logging" default:"true"`
	EnableDBAccess                           bool              `name:"enable_database_access" default:"true"`
	EnableTeamsAPI                           bool              `name:"enable_teams_api" default:"true"`
	EnableTeamSuperuser                      bool              `name:"enable_team_superuser" default:"false"`
	TeamAdminRole                            string            `name:"team_admin_role" default:"admin"`
	RoleDeletionSuffix                       string            `name:"role_deletion_suffix" default:"_deleted"`
	EnableTeamMemberDeprecation              bool              `name:"enable_team_member_deprecation" default:"false"`
	EnableAdminRoleForUsers                  bool              `name:"enable_admin_role_for_users" default:"true"`
	EnablePostgresTeamCRD                    bool              `name:"enable_postgres_team_crd" default:"false"`
	EnablePostgresTeamCRDSuperusers          bool              `name:"enable_postgres_team_crd_superusers" default:"false"`
	EnableMasterLoadBalancer                 bool              `name:"enable_master_load_balancer" default:"true"`
	EnableMasterPoolerLoadBalancer           bool              `name:"enable_master_pooler_load_balancer" default:"false"`
	EnableReplicaLoadBalancer                bool              `name:"enable_replica_load_balancer" default:"false"`
	EnableReplicaPoolerLoadBalancer          bool              `name:"enable_replica_pooler_load_balancer" default:"false"`
	EnableMasterNodePort                     bool              `name:"enable_master_node_port" default:"false"`
	EnableMasterPoolerNodePort               bool              `name:"enable_master_pooler_node_port" default:"false"`
	EnableReplicaNodePort                    bool              `name:"enable_replica_node_port" default:"false"`
	EnableReplicaPoolerNodePort              bool              `name:"enable_replica_pooler_node_port" default:"false"`
	CustomServiceAnnotations                 map[string]string `name:"custom_service_annotations"`
	CustomPodAnnotations                     map[string]string `name:"custom_pod_annotations"`
	EnablePodAntiAffinity                    bool              `name:"enable_pod_antiaffinity" default:"false"`
	PodAntiAffinityPreferredDuringScheduling bool              `name:"pod_antiaffinity_preferred_during_scheduling" default:"false"`
	PodAntiAffinityTopologyKey               string            `name:"pod_antiaffinity_topology_key" default:"kubernetes.io/hostname"`
	StorageResizeMode                        string            `name:"storage_resize_mode" default:"pvc"`
	EnableLoadBalancer                       *bool             `name:"enable_load_balancer"` // deprecated and kept for backward compatibility
	ExternalTrafficPolicy                    string            `name:"external_traffic_policy" default:"Cluster"`
	MasterDNSNameFormat                      StringTemplate    `name:"master_dns_name_format" default:"{cluster}.{namespace}.{hostedzone}"`
	MasterLegacyDNSNameFormat                StringTemplate    `name:"master_legacy_dns_name_format" default:"{cluster}.{team}.{hostedzone}"`
	ReplicaDNSNameFormat                     StringTemplate    `name:"replica_dns_name_format" default:"{cluster}-repl.{namespace}.{hostedzone}"`
	ReplicaLegacyDNSNameFormat               StringTemplate    `name:"replica_legacy_dns_name_format" default:"{cluster}-repl.{team}.{hostedzone}"`
	PDBNameFormat                            StringTemplate    `name:"pdb_name_format" default:"postgres-{cluster}-pdb"`
	PDBMasterLabelSelector                   *bool             `name:"pdb_master_label_selector" default:"true"`
	EnablePodDisruptionBudget                *bool             `name:"enable_pod_disruption_budget" default:"true"`
	EnableInitContainers                     *bool             `name:"enable_init_containers" default:"true"`
	EnableSidecars                           *bool             `name:"enable_sidecars" default:"true"`
	SharePgSocketWithSidecars                *bool             `name:"share_pgsocket_with_sidecars" default:"false"`
	Workers                                  uint32            `name:"workers" default:"8"`
	APIPort                                  int               `name:"api_port" default:"8080"`
	RingLogLines                             int               `name:"ring_log_lines" default:"100"`
	ClusterHistoryEntries                    int               `name:"cluster_history_entries" default:"1000"`
	TeamAPIRoleConfiguration                 map[string]string `name:"team_api_role_configuration" default:"log_statement:all"`
	PodTerminateGracePeriod                  *metav1.Duration  `name:"pod_terminate_grace_period" default:"5m"`
	PodManagementPolicy                      string            `name:"pod_management_policy" default:"ordered_ready"`
	EnableReadinessProbe                     bool              `name:"enable_readiness_probe" default:"false"`
	ProtectedRoles                           []string          `name:"protected_role_names" default:"admin,cron_admin"`
	PostgresSuperuserTeams                   []string          `name:"postgres_superuser_teams" default:""`
	SetMemoryRequestToLimit                  bool              `name:"set_memory_request_to_limit" default:"false"`
	EnableLazySpiloUpgrade                   bool              `name:"enable_lazy_spilo_upgrade" default:"false"`
	EnableCrossNamespaceSecret               bool              `name:"enable_cross_namespace_secret" default:"false"`
	EnableFinalizers                         *bool             `name:"enable_finalizers" default:"false"`
	EnablePgVersionEnvVar                    bool              `name:"enable_pgversion_env_var" default:"true"`
	EnableSpiloWalPathCompat                 bool              `name:"enable_spilo_wal_path_compat" default:"false"`
	EnableTeamIdClusternamePrefix            bool              `name:"enable_team_id_clustername_prefix" default:"false"`
	MajorVersionUpgradeMode                  string            `name:"major_version_upgrade_mode" default:"manual"`
	MajorVersionUpgradeTeamAllowList         []string          `name:"major_version_upgrade_team_allow_list" default:""`
	MinimalMajorVersion                      string            `name:"minimal_major_version" default:"14"`
	TargetMajorVersion                       string            `name:"target_major_version" default:"18"`
	PatroniAPICheckInterval                  *metav1.Duration  `name:"patroni_api_check_interval" default:"1s"`
	PatroniAPICheckTimeout                   *metav1.Duration  `name:"patroni_api_check_timeout" default:"5s"`
	EnablePatroniFailsafeMode                *bool             `name:"enable_patroni_failsafe_mode" default:"false"`
	EnableSecretsDeletion                    *bool             `name:"enable_secrets_deletion" default:"true"`
	EnablePersistentVolumeClaimDeletion      *bool             `name:"enable_persistent_volume_claim_deletion" default:"true"`
	PersistentVolumeClaimRetentionPolicy     map[string]string `name:"persistent_volume_claim_retention_policy" default:"when_deleted:retain,when_scaled:retain"`
}

// MustMarshal marshals the config or panics
func (c Config) MustMarshal() string {
	b, err := json.MarshalIndent(c, "", "   ")
	if err != nil {
		panic(err)
	}

	return string(b)
}

// NewFromMap creates Config from the map
func NewFromMap(m map[string]string) *Config {
	cfg := Config{}
	fields, _ := structFields(&cfg)

	for _, structField := range fields {
		key := strings.ToLower(structField.Name)
		value, ok := m[key]
		if !ok && structField.Default != "" {
			value = structField.Default
		}

		if value == "" {
			continue
		}
		err := processField(value, structField.Field)
		if err != nil {
			panic(err)
		}
	}
	if err := validate(&cfg); err != nil {
		panic(err)
	}

	return &cfg
}

// Copy creates a copy of the config
func Copy(c *Config) Config {
	cfg := *c

	cfg.ClusterLabels = make(map[string]string, len(c.ClusterLabels))
	for k, v := range c.ClusterLabels {
		cfg.ClusterLabels[k] = v
	}

	return cfg
}

func validate(cfg *Config) (err error) {
	if cfg.MinInstances > 0 && cfg.MaxInstances > 0 && cfg.MinInstances > cfg.MaxInstances {
		err = fmt.Errorf("minimum number of instances %d is set higher than the maximum number %d",
			cfg.MinInstances, cfg.MaxInstances)
	}
	if cfg.Workers == 0 {
		err = fmt.Errorf("number of workers should be higher than 0")
	}

	if *cfg.ConnectionPooler.NumberOfInstances < constants.ConnectionPoolerMinInstances {
		msg := "number of connection pooler instances should be higher than %d"
		err = fmt.Errorf(msg, constants.ConnectionPoolerMinInstances)
	}

	if cfg.ConnectionPooler.User == cfg.SuperUsername {
		msg := "connection pool user is not allowed to be the same as super user, username: %s"
		err = fmt.Errorf(msg, cfg.ConnectionPooler.User)
	}

	return
}
