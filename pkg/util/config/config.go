package config

import (
	"encoding/json"
	"strings"
	"time"

	"fmt"

	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	v1 "k8s.io/api/core/v1"
)

// CRD describes CustomResourceDefinition specific configuration parameters
type CRD struct {
	ReadyWaitInterval   time.Duration `name:"ready_wait_interval" default:"4s"`
	ReadyWaitTimeout    time.Duration `name:"ready_wait_timeout" default:"30s"`
	ResyncPeriod        time.Duration `name:"resync_period" default:"30m"`
	RepairPeriod        time.Duration `name:"repair_period" default:"5m"`
	EnableCRDValidation *bool         `name:"enable_crd_validation" default:"true"`
}

// Resources describes kubernetes resource specific configuration parameters
type Resources struct {
	ResourceCheckInterval   time.Duration       `name:"resource_check_interval" default:"3s"`
	ResourceCheckTimeout    time.Duration       `name:"resource_check_timeout" default:"10m"`
	PodLabelWaitTimeout     time.Duration       `name:"pod_label_wait_timeout" default:"10m"`
	PodDeletionWaitTimeout  time.Duration       `name:"pod_deletion_wait_timeout" default:"10m"`
	PodTerminateGracePeriod time.Duration       `name:"pod_terminate_grace_period" default:"5m"`
	SpiloFSGroup            *int64              `name:"spilo_fsgroup"`
	PodPriorityClassName    string              `name:"pod_priority_class_name"`
	ClusterDomain           string              `name:"cluster_domain" default:"cluster.local"`
	SpiloPrivileged         bool                `name:"spilo_privileged" default:"false"`
	ClusterLabels           map[string]string   `name:"cluster_labels" default:"application:spilo"`
	InheritedLabels         []string            `name:"inherited_labels" default:""`
	DownscalerAnnotations   []string            `name:"downscaler_annotations"`
	ClusterNameLabel        string              `name:"cluster_name_label" default:"cluster-name"`
	PodRoleLabel            string              `name:"pod_role_label" default:"spilo-role"`
	PodToleration           map[string]string   `name:"toleration" default:""`
	DefaultCPURequest       string              `name:"default_cpu_request" default:"100m"`
	DefaultMemoryRequest    string              `name:"default_memory_request" default:"100Mi"`
	DefaultCPULimit         string              `name:"default_cpu_limit" default:"1"`
	DefaultMemoryLimit      string              `name:"default_memory_limit" default:"500Mi"`
	MinCPULimit             string              `name:"min_cpu_limit" default:"250m"`
	MinMemoryLimit          string              `name:"min_memory_limit" default:"250Mi"`
	PodEnvironmentConfigMap spec.NamespacedName `name:"pod_environment_configmap"`
	PodEnvironmentSecret    string              `name:"pod_environment_secret"`
	NodeReadinessLabel      map[string]string   `name:"node_readiness_label" default:""`
	MaxInstances            int32               `name:"max_instances" default:"-1"`
	MinInstances            int32               `name:"min_instances" default:"-1"`
	ShmVolume               *bool               `name:"enable_shm_volume" default:"true"`
}

type InfrastructureRole struct {
	// Name of a secret which describes the role, and optionally name of a
	// configmap with an extra information
	SecretName spec.NamespacedName

	UserKey     string
	PasswordKey string
	RoleKey     string

	// This field point out the detailed yaml definition of the role, if exists
	Details string

	// Specify if a secret contains multiple fields in the following format:
	//
	// 	%(userkey)idx: ...
	// 	%(passwordkey)idx: ...
	// 	%(rolekey)idx: ...
	//
	// If it does, Name/Password/Role are interpreted not as unique field
	// names, but as a template.

	Template bool
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
	LogicalBackupSchedule          string `name:"logical_backup_schedule" default:"30 00 * * *"`
	LogicalBackupDockerImage       string `name:"logical_backup_docker_image" default:"registry.opensource.zalan.do/acid/logical-backup"`
	LogicalBackupS3Bucket          string `name:"logical_backup_s3_bucket" default:""`
	LogicalBackupS3Region          string `name:"logical_backup_s3_region" default:""`
	LogicalBackupS3Endpoint        string `name:"logical_backup_s3_endpoint" default:""`
	LogicalBackupS3AccessKeyID     string `name:"logical_backup_s3_access_key_id" default:""`
	LogicalBackupS3SecretAccessKey string `name:"logical_backup_s3_secret_access_key" default:""`
	LogicalBackupS3SSE             string `name:"logical_backup_s3_sse" default:""`
}

// Operator options for connection pooler
type ConnectionPooler struct {
	NumberOfInstances                    *int32 `name:"connection_pooler_number_of_instances" default:"2"`
	Schema                               string `name:"connection_pooler_schema" default:"pooler"`
	User                                 string `name:"connection_pooler_user" default:"pooler"`
	Image                                string `name:"connection_pooler_image" default:"registry.opensource.zalan.do/acid/pgbouncer"`
	Mode                                 string `name:"connection_pooler_mode" default:"transaction"`
	MaxDBConnections                     *int32 `name:"connection_pooler_max_db_connections" default:"60"`
	ConnectionPoolerDefaultCPURequest    string `name:"connection_pooler_default_cpu_request" default:"500m"`
	ConnectionPoolerDefaultMemoryRequest string `name:"connection_pooler_default_memory_request" default:"100Mi"`
	ConnectionPoolerDefaultCPULimit      string `name:"connection_pooler_default_cpu_limit" default:"1"`
	ConnectionPoolerDefaultMemoryLimit   string `name:"connection_pooler_default_memory_limit" default:"100Mi"`
}

// Config describes operator config
type Config struct {
	CRD
	Resources
	Auth
	Scalyr
	LogicalBackup
	ConnectionPooler

	WatchedNamespace        string `name:"watched_namespace"` // special values: "*" means 'watch all namespaces', the empty string "" means 'watch a namespace where operator is deployed to'
	KubernetesUseConfigMaps bool   `name:"kubernetes_use_configmaps" default:"false"`
	EtcdHost                string `name:"etcd_host" default:""` // special values: the empty string "" means Patroni will use K8s as a DCS
	DockerImage             string `name:"docker_image" default:"registry.opensource.zalan.do/acid/spilo-12:1.6-p3"`
	// deprecated in favour of SidecarContainers
	SidecarImages         map[string]string `name:"sidecar_docker_images"`
	SidecarContainers     []v1.Container    `name:"sidecars"`
	PodServiceAccountName string            `name:"pod_service_account_name" default:"postgres-pod"`
	// value of this string must be valid JSON or YAML; see initPodServiceAccount
	PodServiceAccountDefinition            string            `name:"pod_service_account_definition" default:""`
	PodServiceAccountRoleBindingDefinition string            `name:"pod_service_account_role_binding_definition" default:""`
	MasterPodMoveTimeout                   time.Duration     `name:"master_pod_move_timeout" default:"20m"`
	DbHostedZone                           string            `name:"db_hosted_zone" default:"db.example.com"`
	AWSRegion                              string            `name:"aws_region" default:"eu-central-1"`
	WALES3Bucket                           string            `name:"wal_s3_bucket"`
	LogS3Bucket                            string            `name:"log_s3_bucket"`
	KubeIAMRole                            string            `name:"kube_iam_role"`
	WALGSBucket                            string            `name:"wal_gs_bucket"`
	GCPCredentials                         string            `name:"gcp_credentials"`
	AdditionalSecretMount                  string            `name:"additional_secret_mount"`
	AdditionalSecretMountPath              string            `name:"additional_secret_mount_path" default:"/meta/credentials"`
	DebugLogging                           bool              `name:"debug_logging" default:"true"`
	EnableDBAccess                         bool              `name:"enable_database_access" default:"true"`
	EnableTeamsAPI                         bool              `name:"enable_teams_api" default:"true"`
	EnableTeamSuperuser                    bool              `name:"enable_team_superuser" default:"false"`
	TeamAdminRole                          string            `name:"team_admin_role" default:"admin"`
	EnableAdminRoleForUsers                bool              `name:"enable_admin_role_for_users" default:"true"`
	EnableMasterLoadBalancer               bool              `name:"enable_master_load_balancer" default:"true"`
	EnableReplicaLoadBalancer              bool              `name:"enable_replica_load_balancer" default:"false"`
	CustomServiceAnnotations               map[string]string `name:"custom_service_annotations"`
	CustomPodAnnotations                   map[string]string `name:"custom_pod_annotations"`
	EnablePodAntiAffinity                  bool              `name:"enable_pod_antiaffinity" default:"false"`
	PodAntiAffinityTopologyKey             string            `name:"pod_antiaffinity_topology_key" default:"kubernetes.io/hostname"`
	StorageResizeMode                      string            `name:"storage_resize_mode" default:"ebs"`
	// deprecated and kept for backward compatibility
	EnableLoadBalancer        *bool             `name:"enable_load_balancer"`
	MasterDNSNameFormat       StringTemplate    `name:"master_dns_name_format" default:"{cluster}.{team}.{hostedzone}"`
	ReplicaDNSNameFormat      StringTemplate    `name:"replica_dns_name_format" default:"{cluster}-repl.{team}.{hostedzone}"`
	PDBNameFormat             StringTemplate    `name:"pdb_name_format" default:"postgres-{cluster}-pdb"`
	EnablePodDisruptionBudget *bool             `name:"enable_pod_disruption_budget" default:"true"`
	EnableInitContainers      *bool             `name:"enable_init_containers" default:"true"`
	EnableSidecars            *bool             `name:"enable_sidecars" default:"true"`
	SharePGSocketWithSidecars *bool             `name:"share_pg_socket_with_sidecars" default:"false"`
	Workers                   uint32            `name:"workers" default:"8"`
	APIPort                   int               `name:"api_port" default:"8080"`
	RingLogLines              int               `name:"ring_log_lines" default:"100"`
	ClusterHistoryEntries     int               `name:"cluster_history_entries" default:"1000"`
	TeamAPIRoleConfiguration  map[string]string `name:"team_api_role_configuration" default:"log_statement:all"`
	PodTerminateGracePeriod   time.Duration     `name:"pod_terminate_grace_period" default:"5m"`
	PodManagementPolicy       string            `name:"pod_management_policy" default:"ordered_ready"`
	ProtectedRoles            []string          `name:"protected_role_names" default:"admin"`
	PostgresSuperuserTeams    []string          `name:"postgres_superuser_teams" default:""`
	SetMemoryRequestToLimit   bool              `name:"set_memory_request_to_limit" default:"false"`
	EnableLazySpiloUpgrade    bool              `name:"enable_lazy_spilo_upgrade" default:"false"`
}

// MustMarshal marshals the config or panics
func (c Config) MustMarshal() string {
	b, err := json.MarshalIndent(c, "", "\t")
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
		msg := "Connection pool user is not allowed to be the same as super user, username: %s"
		err = fmt.Errorf(msg, cfg.ConnectionPooler.User)
	}

	return
}
