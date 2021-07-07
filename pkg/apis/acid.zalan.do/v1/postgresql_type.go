package v1

// Postgres CRD definition, please use CamelCase for field names.

import (
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Postgresql defines PostgreSQL Custom Resource Definition Object.
type Postgresql struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PostgresSpec   `json:"spec"`
	Status PostgresStatus `json:"status"`
	Error  string         `json:"-"`
}

// PostgresSpec defines the specification for the PostgreSQL TPR.
type PostgresSpec struct {
	PostgresqlParam `json:"postgresql"`
	Volume          `json:"volume,omitempty"`
	Patroni         `json:"patroni,omitempty"`
	Resources       `json:"resources,omitempty"`

	EnableConnectionPooler        *bool             `json:"enableConnectionPooler,omitempty"`
	EnableReplicaConnectionPooler *bool             `json:"enableReplicaConnectionPooler,omitempty"`
	ConnectionPooler              *ConnectionPooler `json:"connectionPooler,omitempty"`

	TeamID      string `json:"teamId"`
	DockerImage string `json:"dockerImage,omitempty"`

	SpiloRunAsUser  *int64 `json:"spiloRunAsUser,omitempty"`
	SpiloRunAsGroup *int64 `json:"spiloRunAsGroup,omitempty"`
	SpiloFSGroup    *int64 `json:"spiloFSGroup,omitempty"`

	// vars that enable load balancers are pointers because it is important to know if any of them is omitted from the Postgres manifest
	// in that case the var evaluates to nil and the value is taken from the operator config
	EnableMasterLoadBalancer  *bool `json:"enableMasterLoadBalancer,omitempty"`
	EnableReplicaLoadBalancer *bool `json:"enableReplicaLoadBalancer,omitempty"`

	// deprecated load balancer settings maintained for backward compatibility
	// see "Load balancers" operator docs
	UseLoadBalancer     *bool `json:"useLoadBalancer,omitempty"`
	ReplicaLoadBalancer *bool `json:"replicaLoadBalancer,omitempty"`

	// load balancers' source ranges are the same for master and replica services
	AllowedSourceRanges []string `json:"allowedSourceRanges"`

	NumberOfInstances     int32                       `json:"numberOfInstances"`
	Users                 map[string]UserFlags        `json:"users,omitempty"`
	MaintenanceWindows    []MaintenanceWindow         `json:"maintenanceWindows,omitempty"`
	Clone                 *CloneDescription           `json:"clone,omitempty"`
	ClusterName           string                      `json:"-"`
	Databases             map[string]string           `json:"databases,omitempty"`
	PreparedDatabases     map[string]PreparedDatabase `json:"preparedDatabases,omitempty"`
	SchedulerName         *string                     `json:"schedulerName,omitempty"`
	NodeAffinity          *v1.NodeAffinity            `json:"nodeAffinity,omitempty"`
	Tolerations           []v1.Toleration             `json:"tolerations,omitempty"`
	Sidecars              []Sidecar                   `json:"sidecars,omitempty"`
	InitContainers        []v1.Container              `json:"initContainers,omitempty"`
	PodPriorityClassName  string                      `json:"podPriorityClassName,omitempty"`
	ShmVolume             *bool                       `json:"enableShmVolume,omitempty"`
	EnableLogicalBackup   bool                        `json:"enableLogicalBackup,omitempty"`
	LogicalBackupSchedule string                      `json:"logicalBackupSchedule,omitempty"`
	StandbyCluster        *StandbyDescription         `json:"standby,omitempty"`
	PodAnnotations        map[string]string           `json:"podAnnotations,omitempty"`
	ServiceAnnotations    map[string]string           `json:"serviceAnnotations,omitempty"`
	TLS                   *TLSDescription             `json:"tls,omitempty"`
	AdditionalVolumes     []AdditionalVolume          `json:"additionalVolumes,omitempty"`

	// deprecated json tags
	InitContainersOld       []v1.Container `json:"init_containers,omitempty"`
	PodPriorityClassNameOld string         `json:"pod_priority_class_name,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PostgresqlList defines a list of PostgreSQL clusters.
type PostgresqlList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []Postgresql `json:"items"`
}

// PreparedDatabase describes elements to be bootstrapped
type PreparedDatabase struct {
	PreparedSchemas map[string]PreparedSchema `json:"schemas,omitempty"`
	DefaultUsers    bool                      `json:"defaultUsers,omitempty" defaults:"false"`
	Extensions      map[string]string         `json:"extensions,omitempty"`
}

// PreparedSchema describes elements to be bootstrapped per schema
type PreparedSchema struct {
	DefaultRoles *bool `json:"defaultRoles,omitempty" defaults:"true"`
	DefaultUsers bool  `json:"defaultUsers,omitempty" defaults:"false"`
}

// MaintenanceWindow describes the time window when the operator is allowed to do maintenance on a cluster.
type MaintenanceWindow struct {
	Everyday  bool
	Weekday   time.Weekday
	StartTime metav1.Time // Start time
	EndTime   metav1.Time // End time
}

// Volume describes a single volume in the manifest.
type Volume struct {
	Size         string `json:"size"`
	StorageClass string `json:"storageClass,omitempty"`
	SubPath      string `json:"subPath,omitempty"`
	Iops         *int64 `json:"iops,omitempty"`
	Throughput   *int64 `json:"throughput,omitempty"`
	VolumeType   string `json:"type,omitempty"`
}

// AdditionalVolume specs additional optional volumes for statefulset
type AdditionalVolume struct {
	Name             string          `json:"name"`
	MountPath        string          `json:"mountPath"`
	SubPath          string          `json:"subPath,omitempty"`
	TargetContainers []string        `json:"targetContainers"`
	VolumeSource     v1.VolumeSource `json:"volumeSource"`
}

// PostgresqlParam describes PostgreSQL version and pairs of configuration parameter name - values.
type PostgresqlParam struct {
	PgVersion  string            `json:"version"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

// ResourceDescription describes CPU and memory resources defined for a cluster.
type ResourceDescription struct {
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
}

// Resources describes requests and limits for the cluster resouces.
type Resources struct {
	ResourceRequests ResourceDescription `json:"requests,omitempty"`
	ResourceLimits   ResourceDescription `json:"limits,omitempty"`
}

// Patroni contains Patroni-specific configuration
type Patroni struct {
	InitDB                map[string]string            `json:"initdb,omitempty"`
	PgHba                 []string                     `json:"pg_hba,omitempty"`
	TTL                   uint32                       `json:"ttl,omitempty"`
	LoopWait              uint32                       `json:"loop_wait,omitempty"`
	RetryTimeout          uint32                       `json:"retry_timeout,omitempty"`
	MaximumLagOnFailover  float32                      `json:"maximum_lag_on_failover,omitempty"` // float32 because https://github.com/kubernetes/kubernetes/issues/30213
	Slots                 map[string]map[string]string `json:"slots,omitempty"`
	SynchronousMode       bool                         `json:"synchronous_mode,omitempty"`
	SynchronousModeStrict bool                         `json:"synchronous_mode_strict,omitempty"`
}

// StandbyDescription contains remote primary config or s3 wal path
type StandbyDescription struct {
	S3WalPath   string `json:"s3_wal_path,omitempty"`
	StandbyHost string `json:"standby_host,omitempty"`
	StandbyPort string `json:"standby_port,omitempty"`
}

// TLSDescription specs TLS properties
type TLSDescription struct {
	SecretName      string `json:"secretName,omitempty"`
	CertificateFile string `json:"certificateFile,omitempty"`
	PrivateKeyFile  string `json:"privateKeyFile,omitempty"`
	CAFile          string `json:"caFile,omitempty"`
	CASecretName    string `json:"caSecretName,omitempty"`
}

// CloneDescription describes which cluster the new should clone and up to which point in time
type CloneDescription struct {
	ClusterName       string `json:"cluster,omitempty"`
	UID               string `json:"uid,omitempty"`
	EndTimestamp      string `json:"timestamp,omitempty"`
	S3WalPath         string `json:"s3_wal_path,omitempty"`
	S3Endpoint        string `json:"s3_endpoint,omitempty"`
	S3AccessKeyId     string `json:"s3_access_key_id,omitempty"`
	S3SecretAccessKey string `json:"s3_secret_access_key,omitempty"`
	S3ForcePathStyle  *bool  `json:"s3_force_path_style,omitempty" defaults:"false"`
}

// Sidecar defines a container to be run in the same pod as the Postgres container.
type Sidecar struct {
	Resources   `json:"resources,omitempty"`
	Name        string             `json:"name,omitempty"`
	DockerImage string             `json:"image,omitempty"`
	Ports       []v1.ContainerPort `json:"ports,omitempty"`
	Env         []v1.EnvVar        `json:"env,omitempty"`
}

// UserFlags defines flags (such as superuser, nologin) that could be assigned to individual users
type UserFlags []string

// PostgresStatus contains status of the PostgreSQL cluster (running, creation failed etc.)
type PostgresStatus struct {
	PostgresClusterStatus string `json:"PostgresClusterStatus"`
}

// ConnectionPooler Options for connection pooler
//
// TODO: prepared snippets of configuration, one can choose via type, e.g.
// pgbouncer-large (with higher resources) or odyssey-small (with smaller
// resources)
// Type              string `json:"type,omitempty"`
//
// TODO: figure out what other important parameters of the connection pooler it
// makes sense to expose. E.g. pool size (min/max boundaries), max client
// connections etc.
type ConnectionPooler struct {
	NumberOfInstances *int32 `json:"numberOfInstances,omitempty"`
	Schema            string `json:"schema,omitempty"`
	User              string `json:"user,omitempty"`
	Mode              string `json:"mode,omitempty"`
	DockerImage       string `json:"dockerImage,omitempty"`
	MaxDBConnections  *int32 `json:"maxDBConnections,omitempty"`

	Resources `json:"resources,omitempty"`
}
