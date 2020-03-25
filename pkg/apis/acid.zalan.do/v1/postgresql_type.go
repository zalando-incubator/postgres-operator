package v1

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

	TeamID      string `json:"teamId"`
	DockerImage string `json:"dockerImage,omitempty"`

	SpiloFSGroup *int64 `json:"spiloFSGroup,omitempty"`

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

	NumberOfInstances     int32                `json:"numberOfInstances"`
	Users                 map[string]UserFlags `json:"users"`
	MaintenanceWindows    []MaintenanceWindow  `json:"maintenanceWindows,omitempty"`
	Clone                 CloneDescription     `json:"clone"`
	ClusterName           string               `json:"-"`
	Databases             map[string]string    `json:"databases,omitempty"`
	Tolerations           []v1.Toleration      `json:"tolerations,omitempty"`
	Sidecars              []Sidecar            `json:"sidecars,omitempty"`
	InitContainers        []v1.Container       `json:"initContainers,omitempty"`
	PodPriorityClassName  string               `json:"podPriorityClassName,omitempty"`
	ShmVolume             *bool                `json:"enableShmVolume,omitempty"`
	EnableLogicalBackup   bool                 `json:"enableLogicalBackup,omitempty"`
	LogicalBackupSchedule string               `json:"logicalBackupSchedule,omitempty"`
	StandbyCluster        *StandbyDescription  `json:"standby"`
	PodAnnotations        map[string]string    `json:"podAnnotations"`
	ServiceAnnotations    map[string]string    `json:"serviceAnnotations"`
	TLS                   *TLSDescription      `json:"tls"`

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
	StorageClass string `json:"storageClass"`
	SubPath      string `json:"subPath,omitempty"`
}

// PostgresqlParam describes PostgreSQL version and pairs of configuration parameter name - values.
type PostgresqlParam struct {
	PgVersion  string            `json:"version"`
	Parameters map[string]string `json:"parameters"`
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
	InitDB               map[string]string            `json:"initdb"`
	PgHba                []string                     `json:"pg_hba"`
	TTL                  uint32                       `json:"ttl"`
	LoopWait             uint32                       `json:"loop_wait"`
	RetryTimeout         uint32                       `json:"retry_timeout"`
	MaximumLagOnFailover float32                      `json:"maximum_lag_on_failover"` // float32 because https://github.com/kubernetes/kubernetes/issues/30213
	Slots                map[string]map[string]string `json:"slots"`
}

//StandbyCluster
type StandbyDescription struct {
	S3WalPath      string `json:"s3_wal_path,omitempty"`
	StandbyOptions map[string]string
}

type TLSDescription struct {
	SecretName      string `json:"secretName,omitempty"`
	CertificateFile string `json:"certificateFile,omitempty"`
	PrivateKeyFile  string `json:"privateKeyFile,omitempty"`
	CAFile          string `json:"caFile,omitempty"`
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
