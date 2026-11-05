package v1

// Postgres CRD definition, please use CamelCase for field names.

import (
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true

// Postgresql defines PostgreSQL Custom Resource Definition Object.
// +kubebuilder:resource:categories=all,shortName=pg,scope=Namespaced
// +kubebuilder:printcolumn:name="Team",type=string,JSONPath=`.spec.teamId`,description="Team responsible for Postgres cluster"
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.postgresql.version`,description="PostgreSQL version"
// +kubebuilder:printcolumn:name="Pods",type=integer,JSONPath=`.spec.numberOfInstances`,description="Number of Pods per Postgres cluster"
// +kubebuilder:printcolumn:name="Volume",type=string,JSONPath=`.spec.volume.size`,description="Size of the bound volume"
// +kubebuilder:printcolumn:name="CPU-Request",type=string,JSONPath=`.spec.resources.requests.cpu`,description="Requested CPU for Postgres containers"
// +kubebuilder:printcolumn:name="Memory-Request",type=string,JSONPath=`.spec.resources.requests.memory`,description="Requested memory for Postgres containers"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`,description="Age of the PostgreSQL cluster"
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.PostgresClusterStatus`,description="Current sync status of postgresql resource"
// +kubebuilder:subresource:status
// +kubebuilder:metadata:labels=app.kubernetes.io/name=postgres-operator
type Postgresql struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec PostgresSpec `json:"spec"`
	// +optional
	Status PostgresStatus `json:"status"`
	Error  string         `json:"-"`
}

// PostgresSpec defines the specification for the PostgreSQL TPR.
type PostgresSpec struct {
	PostgresqlParam `json:"postgresql"`
	Volume          `json:"volume"`
	// +optional
	Patroni    `json:"patroni"`
	*Resources `json:"resources,omitempty"`

	EnableConnectionPooler        *bool             `json:"enableConnectionPooler,omitempty"`
	EnableReplicaConnectionPooler *bool             `json:"enableReplicaConnectionPooler,omitempty"`
	ConnectionPooler              *ConnectionPooler `json:"connectionPooler,omitempty"`

	TeamID      string `json:"teamId"`
	DockerImage string `json:"dockerImage,omitempty"`

	// deprecated field storing cluster name without teamId prefix
	ClusterName string `json:"-"`

	SpiloRunAsUser  *int64 `json:"spiloRunAsUser,omitempty"`
	SpiloRunAsGroup *int64 `json:"spiloRunAsGroup,omitempty"`
	SpiloFSGroup    *int64 `json:"spiloFSGroup,omitempty"`

	// vars that enable load balancers are pointers because it is important to know if any of them is omitted from the Postgres manifest
	// in that case the var evaluates to nil and the value is taken from the operator config
	EnableMasterLoadBalancer        *bool `json:"enableMasterLoadBalancer,omitempty"`
	EnableMasterPoolerLoadBalancer  *bool `json:"enableMasterPoolerLoadBalancer,omitempty"`
	EnableReplicaLoadBalancer       *bool `json:"enableReplicaLoadBalancer,omitempty"`
	EnableReplicaPoolerLoadBalancer *bool `json:"enableReplicaPoolerLoadBalancer,omitempty"`

	// vars to enable and configure nodeport services
	// set ports to 0 or nil to let kubernetes decide which port to use
	// overrides loadbalancer configuration
	EnableMasterNodePort        *bool  `json:"enableMasterNodePort,omitempty"`
	MasterNodePort              *int32 `json:"masterNodePort,omitempty"`
	EnableMasterPoolerNodePort  *bool  `json:"enableMasterPoolerNodePort,omitempty"`
	MasterPoolerNodePort        *int32 `json:"masterPoolerNodePort,omitempty"`
	EnableReplicaNodePort       *bool  `json:"enableReplicaNodePort,omitempty"`
	ReplicaNodePort             *int32 `json:"replicaNodePort,omitempty"`
	EnableReplicaPoolerNodePort *bool  `json:"enableReplicaPoolerNodePort,omitempty"`
	ReplicaPoolerNodePort       *int32 `json:"replicaPoolerNodePort,omitempty"`

	// load balancers' source ranges are the same for master and replica services
	// +nullable
	// +kubebuilder:validation:items:Pattern=`^((\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\.(\d|[1-9]\d|1\d\d|2[0-4]\d|25[0-5])\/(\d|[1-2]\d|3[0-2])|(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))\/(12[0-8]|1[01][0-9]|[1-9]?[0-9]))$`
	// +optional
	AllowedSourceRanges []string `json:"allowedSourceRanges"`

	Users map[string]UserFlags `json:"users,omitempty"`
	// +nullable
	UsersIgnoringSecretRotation []string `json:"usersIgnoringSecretRotation,omitempty"`
	// +nullable
	UsersWithSecretRotation []string `json:"usersWithSecretRotation,omitempty"`
	// +nullable
	UsersWithInPlaceSecretRotation []string `json:"usersWithInPlaceSecretRotation,omitempty"`

	// +kubebuilder:validation:Minimum=0
	NumberOfInstances int32 `json:"numberOfInstances"`
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type=array
	MaintenanceWindows []MaintenanceWindow `json:"maintenanceWindows,omitempty"`
	Clone              *CloneDescription   `json:"clone,omitempty"`
	// Note: usernames specified here as database owners must be declared
	// in the users key of the spec key.
	Databases                 map[string]string             `json:"databases,omitempty"`
	PreparedDatabases         map[string]PreparedDatabase   `json:"preparedDatabases,omitempty"`
	SchedulerName             *string                       `json:"schedulerName,omitempty"`
	NodeAffinity              *v1.NodeAffinity              `json:"nodeAffinity,omitempty"`
	TopologySpreadConstraints []v1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	Tolerations               []v1.Toleration               `json:"tolerations,omitempty"`
	LivenessProbe             *v1.Probe                     `json:"livenessProbe,omitempty"`
	Sidecars                  []Sidecar                     `json:"sidecars,omitempty"`
	InitContainers            []v1.Container                `json:"initContainers,omitempty"`
	PodPriorityClassName      string                        `json:"podPriorityClassName,omitempty"`
	ShmVolume                 *bool                         `json:"enableShmVolume,omitempty"`
	EnableLogicalBackup       bool                          `json:"enableLogicalBackup,omitempty"`
	LogicalBackupRetention    string                        `json:"logicalBackupRetention,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+|\*)(/\d+)?(\s+(\d+|\*)(/\d+)?){4}$`
	LogicalBackupSchedule string              `json:"logicalBackupSchedule,omitempty"`
	StandbyCluster        *StandbyDescription `json:"standby,omitempty"`
	PodAnnotations        map[string]string   `json:"podAnnotations,omitempty"`
	ServiceAnnotations    map[string]string   `json:"serviceAnnotations,omitempty"`
	// MasterServiceAnnotations takes precedence over ServiceAnnotations for master role if not empty
	MasterServiceAnnotations map[string]string `json:"masterServiceAnnotations,omitempty"`
	// ReplicaServiceAnnotations takes precedence over ServiceAnnotations for replica role if not empty
	ReplicaServiceAnnotations map[string]string  `json:"replicaServiceAnnotations,omitempty"`
	TLS                       *TLSDescription    `json:"tls,omitempty"`
	AdditionalVolumes         []AdditionalVolume `json:"additionalVolumes,omitempty"`
	Streams                   []Stream           `json:"streams,omitempty"`
	Env                       []v1.EnvVar        `json:"env,omitempty"`
	EnvFrom                   []v1.EnvFromSource `json:"envFrom,omitempty"`
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
	SecretNamespace string                    `json:"secretNamespace,omitempty"`
}

// PreparedSchema describes elements to be bootstrapped per schema
type PreparedSchema struct {
	DefaultRoles *bool `json:"defaultRoles,omitempty" defaults:"true"`
	DefaultUsers bool  `json:"defaultUsers,omitempty" defaults:"false"`
}

// MaintenanceWindow describes the time window when the operator is allowed to do maintenance on a cluster.
type MaintenanceWindow struct {
	Everyday  bool         `json:"everyday,omitempty"`
	Weekday   time.Weekday `json:"weekday,omitempty"`
	StartTime metav1.Time  `json:"startTime"`
	EndTime   metav1.Time  `json:"endTime"`
}

// Volume describes a single volume in the manifest.
type Volume struct {
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	Size          string `json:"size"`
	StorageClass  string `json:"storageClass,omitempty"`
	SubPath       string `json:"subPath,omitempty"`
	IsSubPathExpr *bool  `json:"isSubPathExpr,omitempty"`
	// +kubebuilder:validation:Maximum=80000
	Iops *int32 `json:"iops,omitempty"`
	// +kubebuilder:validation:Maximum=2000
	Throughput *int32 `json:"throughput,omitempty"`
	VolumeType string `json:"type,omitempty"`
}

// AdditionalVolume specs additional optional volumes for statefulset
type AdditionalVolume struct {
	Name          string `json:"name"`
	MountPath     string `json:"mountPath"`
	SubPath       string `json:"subPath,omitempty"`
	IsSubPathExpr *bool  `json:"isSubPathExpr,omitempty"`
	// +nullable
	// +optional
	TargetContainers []string `json:"targetContainers"`
	// +kubebuilder:validation:XPreserveUnknownFields
	// +kubebuilder:validation:Type=object
	// +kubebuilder:validation:Schemaless
	VolumeSource v1.VolumeSource `json:"volumeSource"`
}

// PostgresqlParam describes PostgreSQL version and pairs of configuration parameter name - values.
type PostgresqlParam struct {
	// +kubebuilder:validation:Enum="14";"15";"16";"17";"18"
	PgVersion  string            `json:"version"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

// ResourceDescription describes CPU and memory resources defined for a cluster.
type ResourceDescription struct {
	// Decimal natural followed by m, or decimal natural followed by
	// dot followed by up to three decimal digits.
	//
	// This is because the Kubernetes CPU resource has millis as the
	// maximum precision.  The actual values are checked in code
	// because the regular expression would be huge and horrible and
	// not very helpful in validation error messages; this one checks
	// only the format of the given number.
	//
	// https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-cpu
	//
	// Note: the value specified here must not be zero or be lower
	// than the corresponding request.
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	CPU *string `json:"cpu,omitempty"`
	// You can express memory as a plain integer or as a fixed-point
	// integer using one of these suffixes: E, P, T, G, M, k. You can
	// also use the power-of-two equivalents: Ei, Pi, Ti, Gi, Mi, Ki
	//
	// https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-memory
	//
	// Note: the value specified here must not be zero or be higher
	// than the corresponding limit.
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	Memory *string `json:"memory,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	HugePages2Mi *string `json:"hugepages-2Mi,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	HugePages1Gi *string `json:"hugepages-1Gi,omitempty"`
}

// Resources describes requests and limits for the cluster resouces.
type Resources struct {
	// +optional
	ResourceRequests ResourceDescription `json:"requests"`
	// +optional
	ResourceLimits ResourceDescription `json:"limits"`
}

// Patroni contains Patroni-specific configuration
type Patroni struct {
	InitDB                map[string]string            `json:"initdb,omitempty"`
	PgHba                 []string                     `json:"pg_hba,omitempty"`
	TTL                   uint32                       `json:"ttl,omitempty"`
	LoopWait              uint32                       `json:"loop_wait,omitempty"`
	RetryTimeout          uint32                       `json:"retry_timeout,omitempty"`
	MaximumLagOnFailover  int64                        `json:"maximum_lag_on_failover,omitempty"`
	Slots                 map[string]map[string]string `json:"slots,omitempty"`
	SynchronousMode       bool                         `json:"synchronous_mode"`
	SynchronousModeStrict bool                         `json:"synchronous_mode_strict"`
	SynchronousNodeCount  uint32                       `json:"synchronous_node_count,omitempty" defaults:"1"`
	FailsafeMode          *bool                        `json:"failsafe_mode,omitempty"`
}

// StandbyDescription contains remote primary config and/or s3/gs wal path.
// standby_host can be specified alone or together with either s3_wal_path OR gs_wal_path (mutually exclusive).
// At least one field must be specified. s3_wal_path and gs_wal_path are mutually exclusive.
type StandbyDescription struct {
	S3WalPath              string `json:"s3_wal_path,omitempty"`
	GSWalPath              string `json:"gs_wal_path,omitempty"`
	StandbyHost            string `json:"standby_host,omitempty"`
	StandbyPort            string `json:"standby_port,omitempty"`
	StandbyPrimarySlotName string `json:"standby_primary_slot_name,omitempty"`
}

// TLSDescription specs TLS properties
type TLSDescription struct {
	// +required
	SecretName      string `json:"secretName,omitempty"`
	CertificateFile string `json:"certificateFile,omitempty"`
	PrivateKeyFile  string `json:"privateKeyFile,omitempty"`
	CAFile          string `json:"caFile,omitempty"`
	CASecretName    string `json:"caSecretName,omitempty"`
}

// CloneDescription describes which cluster the new should clone and up to which point in time
type CloneDescription struct {
	// +required
	ClusterName string `json:"cluster,omitempty"`
	// +kubebuilder:validation:Format=uuid
	UID string `json:"uid,omitempty"`
	// The regexp matches the date-time format (RFC 3339 Section 5.6) that specifies a timezone as an offset relative to UTC
	// Example: 1996-12-19T16:39:57-08:00
	// Note: this field requires a timezone
	// +kubebuilder:validation:Pattern=`^([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?(([+-]([01][0-9]|2[0-3]):[0-5][0-9]))$`
	EndTimestamp      string `json:"timestamp,omitempty"`
	S3WalPath         string `json:"s3_wal_path,omitempty"`
	S3Endpoint        string `json:"s3_endpoint,omitempty"`
	S3AccessKeyId     string `json:"s3_access_key_id,omitempty"`
	S3SecretAccessKey string `json:"s3_secret_access_key,omitempty"`
	S3ForcePathStyle  *bool  `json:"s3_force_path_style,omitempty" defaults:"false"`
}

// Sidecar defines a container to be run in the same pod as the Postgres container.
type Sidecar struct {
	*Resources  `json:"resources,omitempty"`
	Name        string             `json:"name,omitempty"`
	DockerImage string             `json:"image,omitempty"`
	Ports       []v1.ContainerPort `json:"ports,omitempty"`
	Env         []v1.EnvVar        `json:"env,omitempty"`
	Command     []string           `json:"command,omitempty"`
}

// UserFlags defines flags (such as superuser, nologin) that could be assigned to individual users
// +kubebuilder:validation:items:Enum=bypassrls;BYPASSRLS;nobypassrls;NOBYPASSRLS;createdb;CREATEDB;nocreatedb;NOCREATEDB;createrole;CREATEROLE;nocreaterole;NOCREATEROLE;inherit;INHERIT;noinherit;NOINHERIT;login;LOGIN;nologin;NOLOGIN;replication;REPLICATION;noreplication;NOREPLICATION;superuser;SUPERUSER;nosuperuser;NOSUPERUSER
// +nullable
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
	// +kubebuilder:validation:Minimum=1
	NumberOfInstances *int32 `json:"numberOfInstances,omitempty"`
	Schema            string `json:"schema,omitempty"`
	User              string `json:"user,omitempty"`
	// +kubebuilder:validation:Enum=session;transaction
	Mode             string `json:"mode,omitempty"`
	DockerImage      string `json:"dockerImage,omitempty"`
	MaxDBConnections *int32 `json:"maxDBConnections,omitempty"`

	*Resources `json:"resources,omitempty"`
}

// Stream defines properties for creating FabricEventStream resources
type Stream struct {
	ApplicationId string                 `json:"applicationId"`
	Database      string                 `json:"database"`
	Tables        map[string]StreamTable `json:"tables"`
	Filter        map[string]*string     `json:"filter,omitempty"`
	BatchSize     *uint32                `json:"batchSize,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+m|\d+(\.\d{1,3})?)$`
	CPU *string `json:"cpu,omitempty"`
	// +kubebuilder:validation:Pattern=`^(\d+(e\d+)?|\d+(\.\d+)?(e\d+)?[EPTGMK]i?)$`
	Memory         *string `json:"memory,omitempty"`
	EnableRecovery *bool   `json:"enableRecovery,omitempty"`
}

// StreamTable defines properties of outbox tables for FabricEventStreams
type StreamTable struct {
	EventType         string  `json:"eventType"`
	RecoveryEventType string  `json:"recoveryEventType,omitempty"`
	IgnoreRecovery    *bool   `json:"ignoreRecovery,omitempty"`
	IdColumn          *string `json:"idColumn,omitempty"`
	PayloadColumn     *string `json:"payloadColumn,omitempty"`
}
