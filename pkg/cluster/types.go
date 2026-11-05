package cluster

import (
	"time"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PostgresRole describes role of the node
type PostgresRole string

const (
	// spilo roles
	Master  PostgresRole = "master"
	Replica PostgresRole = "replica"
	Patroni PostgresRole = "config"

	// roles returned by Patroni cluster endpoint
	Leader        PostgresRole = "leader"
	StandbyLeader PostgresRole = "standby_leader"
	SyncStandby   PostgresRole = "sync_standby"
)

// PodEventType represents the type of a pod-related event
type PodEventType string

// Possible values for the EventType
const (
	PodEventAdd    PodEventType = "ADD"
	PodEventUpdate PodEventType = "UPDATE"
	PodEventDelete PodEventType = "DELETE"
)

// podSubscriberBufferSize is the number of pod events buffered for a subscriber waiting on a pod.
// A pod emits only a handful of events while it is being recreated.
const podSubscriberBufferSize = 100

// PodEvent describes the event for a single Pod
type PodEvent struct {
	ResourceVersion string
	PodName         types.NamespacedName
	PrevPod         *v1.Pod
	CurPod          *v1.Pod
	EventType       PodEventType
}

// Process describes process of the cluster
type Process struct {
	Name      string
	StartTime time.Time
}

// WorkerStatus describes status of the worker
type WorkerStatus struct {
	CurrentCluster types.NamespacedName
	CurrentProcess Process
}

// ClusterStatus describes status of the cluster
type ClusterStatus struct {
	Team                          string
	Cluster                       string
	Namespace                     string
	MasterService                 *v1.Service
	ReplicaService                *v1.Service
	MasterEndpoint                *v1.Endpoints
	ReplicaEndpoint               *v1.Endpoints
	StatefulSet                   *appsv1.StatefulSet
	PrimaryPodDisruptionBudget    *policyv1.PodDisruptionBudget
	CriticalOpPodDisruptionBudget *policyv1.PodDisruptionBudget

	CurrentProcess Process
	Worker         uint32
	Status         acidv1.PostgresStatus
	Spec           acidv1.PostgresSpec
	Error          error
}

type TemplateParams map[string]interface{}

type InstallFunction func(schema string, user string) error

type SyncReason []string

// PasswordEncryption is the password hashing method used by Postgres and the pooler
type PasswordEncryption string

const (
	PasswordEncryptionMD5         PasswordEncryption = "md5"
	PasswordEncryptionScramSHA256 PasswordEncryption = "scram-sha-256"
)

// passwordEncryptionFromSpec returns the password_encryption parameter, falling back to scram-sha-256 for unset or unsupported values
func passwordEncryptionFromSpec(spec *acidv1.PostgresSpec) PasswordEncryption {
	switch pe := PasswordEncryption(spec.PostgresqlParam.Parameters["password_encryption"]); pe {
	case PasswordEncryptionMD5, PasswordEncryptionScramSHA256:
		return pe
	default:
		return PasswordEncryptionScramSHA256
	}
}

// no sync happened, empty value
var NoSync SyncReason = []string{}
