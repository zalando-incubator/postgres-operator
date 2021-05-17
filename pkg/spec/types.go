package spec

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// NamespacedName describes the namespace/name pairs used in Kubernetes names.
type NamespacedName types.NamespacedName

const fileWithNamespace = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// RoleOrigin contains the code of the origin of a role
type RoleOrigin int

// The rolesOrigin constant values must be sorted by the role priority for
// resolveNameConflict(...) to work.
const (
	RoleOriginUnknown RoleOrigin = iota
	RoleOriginManifest
	RoleOriginInfrastructure
	RoleOriginTeamsAPI
	RoleOriginSystem
	RoleOriginBootstrap
	RoleConnectionPooler
)

type syncUserOperation int

// Possible values for the sync user operation (removal of users is not supported yet)
const (
	PGSyncUserAdd = iota
	PGsyncUserAlter
	PGSyncAlterSet // handle ALTER ROLE SET parameter = value
)

// PgUser contains information about a single user.
type PgUser struct {
	Origin     RoleOrigin        `yaml:"-"`
	Name       string            `yaml:"-"`
	Password   string            `yaml:"-"`
	Flags      []string          `yaml:"user_flags"`
	MemberOf   []string          `yaml:"inrole"`
	Parameters map[string]string `yaml:"db_parameters"`
	AdminRole  string            `yaml:"admin_role"`
}

func (user *PgUser) Valid() bool {
	return user.Name != "" && user.Password != ""
}

// PgUserMap maps user names to the definitions.
type PgUserMap map[string]PgUser

// PgSyncUserRequest has information about a single request to sync a user.
type PgSyncUserRequest struct {
	Kind syncUserOperation
	User PgUser
}

// UserSyncer defines an interface for the implementations to sync users from the manifest to the DB.
type UserSyncer interface {
	ProduceSyncRequests(dbUsers PgUserMap, newUsers PgUserMap) (req []PgSyncUserRequest)
	ExecuteSyncRequests(req []PgSyncUserRequest, db *sql.DB) error
}

// LogEntry describes log entry in the RingLogger
type LogEntry struct {
	Time        time.Time
	Level       logrus.Level
	ClusterName *NamespacedName `json:",omitempty"`
	Worker      *uint32         `json:",omitempty"`
	Message     string
}

// Diff describes diff
type Diff struct {
	EventTime   time.Time
	ProcessTime time.Time
	Diff        []string
}

// ControllerStatus describes status of the controller
type ControllerStatus struct {
	LastSyncTime    int64
	Clusters        int
	WorkerQueueSize map[int]int
}

// QueueDump describes cache.FIFO queue
type QueueDump struct {
	Keys []string
	List []interface{}
}

// ControllerConfig describes configuration of the controller
type ControllerConfig struct {
	RestConfig          *rest.Config `json:"-"`
	InfrastructureRoles map[string]PgUser

	NoDatabaseAccess     bool
	NoTeamsAPI           bool
	CRDReadyWaitInterval time.Duration
	CRDReadyWaitTimeout  time.Duration
	ConfigMapName        NamespacedName
	Namespace            string
	IgnoredAnnotations   []string

	EnableJsonLogging bool
}

// cached value for the GetOperatorNamespace
var operatorNamespace string

func (n NamespacedName) String() string {
	return types.NamespacedName(n).String()
}

// MarshalJSON defines marshaling rule for the namespaced name type.
func (n NamespacedName) MarshalJSON() ([]byte, error) {
	return []byte("\"" + n.String() + "\""), nil
}

// Decode converts a (possibly unqualified) string into the namespaced name object.
func (n *NamespacedName) Decode(value string) error {
	return n.DecodeWorker(value, GetOperatorNamespace())
}

// UnmarshalJSON converts a byte slice to NamespacedName
func (n *NamespacedName) UnmarshalJSON(data []byte) error {
	result := NamespacedName{}
	var tmp string
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	if err := result.Decode(tmp); err != nil {
		return err
	}
	*n = result
	return nil
}

// DecodeWorker separates the decode logic to (unit) test
// from obtaining the operator namespace that depends on k8s mounting files at runtime
func (n *NamespacedName) DecodeWorker(value, operatorNamespace string) error {
	var (
		name types.NamespacedName
	)

	result := strings.SplitN(value, string(types.Separator), 2)
	if len(result) < 2 {
		name.Name = result[0]
	} else {
		name.Name = strings.TrimLeft(result[1], string(types.Separator))
		name.Namespace = result[0]
	}
	if name.Name == "" {
		return fmt.Errorf("incorrect namespaced name: %v", value)
	}
	if name.Namespace == "" {
		name.Namespace = operatorNamespace
	}

	*n = NamespacedName(name)

	return nil
}

func (r RoleOrigin) String() string {
	switch r {
	case RoleOriginUnknown:
		return "unknown"
	case RoleOriginManifest:
		return "manifest role"
	case RoleOriginInfrastructure:
		return "infrastructure role"
	case RoleOriginTeamsAPI:
		return "teams API role"
	case RoleOriginSystem:
		return "system role"
	case RoleOriginBootstrap:
		return "bootstrapped role"
	case RoleConnectionPooler:
		return "connection pooler role"
	default:
		panic(fmt.Sprintf("bogus role origin value %d", r))
	}
}

// GetOperatorNamespace assumes serviceaccount secret is mounted by kubernetes
// Placing this func here instead of pgk/util avoids circular import
func GetOperatorNamespace() string {
	if operatorNamespace == "" {
		if namespaceFromEnvironment := os.Getenv("OPERATOR_NAMESPACE"); namespaceFromEnvironment != "" {
			return namespaceFromEnvironment
		}
		operatorNamespaceBytes, err := ioutil.ReadFile(fileWithNamespace)
		if err != nil {
			log.Fatalf("Unable to detect operator namespace from within its pod due to: %v", err)
		}
		operatorNamespace = string(operatorNamespaceBytes)
	}
	return operatorNamespace
}
