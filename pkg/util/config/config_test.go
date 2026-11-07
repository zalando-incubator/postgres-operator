package config

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	// Set OPERATOR_NAMESPACE to avoid log.Fatal in GetOperatorNamespace
	// when running tests outside a Kubernetes pod
	os.Setenv("OPERATOR_NAMESPACE", "default")
	os.Exit(m.Run())
}

var getMapPairsFromStringTest = []struct {
	in       string
	expected []string
	err      error
}{
	{"log_statement:all, work_mem:'4GB'", []string{"log_statement:all", "work_mem:'4GB'"}, nil},
	{`log_statement:none, search_path:'"$user", public'`, []string{"log_statement:none", `search_path:'"$user", public'`}, nil},
	{`search_path:'"$user"`, nil, fmt.Errorf("unmatched quote starting at position 13")},
	{"", []string{""}, nil},
	{",,log_statement:all	,", []string{"", "", "log_statement:all", ""}, nil},
}

func TestGetMapPairsFromString(t *testing.T) {
	for _, tt := range getMapPairsFromStringTest {
		got, err := getMapPairsFromString(tt.in)
		if err != tt.err && ((err == nil || tt.err == nil) || (err.Error() != tt.err.Error())) {
			t.Errorf("TestGetMapPairsFromString with %s: expected error: %#v, got %#v", tt.in, tt.err, err)
		}
		if !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("TestGetMapPairsFromString with %s: expected %#v, got %#v", tt.in, tt.expected, got)
		}
	}
}

func int32Ptr(i int32) *int32 {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}

var validateTests = []struct {
	description string
	cfg         Config
	expectError bool
	errorMsg    string
}{
	{
		description: "valid config",
		cfg: Config{
			Resources: Resources{
				MinInstances: 1,
				MaxInstances: 5,
			},
			Auth: Auth{
				SuperUsername: "postgres",
			},
			ConnectionPooler: ConnectionPooler{
				NumberOfInstances: int32Ptr(2),
				User:              "pooler",
			},
			Workers: 4,
		},
		expectError: false,
	},
	{
		description: "min instances greater than max instances",
		cfg: Config{
			Resources: Resources{
				MinInstances: 10,
				MaxInstances: 5,
			},
			Auth: Auth{
				SuperUsername: "postgres",
			},
			ConnectionPooler: ConnectionPooler{
				NumberOfInstances: int32Ptr(2),
				User:              "pooler",
			},
			Workers: 4,
		},
		expectError: true,
		errorMsg:    "minimum number of instances",
	},
	{
		description: "workers set to zero",
		cfg: Config{
			Resources: Resources{
				MinInstances: 1,
				MaxInstances: 5,
			},
			Auth: Auth{
				SuperUsername: "postgres",
			},
			ConnectionPooler: ConnectionPooler{
				NumberOfInstances: int32Ptr(2),
				User:              "pooler",
			},
			Workers: 0,
		},
		expectError: true,
		errorMsg:    "number of workers should be higher than 0",
	},
	{
		description: "connection pooler instances below minimum",
		cfg: Config{
			Resources: Resources{
				MinInstances: 1,
				MaxInstances: 5,
			},
			Auth: Auth{
				SuperUsername: "postgres",
			},
			ConnectionPooler: ConnectionPooler{
				NumberOfInstances: int32Ptr(0),
				User:              "pooler",
			},
			Workers: 4,
		},
		expectError: true,
		errorMsg:    "number of connection pooler instances",
	},
	{
		description: "connection pooler user same as super user",
		cfg: Config{
			Resources: Resources{
				MinInstances: 1,
				MaxInstances: 5,
			},
			Auth: Auth{
				SuperUsername: "postgres",
			},
			ConnectionPooler: ConnectionPooler{
				NumberOfInstances: int32Ptr(2),
				User:              "postgres",
			},
			Workers: 4,
		},
		expectError: true,
		errorMsg:    "connection pool user is not allowed to be the same as super user",
	},
	{
		description: "min and max instances both negative (disabled)",
		cfg: Config{
			Resources: Resources{
				MinInstances: -1,
				MaxInstances: -1,
			},
			Auth: Auth{
				SuperUsername: "postgres",
			},
			ConnectionPooler: ConnectionPooler{
				NumberOfInstances: int32Ptr(2),
				User:              "pooler",
			},
			Workers: 4,
		},
		expectError: false,
	},
}

func TestValidate(t *testing.T) {
	for _, tt := range validateTests {
		t.Run(tt.description, func(t *testing.T) {
			err := validate(&tt.cfg)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}
		})
	}
}

var newFromMapTests = []struct {
	description  string
	input        map[string]string
	expectPanic  bool
	panicMsg     string
	validateFunc func(t *testing.T, cfg *Config)
}{
	{
		description: "empty map uses defaults",
		input:       map[string]string{},
		expectPanic: false,
		validateFunc: func(t *testing.T, cfg *Config) {
			if cfg.Workers != 8 {
				t.Errorf("expected default Workers=8, got %d", cfg.Workers)
			}
			if cfg.SuperUsername != "postgres" {
				t.Errorf("expected default SuperUsername=postgres, got %s", cfg.SuperUsername)
			}
			if cfg.ReplicationUsername != "standby" {
				t.Errorf("expected default ReplicationUsername=standby, got %s", cfg.ReplicationUsername)
			}
		},
	},
	{
		description: "custom values override defaults",
		input: map[string]string{
			"workers":        "16",
			"super_username": "admin",
		},
		expectPanic: false,
		validateFunc: func(t *testing.T, cfg *Config) {
			if cfg.Workers != 16 {
				t.Errorf("expected Workers=16, got %d", cfg.Workers)
			}
			if cfg.SuperUsername != "admin" {
				t.Errorf("expected SuperUsername=admin, got %s", cfg.SuperUsername)
			}
		},
	},
	{
		description: "duration parsing",
		input: map[string]string{
			"patroni_api_check_interval": "1s",
			"patroni_api_check_timeout":  "5s",
		},
		expectPanic: false,
		validateFunc: func(t *testing.T, cfg *Config) {
			if cfg.PatroniAPICheckInterval.Seconds() != 1 {
				t.Errorf("expected check interval of 1s, got %.0fs", cfg.PatroniAPICheckInterval.Seconds())
			}
			if cfg.PatroniAPICheckTimeout.Seconds() != 5 {
				t.Errorf("expected check timeout of 5s, got %.0fs", cfg.PatroniAPICheckTimeout.Seconds())
			}
		},
	},
	{
		description: "boolean parsing",
		input: map[string]string{
			"enable_teams_api": "false",
			"debug_logging":    "false",
		},
		expectPanic: false,
		validateFunc: func(t *testing.T, cfg *Config) {
			if cfg.EnableTeamsAPI != false {
				t.Errorf("expected EnableTeamsAPI=false, got %v", cfg.EnableTeamsAPI)
			}
			if cfg.DebugLogging != false {
				t.Errorf("expected DebugLogging=false, got %v", cfg.DebugLogging)
			}
		},
	},
	{
		description: "map parsing",
		input: map[string]string{
			"cluster_labels": "app:myapp,env:prod",
		},
		expectPanic: false,
		validateFunc: func(t *testing.T, cfg *Config) {
			if cfg.ClusterLabels["app"] != "myapp" {
				t.Errorf("expected ClusterLabels[app]=myapp, got %s", cfg.ClusterLabels["app"])
			}
			if cfg.ClusterLabels["env"] != "prod" {
				t.Errorf("expected ClusterLabels[env]=prod, got %s", cfg.ClusterLabels["env"])
			}
		},
	},
	{
		description: "slice parsing",
		input: map[string]string{
			"inherited_labels": "label1,label2,label3",
		},
		expectPanic: false,
		validateFunc: func(t *testing.T, cfg *Config) {
			expected := []string{"label1", "label2", "label3"}
			if !reflect.DeepEqual(cfg.InheritedLabels, expected) {
				t.Errorf("expected InheritedLabels=%v, got %v", expected, cfg.InheritedLabels)
			}
		},
	},
	{
		description: "invalid workers triggers validation panic",
		input: map[string]string{
			"workers": "0",
		},
		expectPanic: true,
		panicMsg:    "number of workers should be higher than 0",
	},
	{
		description: "invalid integer causes panic",
		input: map[string]string{
			"workers": "invalid",
		},
		expectPanic: true,
		panicMsg:    "invalid syntax",
	},
}

func TestNewFromMap(t *testing.T) {
	for _, tt := range newFromMapTests {
		t.Run(tt.description, func(t *testing.T) {
			if tt.expectPanic {
				defer func() {
					r := recover()
					if r == nil {
						t.Errorf("expected panic with message containing %q, but no panic occurred", tt.panicMsg)
						return
					}
					errMsg := fmt.Sprintf("%v", r)
					if !strings.Contains(errMsg, tt.panicMsg) {
						t.Errorf("expected panic message containing %q, got %q", tt.panicMsg, errMsg)
					}
				}()
			}

			cfg := NewFromMap(tt.input)

			if tt.expectPanic {
				t.Errorf("expected panic but NewFromMap returned successfully")
				return
			}

			if tt.validateFunc != nil {
				tt.validateFunc(t, cfg)
			}
		})
	}
}

func TestSecurityContextOptionsFromMap(t *testing.T) {
	cfg := NewFromMap(map[string]string{})
	if cfg.SpiloRunAsNonRoot != nil || cfg.SpiloSeccompProfile != "" || len(cfg.DropPodCapabilities) != 0 {
		t.Errorf("expected security context options to be unset by default, got %v %q %v",
			cfg.SpiloRunAsNonRoot, cfg.SpiloSeccompProfile, cfg.DropPodCapabilities)
	}

	cfg = NewFromMap(map[string]string{
		"spilo_runasnonroot":    "true",
		"spilo_seccomp_profile": "Localhost/profiles/postgres.json",
		"drop_pod_capabilities": "ALL",
	})
	if cfg.SpiloRunAsNonRoot == nil || !*cfg.SpiloRunAsNonRoot {
		t.Errorf("expected spilo_runasnonroot to be true")
	}
	if cfg.SpiloSeccompProfile != "Localhost/profiles/postgres.json" {
		t.Errorf("unexpected spilo_seccomp_profile %q", cfg.SpiloSeccompProfile)
	}
	if !reflect.DeepEqual(cfg.DropPodCapabilities, []string{"ALL"}) {
		t.Errorf("unexpected drop_pod_capabilities %v", cfg.DropPodCapabilities)
	}

	for _, profile := range []string{"RuntimeDefault", "Unconfined", "Localhost/x"} {
		NewFromMap(map[string]string{"spilo_seccomp_profile": profile})
	}

	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "spilo_seccomp_profile") {
			t.Errorf("expected panic for invalid spilo_seccomp_profile, got %v", r)
		}
	}()
	NewFromMap(map[string]string{"spilo_seccomp_profile": "runtime/default"})
}
