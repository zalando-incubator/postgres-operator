package cluster

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

func newFakeK8sTestClient() (k8sutil.KubernetesClient, *fake.Clientset) {
	acidClientSet := fakeacidv1.NewSimpleClientset()
	clientSet := fake.NewSimpleClientset()

	return k8sutil.KubernetesClient{
		PodsGetter:         clientSet.CoreV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		StatefulSetsGetter: clientSet.AppsV1(),
	}, clientSet
}

// For testing purposes
type ExpectedValue struct {
	envIndex       int
	envVarConstant string
	envVarValue    string
	envVarValueRef *v1.EnvVarSource
}

func TestGenerateSpiloJSONConfiguration(t *testing.T) {
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	tests := []struct {
		subtest  string
		pgParam  *acidv1.PostgresqlParam
		patroni  *acidv1.Patroni
		opConfig *config.Config
		result   string
	}{
		{
			subtest: "Patroni default configuration",
			pgParam: &acidv1.PostgresqlParam{PgVersion: "18"},
			patroni: &acidv1.Patroni{},
			opConfig: &config.Config{
				Auth: config.Auth{
					PamRoleName: "zalandos",
				},
			},
			result: `{"postgresql":{"bin_dir":"/usr/lib/postgresql/18/bin"},"bootstrap":{"initdb":[{"auth-host":"scram-sha-256"},{"auth-local":"trust"}],"dcs":{}}}`,
		},
		{
			subtest: "Patroni configured",
			pgParam: &acidv1.PostgresqlParam{PgVersion: "18"},
			patroni: &acidv1.Patroni{
				InitDB: map[string]string{
					"encoding":       "UTF8",
					"locale":         "en_US.UTF-8",
					"data-checksums": "true",
				},
				PgHba:                 []string{"hostssl all all 0.0.0.0/0 scram-sha-256", "host    all all 0.0.0.0/0 scram-sha-256"},
				TTL:                   30,
				LoopWait:              10,
				RetryTimeout:          10,
				MaximumLagOnFailover:  33554432,
				SynchronousMode:       true,
				SynchronousModeStrict: true,
				SynchronousNodeCount:  1,
				Slots:                 map[string]map[string]string{"permanent_logical_1": {"type": "logical", "database": "foo", "plugin": "pgoutput"}},
				FailsafeMode:          util.True(),
			},
			opConfig: &config.Config{},
			result:   `{"postgresql":{"bin_dir":"/usr/lib/postgresql/18/bin","pg_hba":["hostssl all all 0.0.0.0/0 scram-sha-256","host    all all 0.0.0.0/0 scram-sha-256"]},"bootstrap":{"initdb":[{"auth-host":"scram-sha-256"},{"auth-local":"trust"},"data-checksums",{"encoding":"UTF8"},{"locale":"en_US.UTF-8"}],"dcs":{"ttl":30,"loop_wait":10,"retry_timeout":10,"maximum_lag_on_failover":33554432,"synchronous_mode":true,"synchronous_mode_strict":true,"synchronous_node_count":1,"slots":{"permanent_logical_1":{"database":"foo","plugin":"pgoutput","type":"logical"}},"failsafe_mode":true}}}`,
		},
		{
			subtest: "Patroni failsafe_mode configured globally",
			pgParam: &acidv1.PostgresqlParam{PgVersion: "18"},
			patroni: &acidv1.Patroni{},
			opConfig: &config.Config{
				EnablePatroniFailsafeMode: util.True(),
			},
			result: `{"postgresql":{"bin_dir":"/usr/lib/postgresql/18/bin"},"bootstrap":{"initdb":[{"auth-host":"scram-sha-256"},{"auth-local":"trust"}],"dcs":{"failsafe_mode":true}}}`,
		},
		{
			subtest: "Patroni failsafe_mode configured globally, disabled for cluster",
			pgParam: &acidv1.PostgresqlParam{PgVersion: "18"},
			patroni: &acidv1.Patroni{
				FailsafeMode: util.False(),
			},
			opConfig: &config.Config{
				EnablePatroniFailsafeMode: util.True(),
			},
			result: `{"postgresql":{"bin_dir":"/usr/lib/postgresql/18/bin"},"bootstrap":{"initdb":[{"auth-host":"scram-sha-256"},{"auth-local":"trust"}],"dcs":{"failsafe_mode":false}}}`,
		},
		{
			subtest: "Patroni failsafe_mode disabled globally, configured for cluster",
			pgParam: &acidv1.PostgresqlParam{PgVersion: "18"},
			patroni: &acidv1.Patroni{
				FailsafeMode: util.True(),
			},
			opConfig: &config.Config{
				EnablePatroniFailsafeMode: util.False(),
			},
			result: `{"postgresql":{"bin_dir":"/usr/lib/postgresql/18/bin"},"bootstrap":{"initdb":[{"auth-host":"scram-sha-256"},{"auth-local":"trust"}],"dcs":{"failsafe_mode":true}}}`,
		},
	}
	for _, tt := range tests {
		cluster.OpConfig = *tt.opConfig
		result, err := generateSpiloJSONConfiguration(tt.pgParam, tt.patroni, tt.opConfig, logger)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if tt.result != result {
			t.Errorf("%s %s: Spilo Config is %v, expected %v and param %#v",
				t.Name(), tt.subtest, result, tt.result, tt.pgParam)
		}
	}
}

func TestExtractPgVersionFromBinPath(t *testing.T) {
	tests := []struct {
		subTest  string
		binPath  string
		template string
		expected string
	}{
		{
			subTest:  "test current bin path with decimal against hard coded template",
			binPath:  "/usr/lib/postgresql/9.6/bin",
			template: pgBinariesLocationTemplate,
			expected: "9.6",
		},
		{
			subTest:  "test current bin path against hard coded template",
			binPath:  "/usr/lib/postgresql/18/bin",
			template: pgBinariesLocationTemplate,
			expected: "18",
		},
		{
			subTest:  "test alternative bin path against a matching template",
			binPath:  "/usr/pgsql-18/bin",
			template: "/usr/pgsql-%v/bin",
			expected: "18",
		},
	}

	for _, tt := range tests {
		pgVersion, err := extractPgVersionFromBinPath(tt.binPath, tt.template)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if pgVersion != tt.expected {
			t.Errorf("%s %s: Expected version %s, have %s instead",
				t.Name(), tt.subTest, tt.expected, pgVersion)
		}
	}
}

const (
	testPodEnvironmentConfigMapName      = "pod_env_cm"
	testPodEnvironmentSecretName         = "pod_env_sc"
	testCronjobEnvironmentSecretName     = "pod_env_sc"
	testPodEnvironmentObjectNotExists    = "idonotexist"
	testPodEnvironmentSecretNameAPIError = "pod_env_sc_apierror"
	testResourceCheckInterval            = 3
	testResourceCheckTimeout             = 10
)

type mockSecret struct {
	v1core.SecretInterface
}

type mockConfigMap struct {
	v1core.ConfigMapInterface
}

func (c *mockSecret) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.Secret, error) {
	if name == testPodEnvironmentSecretNameAPIError {
		return nil, fmt.Errorf("Secret PodEnvironmentSecret API error")
	}
	if name != testPodEnvironmentSecretName {
		return nil, k8serrors.NewNotFound(schema.GroupResource{Group: "core", Resource: "secret"}, name)
	}
	secret := &v1.Secret{}
	secret.Name = testPodEnvironmentSecretName
	secret.Data = map[string][]byte{
		"clone_aws_access_key_id":                []byte("0123456789abcdef0123456789abcdef"),
		"custom_variable":                        []byte("secret-test"),
		"standby_google_application_credentials": []byte("0123456789abcdef0123456789abcdef"),
	}
	return secret, nil
}

func (c *mockConfigMap) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.ConfigMap, error) {
	if name != testPodEnvironmentConfigMapName {
		return nil, fmt.Errorf("NotFound")
	}
	configmap := &v1.ConfigMap{}
	configmap.Name = testPodEnvironmentConfigMapName
	configmap.Data = map[string]string{
		// hard-coded clone env variable, can set when not specified in manifest
		"clone_aws_endpoint": "s3.eu-west-1.amazonaws.com",
		// custom variable, can be overridden by c.Spec.Env
		"custom_variable": "configmap-test",
		// hard-coded env variable, can not be overridden
		"kubernetes_scope_label": "pgaas",
		// hard-coded env variable, can be overridden
		"wal_s3_bucket": "global-s3-bucket-configmap",
	}
	return configmap, nil
}

type MockSecretGetter struct {
}

type MockConfigMapsGetter struct {
}

func (c *MockSecretGetter) Secrets(namespace string) v1core.SecretInterface {
	return &mockSecret{}
}

func (c *MockConfigMapsGetter) ConfigMaps(namespace string) v1core.ConfigMapInterface {
	return &mockConfigMap{}
}

func newMockKubernetesClient() k8sutil.KubernetesClient {
	return k8sutil.KubernetesClient{
		SecretsGetter:    &MockSecretGetter{},
		ConfigMapsGetter: &MockConfigMapsGetter{},
	}
}
func newMockCluster(opConfig config.Config) *Cluster {
	cluster := &Cluster{
		Config:     Config{OpConfig: opConfig},
		KubeClient: newMockKubernetesClient(),
		logger:     logger,
	}
	return cluster
}

func TestPodEnvironmentConfigMapVariables(t *testing.T) {
	tests := []struct {
		subTest  string
		opConfig config.Config
		envVars  []v1.EnvVar
		err      error
	}{
		{
			subTest: "no PodEnvironmentConfigMap",
			envVars: []v1.EnvVar{},
		},
		{
			subTest: "missing PodEnvironmentConfigMap",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentObjectNotExists,
					},
				},
			},
			err: fmt.Errorf("could not read PodEnvironmentConfigMap: NotFound"),
		},
		{
			subTest: "Pod environment vars configured by PodEnvironmentConfigMap",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
			},
			envVars: []v1.EnvVar{
				{
					Name:  "clone_aws_endpoint",
					Value: "s3.eu-west-1.amazonaws.com",
				},
				{
					Name:  "custom_variable",
					Value: "configmap-test",
				},
				{
					Name:  "kubernetes_scope_label",
					Value: "pgaas",
				},
				{
					Name:  "wal_s3_bucket",
					Value: "global-s3-bucket-configmap",
				},
			},
		},
	}
	for _, tt := range tests {
		c := newMockCluster(tt.opConfig)
		vars, err := c.getPodEnvironmentConfigMapVariables()
		if !reflect.DeepEqual(vars, tt.envVars) {
			t.Errorf("%s %s: expected `%v` but got `%v`",
				t.Name(), tt.subTest, tt.envVars, vars)
		}
		if tt.err != nil {
			if err.Error() != tt.err.Error() {
				t.Errorf("%s %s: expected error `%v` but got `%v`",
					t.Name(), tt.subTest, tt.err, err)
			}
		} else {
			if err != nil {
				t.Errorf("%s %s: expected no error but got error: `%v`",
					t.Name(), tt.subTest, err)
			}
		}
	}
}

// Test if the keys of an existing secret are properly referenced
func TestPodEnvironmentSecretVariables(t *testing.T) {
	maxRetries := int(testResourceCheckTimeout / testResourceCheckInterval)
	tests := []struct {
		subTest  string
		opConfig config.Config
		envVars  []v1.EnvVar
		err      error
	}{
		{
			subTest: "No PodEnvironmentSecret configured",
			envVars: []v1.EnvVar{},
		},
		{
			subTest: "Secret referenced by PodEnvironmentSecret does not exist",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentSecret:  testPodEnvironmentObjectNotExists,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
			},
			err: fmt.Errorf("could not read Secret PodEnvironmentSecretName: still failing after %d retries: secret.core %q not found", maxRetries, testPodEnvironmentObjectNotExists),
		},
		{
			subTest: "API error during PodEnvironmentSecret retrieval",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentSecret:  testPodEnvironmentSecretNameAPIError,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
			},
			err: fmt.Errorf("could not read Secret PodEnvironmentSecretName: Secret PodEnvironmentSecret API error"),
		},
		{
			subTest: "Pod environment vars reference all keys from secret configured by PodEnvironmentSecret",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentSecret:  testPodEnvironmentSecretName,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
			},
			envVars: []v1.EnvVar{
				{
					Name: "clone_aws_access_key_id",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "clone_aws_access_key_id",
						},
					},
				},
				{
					Name: "custom_variable",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "custom_variable",
						},
					},
				},
				{
					Name: "standby_google_application_credentials",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "standby_google_application_credentials",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		c := newMockCluster(tt.opConfig)
		vars, err := c.getPodEnvironmentSecretVariables()
		sort.Slice(vars, func(i, j int) bool { return vars[i].Name < vars[j].Name })
		if !reflect.DeepEqual(vars, tt.envVars) {
			t.Errorf("%s %s: expected `%v` but got `%v`",
				t.Name(), tt.subTest, tt.envVars, vars)
		}
		if tt.err != nil {
			if err.Error() != tt.err.Error() {
				t.Errorf("%s %s: expected error `%v` but got `%v`",
					t.Name(), tt.subTest, tt.err, err)
			}
		} else {
			if err != nil {
				t.Errorf("%s %s: expected no error but got error: `%v`",
					t.Name(), tt.subTest, err)
			}
		}
	}

}

// Test if the keys of an existing secret are properly referenced
func TestCronjobEnvironmentSecretVariables(t *testing.T) {
	testName := "TestCronjobEnvironmentSecretVariables"
	tests := []struct {
		subTest  string
		opConfig config.Config
		envVars  []v1.EnvVar
		err      error
	}{
		{
			subTest: "No CronjobEnvironmentSecret configured",
			envVars: []v1.EnvVar{},
		},
		{
			subTest: "Secret referenced by CronjobEnvironmentSecret does not exist",
			opConfig: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupCronjobEnvironmentSecret: "idonotexist",
				},
			},
			err: fmt.Errorf("could not read Secret CronjobEnvironmentSecretName: secret.core \"idonotexist\" not found"),
		},
		{
			subTest: "Cronjob environment vars reference all keys from secret configured by CronjobEnvironmentSecret",
			opConfig: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupCronjobEnvironmentSecret: testCronjobEnvironmentSecretName,
				},
			},
			envVars: []v1.EnvVar{
				{
					Name: "clone_aws_access_key_id",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "clone_aws_access_key_id",
						},
					},
				},
				{
					Name: "custom_variable",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "custom_variable",
						},
					},
				},
				{
					Name: "standby_google_application_credentials",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "standby_google_application_credentials",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		c := newMockCluster(tt.opConfig)
		vars, err := c.getCronjobEnvironmentSecretVariables()
		sort.Slice(vars, func(i, j int) bool { return vars[i].Name < vars[j].Name })
		if !reflect.DeepEqual(vars, tt.envVars) {
			t.Errorf("%s %s: expected `%v` but got `%v`",
				testName, tt.subTest, tt.envVars, vars)
		}
		if tt.err != nil {
			if err.Error() != tt.err.Error() {
				t.Errorf("%s %s: expected error `%v` but got `%v`",
					testName, tt.subTest, tt.err, err)
			}
		} else {
			if err != nil {
				t.Errorf("%s %s: expected no error but got error: `%v`",
					testName, tt.subTest, err)
			}
		}
	}

}

func testEnvs(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole) error {
	required := map[string]bool{
		"PGHOST":                      false,
		"PGPORT":                      false,
		"PGUSER":                      false,
		"PGSCHEMA":                    false,
		"PGPASSWORD":                  false,
		"CONNECTION_POOLER_MODE":      false,
		"CONNECTION_POOLER_PORT":      false,
		"CONNECTION_POOLER_AUTH_TYPE": false,
	}

	container := getPostgresContainer(&podSpec.Spec)
	envs := container.Env
	for _, env := range envs {
		required[env.Name] = true
	}

	for env, value := range required {
		if !value {
			return fmt.Errorf("Environment variable %s is not present", env)
		}
	}

	return nil
}

func TestGenerateSpiloPodEnvVars(t *testing.T) {
	var dummyUUID = "efd12e58-5786-11e8-b5a7-06148230260c"

	expectedClusterNameLabel := []ExpectedValue{
		{
			envIndex:       5,
			envVarConstant: "KUBERNETES_SCOPE_LABEL",
			envVarValue:    "cluster-name",
		},
	}
	expectedSpiloWalPathCompat := []ExpectedValue{
		{
			envIndex:       12,
			envVarConstant: "ENABLE_WAL_PATH_COMPAT",
			envVarValue:    "true",
		},
	}
	expectedValuesS3Bucket := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "WAL_S3_BUCKET",
			envVarValue:    "global-s3-bucket",
		},
		{
			envIndex:       17,
			envVarConstant: "WAL_BUCKET_SCOPE_SUFFIX",
			envVarValue:    fmt.Sprintf("/%s", dummyUUID),
		},
		{
			envIndex:       18,
			envVarConstant: "WAL_BUCKET_SCOPE_PREFIX",
			envVarValue:    "",
		},
	}
	expectedValuesGCPCreds := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "WAL_GS_BUCKET",
			envVarValue:    "global-gs-bucket",
		},
		{
			envIndex:       17,
			envVarConstant: "WAL_BUCKET_SCOPE_SUFFIX",
			envVarValue:    fmt.Sprintf("/%s", dummyUUID),
		},
		{
			envIndex:       18,
			envVarConstant: "WAL_BUCKET_SCOPE_PREFIX",
			envVarValue:    "",
		},
		{
			envIndex:       19,
			envVarConstant: "GOOGLE_APPLICATION_CREDENTIALS",
			envVarValue:    "some-path-to-credentials",
		},
	}
	expectedS3BucketConfigMap := []ExpectedValue{
		{
			envIndex:       18,
			envVarConstant: "wal_s3_bucket",
			envVarValue:    "global-s3-bucket-configmap",
		},
	}
	expectedCustomS3BucketSpec := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "WAL_S3_BUCKET",
			envVarValue:    "custom-s3-bucket",
		},
	}
	expectedCustomVariableSecret := []ExpectedValue{
		{
			envIndex:       17,
			envVarConstant: "custom_variable",
			envVarValueRef: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: testPodEnvironmentSecretName,
					},
					Key: "custom_variable",
				},
			},
		},
	}
	expectedCustomVariableConfigMap := []ExpectedValue{
		{
			envIndex:       17,
			envVarConstant: "custom_variable",
			envVarValue:    "configmap-test",
		},
	}
	expectedCustomVariableSpec := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "CUSTOM_VARIABLE",
			envVarValue:    "spec-env-test",
		},
	}
	expectedCloneEnvSpec := []ExpectedValue{
		{
			envIndex:       17,
			envVarConstant: "CLONE_WALE_S3_PREFIX",
			envVarValue:    "s3://another-bucket",
		},
		{
			envIndex:       20,
			envVarConstant: "CLONE_WAL_BUCKET_SCOPE_PREFIX",
			envVarValue:    "",
		},
		{
			envIndex:       21,
			envVarConstant: "CLONE_AWS_ENDPOINT",
			envVarValue:    "s3.eu-central-1.amazonaws.com",
		},
	}
	expectedCloneEnvSpecEnv := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "CLONE_WAL_BUCKET_SCOPE_PREFIX",
			envVarValue:    "test-cluster",
		},
		{
			envIndex:       18,
			envVarConstant: "CLONE_WALE_S3_PREFIX",
			envVarValue:    "s3://another-bucket",
		},
		{
			envIndex:       22,
			envVarConstant: "CLONE_AWS_ENDPOINT",
			envVarValue:    "s3.eu-central-1.amazonaws.com",
		},
	}
	expectedCloneEnvConfigMap := []ExpectedValue{
		{
			envIndex:       17,
			envVarConstant: "CLONE_WAL_S3_BUCKET",
			envVarValue:    "global-s3-bucket",
		},
		{
			envIndex:       18,
			envVarConstant: "CLONE_WAL_BUCKET_SCOPE_SUFFIX",
			envVarValue:    fmt.Sprintf("/%s", dummyUUID),
		},
		{
			envIndex:       22,
			envVarConstant: "clone_aws_endpoint",
			envVarValue:    "s3.eu-west-1.amazonaws.com",
		},
	}
	expectedCloneEnvSecret := []ExpectedValue{
		{
			envIndex:       22,
			envVarConstant: "clone_aws_access_key_id",
			envVarValueRef: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: testPodEnvironmentSecretName,
					},
					Key: "clone_aws_access_key_id",
				},
			},
		},
	}
	expectedStandbyEnvSecret := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "STANDBY_WALE_GS_PREFIX",
			envVarValue:    "gs://some/path/",
		},
		{
			envIndex:       21,
			envVarConstant: "standby_google_application_credentials",
			envVarValueRef: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: testPodEnvironmentSecretName,
					},
					Key: "standby_google_application_credentials",
				},
			},
		},
	}

	tests := []struct {
		subTest            string
		opConfig           config.Config
		cloneDescription   *acidv1.CloneDescription
		standbyDescription *acidv1.StandbyDescription
		expectedValues     []ExpectedValue
		pgsql              acidv1.Postgresql
	}{
		{
			subTest: "will set ENABLE_WAL_PATH_COMPAT env",
			opConfig: config.Config{
				EnableSpiloWalPathCompat: true,
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedSpiloWalPathCompat,
		},
		{
			subTest: "will set WAL_S3_BUCKET env",
			opConfig: config.Config{
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedValuesS3Bucket,
		},
		{
			subTest: "will set GOOGLE_APPLICATION_CREDENTIALS env",
			opConfig: config.Config{
				WALGSBucket:    "global-gs-bucket",
				GCPCredentials: "some-path-to-credentials",
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedValuesGCPCreds,
		},
		{
			subTest: "will not override global config KUBERNETES_SCOPE_LABEL parameter",
			opConfig: config.Config{
				Resources: config.Resources{
					ClusterNameLabel: "cluster-name",
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName, // contains kubernetes_scope_label, too
					},
				},
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedClusterNameLabel,
			pgsql: acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					Env: []v1.EnvVar{
						{
							Name:  "KUBERNETES_SCOPE_LABEL",
							Value: "my-scope-label",
						},
					},
				},
			},
		},
		{
			subTest: "will override global WAL_S3_BUCKET parameter from pod environment config map",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedS3BucketConfigMap,
		},
		{
			subTest: "will override global WAL_S3_BUCKET parameter from manifest `env` section",
			opConfig: config.Config{
				WALGSBucket: "global-s3-bucket",
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCustomS3BucketSpec,
			pgsql: acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					Env: []v1.EnvVar{
						{
							Name:  "WAL_S3_BUCKET",
							Value: "custom-s3-bucket",
						},
					},
				},
			},
		},
		{
			subTest: "will set CUSTOM_VARIABLE from pod environment secret and not config map",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
					PodEnvironmentSecret:  testPodEnvironmentSecretName,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCustomVariableSecret,
		},
		{
			subTest: "will set CUSTOM_VARIABLE from pod environment config map",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCustomVariableConfigMap,
		},
		{
			subTest: "will override CUSTOM_VARIABLE of pod environment secret/configmap from manifest `env` section",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
					PodEnvironmentSecret:  testPodEnvironmentSecretName,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
			},
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCustomVariableSpec,
			pgsql: acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					Env: []v1.EnvVar{
						{
							Name:  "CUSTOM_VARIABLE",
							Value: "spec-env-test",
						},
					},
				},
			},
		},
		{
			subTest: "will set CLONE_ parameters from spec and not global config or pod environment config map",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				EndTimestamp: "somewhen",
				UID:          dummyUUID,
				S3WalPath:    "s3://another-bucket",
				S3Endpoint:   "s3.eu-central-1.amazonaws.com",
			},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCloneEnvSpec,
		},
		{
			subTest: "will set CLONE_ parameters from manifest `env` section, followed by other options",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				EndTimestamp: "somewhen",
				UID:          dummyUUID,
				S3WalPath:    "s3://another-bucket",
				S3Endpoint:   "s3.eu-central-1.amazonaws.com",
			},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCloneEnvSpecEnv,
			pgsql: acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					Env: []v1.EnvVar{
						{
							Name:  "CLONE_WAL_BUCKET_SCOPE_PREFIX",
							Value: "test-cluster",
						},
					},
				},
			},
		},
		{
			subTest: "will set CLONE_AWS_ENDPOINT parameter from pod environment config map",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				EndTimestamp: "somewhen",
				UID:          dummyUUID,
			},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCloneEnvConfigMap,
		},
		{
			subTest: "will set CLONE_AWS_ACCESS_KEY_ID parameter from pod environment secret",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentSecret:  testPodEnvironmentSecretName,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				EndTimestamp: "somewhen",
				UID:          dummyUUID,
			},
			standbyDescription: &acidv1.StandbyDescription{},
			expectedValues:     expectedCloneEnvSecret,
		},
		{
			subTest: "will set STANDBY_GOOGLE_APPLICATION_CREDENTIALS parameter from pod environment secret",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentSecret:  testPodEnvironmentSecretName,
					ResourceCheckInterval: &metav1.Duration{Duration: testResourceCheckInterval},
					ResourceCheckTimeout:  &metav1.Duration{Duration: testResourceCheckTimeout},
				},
				WALES3Bucket: "global-s3-bucket",
			},
			cloneDescription: &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{
				GSWalPath: "gs://some/path/",
			},
			expectedValues: expectedStandbyEnvSecret,
		},
	}

	for _, tt := range tests {
		c := newMockCluster(tt.opConfig)
		pgsql := tt.pgsql
		pgsql.Spec.Clone = tt.cloneDescription
		pgsql.Spec.StandbyCluster = tt.standbyDescription
		c.Postgresql = pgsql

		actualEnvs, err := c.generateSpiloPodEnvVars(&pgsql.Spec, types.UID(dummyUUID), exampleSpiloConfig)
		assert.NoError(t, err)

		for _, ev := range tt.expectedValues {
			env := actualEnvs[ev.envIndex]

			if env.Name != ev.envVarConstant {
				t.Errorf("%s %s: expected env name %s, have %s instead",
					t.Name(), tt.subTest, ev.envVarConstant, env.Name)
			}

			if ev.envVarValueRef != nil {
				if !reflect.DeepEqual(env.ValueFrom, ev.envVarValueRef) {
					t.Errorf("%s %s: expected env value reference %#v, have %#v instead",
						t.Name(), tt.subTest, ev.envVarValueRef, env.ValueFrom)
				}
				continue
			}

			if env.Value != ev.envVarValue {
				t.Errorf("%s %s: expected env value %s, have %s instead",
					t.Name(), tt.subTest, ev.envVarValue, env.Value)
			}
		}
	}
}

func TestGetNumberOfInstances(t *testing.T) {
	tests := []struct {
		subTest         string
		config          config.Config
		annotationKey   string
		annotationValue string
		desired         int32
		provided        int32
	}{
		{
			subTest: "no constraints",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      -1,
					MaxInstances:                      -1,
					IgnoreInstanceLimitsAnnotationKey: "",
				},
			},
			annotationKey:   "",
			annotationValue: "",
			desired:         2,
			provided:        2,
		},
		{
			subTest: "minInstances defined",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      2,
					MaxInstances:                      -1,
					IgnoreInstanceLimitsAnnotationKey: "",
				},
			},
			annotationKey:   "",
			annotationValue: "",
			desired:         1,
			provided:        2,
		},
		{
			subTest: "maxInstances defined",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      -1,
					MaxInstances:                      5,
					IgnoreInstanceLimitsAnnotationKey: "",
				},
			},
			annotationKey:   "",
			annotationValue: "",
			desired:         10,
			provided:        5,
		},
		{
			subTest: "ignore minInstances",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      2,
					MaxInstances:                      -1,
					IgnoreInstanceLimitsAnnotationKey: "ignore-instance-limits",
				},
			},
			annotationKey:   "ignore-instance-limits",
			annotationValue: "true",
			desired:         1,
			provided:        1,
		},
		{
			subTest: "want to ignore minInstances but wrong key",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      2,
					MaxInstances:                      -1,
					IgnoreInstanceLimitsAnnotationKey: "ignore-instance-limits",
				},
			},
			annotationKey:   "ignoring-instance-limits",
			annotationValue: "true",
			desired:         1,
			provided:        2,
		},
		{
			subTest: "want to ignore minInstances but wrong value",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      2,
					MaxInstances:                      -1,
					IgnoreInstanceLimitsAnnotationKey: "ignore-instance-limits",
				},
			},
			annotationKey:   "ignore-instance-limits",
			annotationValue: "active",
			desired:         1,
			provided:        2,
		},
		{
			subTest: "annotation set but no constraints to ignore",
			config: config.Config{
				Resources: config.Resources{
					MinInstances:                      -1,
					MaxInstances:                      -1,
					IgnoreInstanceLimitsAnnotationKey: "ignore-instance-limits",
				},
			},
			annotationKey:   "ignore-instance-limits",
			annotationValue: "true",
			desired:         1,
			provided:        1,
		},
	}

	for _, tt := range tests {
		var cluster = New(
			Config{
				OpConfig: tt.config,
			}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

		cluster.Spec.NumberOfInstances = tt.desired
		if tt.annotationKey != "" {
			cluster.ObjectMeta.Annotations = make(map[string]string)
			cluster.ObjectMeta.Annotations[tt.annotationKey] = tt.annotationValue
		}
		numInstances := cluster.getNumberOfInstances(&cluster.Spec)

		if numInstances != tt.provided {
			t.Errorf("%s %s: Expected to get %d instances, have %d instead",
				t.Name(), tt.subTest, tt.provided, numInstances)
		}
	}
}

func TestCloneEnv(t *testing.T) {
	tests := []struct {
		subTest   string
		cloneOpts *acidv1.CloneDescription
		env       v1.EnvVar
		envPos    int
	}{
		{
			subTest: "custom s3 path",
			cloneOpts: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				S3WalPath:    "s3://some/path/",
				EndTimestamp: "somewhen",
			},
			env: v1.EnvVar{
				Name:  "CLONE_WALE_S3_PREFIX",
				Value: "s3://some/path/",
			},
			envPos: 1,
		},
		{
			subTest: "generated s3 path, bucket",
			cloneOpts: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				EndTimestamp: "somewhen",
				UID:          "0000",
			},
			env: v1.EnvVar{
				Name:  "CLONE_WAL_S3_BUCKET",
				Value: "wale-bucket",
			},
			envPos: 1,
		},
		{
			subTest: "generated s3 path, target time",
			cloneOpts: &acidv1.CloneDescription{
				ClusterName:  "test-cluster",
				EndTimestamp: "somewhen",
				UID:          "0000",
			},
			env: v1.EnvVar{
				Name:  "CLONE_TARGET_TIME",
				Value: "somewhen",
			},
			envPos: 4,
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				WALES3Bucket:   "wale-bucket",
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	for _, tt := range tests {
		envs := cluster.generateCloneEnvironment(tt.cloneOpts)

		env := envs[tt.envPos]

		if env.Name != tt.env.Name {
			t.Errorf("%s %s: Expected env name %s, have %s instead",
				t.Name(), tt.subTest, tt.env.Name, env.Name)
		}

		if env.Value != tt.env.Value {
			t.Errorf("%s %s: Expected env value %s, have %s instead",
				t.Name(), tt.subTest, tt.env.Value, env.Value)
		}
	}
}

func TestAppendEnvVar(t *testing.T) {
	tests := []struct {
		subTest      string
		envs         []v1.EnvVar
		envsToAppend []v1.EnvVar
		expectedSize int
	}{
		{
			subTest: "append two variables - one with same key that should get rejected",
			envs: []v1.EnvVar{
				{
					Name:  "CUSTOM_VARIABLE",
					Value: "test",
				},
			},
			envsToAppend: []v1.EnvVar{
				{
					Name:  "CUSTOM_VARIABLE",
					Value: "new-test",
				},
				{
					Name:  "ANOTHER_CUSTOM_VARIABLE",
					Value: "another-test",
				},
			},
			expectedSize: 2,
		},
		{
			subTest: "append empty slice",
			envs: []v1.EnvVar{
				{
					Name:  "CUSTOM_VARIABLE",
					Value: "test",
				},
			},
			envsToAppend: []v1.EnvVar{},
			expectedSize: 1,
		},
		{
			subTest: "append nil",
			envs: []v1.EnvVar{
				{
					Name:  "CUSTOM_VARIABLE",
					Value: "test",
				},
			},
			envsToAppend: nil,
			expectedSize: 1,
		},
	}

	for _, tt := range tests {
		finalEnvs := appendEnvVars(tt.envs, tt.envsToAppend...)

		if len(finalEnvs) != tt.expectedSize {
			t.Errorf("%s %s: expected %d env variables, got %d",
				t.Name(), tt.subTest, tt.expectedSize, len(finalEnvs))
		}

		for _, env := range tt.envs {
			for _, finalEnv := range finalEnvs {
				if env.Name == finalEnv.Name {
					if env.Value != finalEnv.Value {
						t.Errorf("%s %s: expected env value %s of variable %s, got %s instead",
							t.Name(), tt.subTest, env.Value, env.Name, finalEnv.Value)
					}
				}
			}
		}
	}
}

func TestStandbyEnv(t *testing.T) {
	tests := []struct {
		subTest     string
		standbyOpts *acidv1.StandbyDescription
		env         v1.EnvVar
		envPos      int
		envLen      int
	}{
		{
			subTest: "from custom s3 path",
			standbyOpts: &acidv1.StandbyDescription{
				S3WalPath: "s3://some/path/",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_WALE_S3_PREFIX",
				Value: "s3://some/path/",
			},
			envPos: 0,
			envLen: 3,
		},
		{
			subTest: "ignore gs path if s3 is set",
			standbyOpts: &acidv1.StandbyDescription{
				S3WalPath: "s3://some/path/",
				GSWalPath: "gs://some/path/",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_METHOD",
				Value: "STANDBY_WITH_WALE",
			},
			envPos: 1,
			envLen: 3,
		},
		{
			subTest: "from remote primary",
			standbyOpts: &acidv1.StandbyDescription{
				StandbyHost: "remote-primary",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_HOST",
				Value: "remote-primary",
			},
			envPos: 0,
			envLen: 1,
		},
		{
			subTest: "from remote primary with port",
			standbyOpts: &acidv1.StandbyDescription{
				StandbyHost: "remote-primary",
				StandbyPort: "9876",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_PORT",
				Value: "9876",
			},
			envPos: 1,
			envLen: 2,
		},
		{
			subTest: "from remote primary with S3 WAL path",
			standbyOpts: &acidv1.StandbyDescription{
				S3WalPath:   "s3://some/path/",
				StandbyHost: "remote-primary",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_HOST",
				Value: "remote-primary",
			},
			envPos: 0,
			envLen: 4,
		},
		{
			subTest: "verify S3 WAL env with standby host",
			standbyOpts: &acidv1.StandbyDescription{
				S3WalPath:   "s3://some/path/",
				StandbyHost: "remote-primary",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_WALE_S3_PREFIX",
				Value: "s3://some/path/",
			},
			envPos: 1,
			envLen: 4,
		},
		{
			subTest: "from remote primary with GCS WAL path",
			standbyOpts: &acidv1.StandbyDescription{
				GSWalPath:   "gs://some/path/",
				StandbyHost: "remote-primary",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_HOST",
				Value: "remote-primary",
			},
			envPos: 0,
			envLen: 4,
		},
		{
			subTest: "from remote primary with slot name",
			standbyOpts: &acidv1.StandbyDescription{
				StandbyHost:            "remote-primary",
				StandbyPrimarySlotName: "my_slot",
			},
			env: v1.EnvVar{
				Name:  "STANDBY_PRIMARY_SLOT_NAME",
				Value: "my_slot",
			},
			envPos: 1,
			envLen: 2,
		},
	}

	var cluster = New(
		Config{}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	for _, tt := range tests {
		envs := cluster.generateStandbyEnvironment(tt.standbyOpts)

		env := envs[tt.envPos]

		if env.Name != tt.env.Name {
			t.Errorf("%s %s: Expected env name %s, have %s instead",
				t.Name(), tt.subTest, tt.env.Name, env.Name)
		}

		if env.Value != tt.env.Value {
			t.Errorf("%s %s: Expected env value %s, have %s instead",
				t.Name(), tt.subTest, tt.env.Value, env.Value)
		}

		if len(envs) != tt.envLen {
			t.Errorf("%s %s: Expected number of env variables %d, have %d instead",
				t.Name(), tt.subTest, tt.envLen, len(envs))
		}
	}
}

func TestNodeAffinity(t *testing.T) {
	var err error
	var spec acidv1.PostgresSpec
	var cluster *Cluster
	var spiloRunAsUser = int64(101)
	var spiloRunAsGroup = int64(103)
	var spiloFSGroup = int64(103)

	makeSpec := func(nodeAffinity *v1.NodeAffinity) acidv1.PostgresSpec {
		return acidv1.PostgresSpec{
			TeamID: "myapp", NumberOfInstances: 1,
			Resources: &acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			},
			Volume: acidv1.Volume{
				Size: "1G",
			},
			NodeAffinity: nodeAffinity,
		}
	}

	cluster = New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				ProtectedRoles:      []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				Resources: config.Resources{
					SpiloRunAsUser:  &spiloRunAsUser,
					SpiloRunAsGroup: &spiloRunAsGroup,
					SpiloFSGroup:    &spiloFSGroup,
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	nodeAff := &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "test-label",
							Operator: v1.NodeSelectorOpIn,
							Values: []string{
								"test-value",
							},
						},
					},
				},
			},
		},
	}
	spec = makeSpec(nodeAff)
	s, err := cluster.generateStatefulSet(&spec)
	if err != nil {
		assert.NoError(t, err)
	}

	assert.NotNil(t, s.Spec.Template.Spec.Affinity.NodeAffinity, "node affinity in statefulset shouldn't be nil")
	assert.Equal(t, s.Spec.Template.Spec.Affinity.NodeAffinity, nodeAff, "cluster template has correct node affinity")
}

func TestPodAffinity(t *testing.T) {
	clusterName := "acid-test-cluster"
	namespace := "default"

	tests := []struct {
		subTest   string
		preferred bool
		anti      bool
	}{
		{
			subTest:   "generate affinity RequiredDuringSchedulingIgnoredDuringExecution",
			preferred: false,
			anti:      false,
		},
		{
			subTest:   "generate affinity PreferredDuringSchedulingIgnoredDuringExecution",
			preferred: true,
			anti:      false,
		},
		{
			subTest:   "generate anitAffinity RequiredDuringSchedulingIgnoredDuringExecution",
			preferred: false,
			anti:      true,
		},
		{
			subTest:   "generate anitAffinity PreferredDuringSchedulingIgnoredDuringExecution",
			preferred: true,
			anti:      true,
		},
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			NumberOfInstances: 1,
			Resources: &acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			},
			Volume: acidv1.Volume{
				Size: "1G",
			},
		},
	}

	for _, tt := range tests {
		cluster := New(
			Config{
				OpConfig: config.Config{
					EnablePodAntiAffinity:                    tt.anti,
					PodManagementPolicy:                      "ordered_ready",
					ProtectedRoles:                           []string{"admin"},
					PodAntiAffinityPreferredDuringScheduling: tt.preferred,
					Resources: config.Resources{
						ClusterLabels:        map[string]string{"application": "spilo"},
						ClusterNameLabel:     "cluster-name",
						DefaultCPURequest:    "300m",
						DefaultCPULimit:      "300m",
						DefaultMemoryRequest: "300Mi",
						DefaultMemoryLimit:   "300Mi",
						PodRoleLabel:         "spilo-role",
					},
				},
			}, k8sutil.KubernetesClient{}, pg, logger, eventRecorder)

		cluster.Name = clusterName
		cluster.Namespace = namespace

		s, err := cluster.generateStatefulSet(&pg.Spec)
		if err != nil {
			assert.NoError(t, err)
		}

		if !tt.anti {
			assert.Nil(t, s.Spec.Template.Spec.Affinity, "pod affinity should not be set")
		} else {
			if tt.preferred {
				assert.NotNil(t, s.Spec.Template.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, "pod anti-affinity should use preferredDuringScheduling")
				assert.Nil(t, s.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, "pod anti-affinity should not use requiredDuringScheduling")
			} else {
				assert.Nil(t, s.Spec.Template.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, "pod anti-affinity should not use preferredDuringScheduling")
				assert.NotNil(t, s.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, "pod anti-affinity should use requiredDuringScheduling")
			}
		}
	}
}

func testDeploymentOwnerReference(cluster *Cluster, deployment *appsv1.Deployment) error {
	if len(deployment.ObjectMeta.OwnerReferences) == 0 {
		return nil
	}
	owner := deployment.ObjectMeta.OwnerReferences[0]

	if owner.Name != cluster.Postgresql.ObjectMeta.Name {
		return fmt.Errorf("Owner reference is incorrect, got %s, expected %s",
			owner.Name, cluster.Postgresql.ObjectMeta.Name)
	}

	return nil
}

func testServiceOwnerReference(cluster *Cluster, service *v1.Service, role PostgresRole) error {
	if len(service.ObjectMeta.OwnerReferences) == 0 {
		return nil
	}
	owner := service.ObjectMeta.OwnerReferences[0]

	if owner.Name != cluster.Postgresql.ObjectMeta.Name {
		return fmt.Errorf("Owner reference is incorrect, got %s, expected %s",
			owner.Name, cluster.Postgresql.ObjectMeta.Name)
	}

	return nil
}

func TestSharePgSocketWithSidecars(t *testing.T) {
	tests := []struct {
		subTest   string
		podSpec   *v1.PodSpec
		runVolPos int
	}{
		{
			subTest: "empty PodSpec",
			podSpec: &v1.PodSpec{
				Volumes: []v1.Volume{},
				Containers: []v1.Container{
					{
						VolumeMounts: []v1.VolumeMount{},
					},
				},
			},
			runVolPos: 0,
		},
		{
			subTest: "non empty PodSpec",
			podSpec: &v1.PodSpec{
				Volumes: []v1.Volume{{}},
				Containers: []v1.Container{
					{
						Name: "postgres",
						VolumeMounts: []v1.VolumeMount{
							{},
						},
					},
				},
			},
			runVolPos: 1,
		},
	}
	for _, tt := range tests {
		addVarRunVolume(tt.podSpec)
		postgresContainer := getPostgresContainer(tt.podSpec)

		volumeName := tt.podSpec.Volumes[tt.runVolPos].Name
		volumeMountName := postgresContainer.VolumeMounts[tt.runVolPos].Name

		if volumeName != constants.RunVolumeName {
			t.Errorf("%s %s: Expected volume %s was not created, have %s instead",
				t.Name(), tt.subTest, constants.RunVolumeName, volumeName)
		}
		if volumeMountName != constants.RunVolumeName {
			t.Errorf("%s %s: Expected mount %s was not created, have %s instead",
				t.Name(), tt.subTest, constants.RunVolumeName, volumeMountName)
		}
	}
}

func TestTLS(t *testing.T) {
	client, _ := newFakeK8sTestClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	tlsSecretName := "my-secret"
	spiloRunAsUser := int64(101)
	spiloRunAsGroup := int64(103)
	spiloFSGroup := int64(103)
	defaultMode := int32(0640)
	mountPath := "/tls"

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			TeamID: "myapp", NumberOfInstances: 1,
			Resources: &acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			},
			Volume: acidv1.Volume{
				Size: "1G",
			},
			TLS: &acidv1.TLSDescription{
				SecretName: tlsSecretName, CAFile: "ca.crt"},
			AdditionalVolumes: []acidv1.AdditionalVolume{
				{
					Name:      tlsSecretName,
					MountPath: mountPath,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName:  tlsSecretName,
							DefaultMode: &defaultMode,
						},
					},
				},
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				ProtectedRoles:      []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				Resources: config.Resources{
					SpiloRunAsUser:  &spiloRunAsUser,
					SpiloRunAsGroup: &spiloRunAsGroup,
					SpiloFSGroup:    &spiloFSGroup,
				},
			},
		}, client, pg, logger, eventRecorder)

	// create a statefulset
	sts, err := cluster.createStatefulSet()
	assert.NoError(t, err)

	fsGroup := int64(103)
	assert.Equal(t, &fsGroup, sts.Spec.Template.Spec.SecurityContext.FSGroup, "has a default FSGroup assigned")

	volume := v1.Volume{
		Name: "my-secret",
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName:  "my-secret",
				DefaultMode: &defaultMode,
			},
		},
	}
	assert.Contains(t, sts.Spec.Template.Spec.Volumes, volume, "the pod gets a secret volume")

	postgresContainer := getPostgresContainer(&sts.Spec.Template.Spec)
	assert.Contains(t, postgresContainer.VolumeMounts, v1.VolumeMount{
		MountPath: "/tls",
		Name:      "my-secret",
	}, "the volume gets mounted in /tls")

	assert.Contains(t, postgresContainer.Env, v1.EnvVar{Name: "SSL_CERTIFICATE_FILE", Value: "/tls/tls.crt"})
	assert.Contains(t, postgresContainer.Env, v1.EnvVar{Name: "SSL_PRIVATE_KEY_FILE", Value: "/tls/tls.key"})
	assert.Contains(t, postgresContainer.Env, v1.EnvVar{Name: "SSL_CA_FILE", Value: "/tls/ca.crt"})
}

func TestShmVolume(t *testing.T) {
	tests := []struct {
		subTest string
		podSpec *v1.PodSpec
		shmPos  int
	}{
		{
			subTest: "empty PodSpec",
			podSpec: &v1.PodSpec{
				Volumes: []v1.Volume{},
				Containers: []v1.Container{
					{
						VolumeMounts: []v1.VolumeMount{},
					},
				},
			},
			shmPos: 0,
		},
		{
			subTest: "non empty PodSpec",
			podSpec: &v1.PodSpec{
				Volumes: []v1.Volume{{}},
				Containers: []v1.Container{
					{
						Name: "postgres",
						VolumeMounts: []v1.VolumeMount{
							{},
						},
					},
				},
			},
			shmPos: 1,
		},
	}
	for _, tt := range tests {
		addShmVolume(tt.podSpec)
		postgresContainer := getPostgresContainer(tt.podSpec)

		volumeName := tt.podSpec.Volumes[tt.shmPos].Name
		volumeMountName := postgresContainer.VolumeMounts[tt.shmPos].Name

		if volumeName != constants.ShmVolumeName {
			t.Errorf("%s %s: Expected volume %s was not created, have %s instead",
				t.Name(), tt.subTest, constants.ShmVolumeName, volumeName)
		}
		if volumeMountName != constants.ShmVolumeName {
			t.Errorf("%s %s: Expected mount %s was not created, have %s instead",
				t.Name(), tt.subTest, constants.ShmVolumeName, volumeMountName)
		}
	}
}

func TestSpiloRunAsNonRoot(t *testing.T) {
	superUserName := "superuser"
	replicationUserName := "replicationuser"

	spiloRunAsUser := int64(101)
	spiloRunAsGroup := int64(103)
	spiloFSGroup := int64(103)

	createCluster := func(manifestRunAsNonRoot, optionRunAsNonRoot *bool) *Cluster {
		pg := acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{
				Name:      clusterName,
				Namespace: namespace,
			},
			Spec: acidv1.PostgresSpec{
				TeamID:            "myapp",
				NumberOfInstances: 1,
				Volume: acidv1.Volume{
					Size: "5Gi",
				},
				SpiloRunAsNonRoot: manifestRunAsNonRoot,
			},
		}

		client, _ := newFakeK8sTestClient()

		return New(
			Config{
				OpConfig: config.Config{
					PodManagementPolicy: "ordered_ready",
					ProtectedRoles:      []string{"admin"},
					Auth: config.Auth{
						SuperUsername:       superUserName,
						ReplicationUsername: replicationUserName,
					},
					Resources: config.Resources{
						SpiloRunAsUser:    &spiloRunAsUser,
						SpiloRunAsGroup:   &spiloRunAsGroup,
						SpiloFSGroup:      &spiloFSGroup,
						SpiloRunAsNonRoot: optionRunAsNonRoot,
					},
				},
			}, client, pg, logger, eventRecorder)
	}

	t.Run("unset leaves runAsNonRoot out of the pod spec", func(t *testing.T) {
		cluster := createCluster(nil, nil)
		sts, err := cluster.createStatefulSet()
		assert.NoError(t, err)

		assert.Nil(t, sts.Spec.Template.Spec.SecurityContext.RunAsNonRoot)
		postgresContainer := getPostgresContainer(&sts.Spec.Template.Spec)
		assert.Nil(t, postgresContainer.SecurityContext.RunAsNonRoot)
	})

	t.Run("operator option sets runAsNonRoot on pod and container level", func(t *testing.T) {
		cluster := createCluster(nil, util.True())
		sts, err := cluster.createStatefulSet()
		assert.NoError(t, err)

		assert.True(t, *sts.Spec.Template.Spec.SecurityContext.RunAsNonRoot)
		postgresContainer := getPostgresContainer(&sts.Spec.Template.Spec)
		assert.True(t, *postgresContainer.SecurityContext.RunAsNonRoot)
	})

	t.Run("cluster manifest overrides the operator option", func(t *testing.T) {
		cluster := createCluster(util.False(), util.True())
		sts, err := cluster.createStatefulSet()
		assert.NoError(t, err)

		assert.False(t, *sts.Spec.Template.Spec.SecurityContext.RunAsNonRoot)
		postgresContainer := getPostgresContainer(&sts.Spec.Template.Spec)
		assert.False(t, *postgresContainer.SecurityContext.RunAsNonRoot)
	})
}

func TestSecretVolume(t *testing.T) {
	tests := []struct {
		subTest   string
		podSpec   *v1.PodSpec
		secretPos int
	}{
		{
			subTest: "empty PodSpec",
			podSpec: &v1.PodSpec{
				Volumes: []v1.Volume{},
				Containers: []v1.Container{
					{
						VolumeMounts: []v1.VolumeMount{},
					},
				},
			},
			secretPos: 0,
		},
		{
			subTest: "non empty PodSpec",
			podSpec: &v1.PodSpec{
				Volumes: []v1.Volume{{}},
				Containers: []v1.Container{
					{
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      "data",
								ReadOnly:  false,
								MountPath: "/data",
							},
						},
					},
				},
			},
			secretPos: 1,
		},
	}
	for _, tt := range tests {
		additionalSecretMount := "aws-iam-s3-role"
		additionalSecretMountPath := "/meta/credentials"
		postgresContainer := getPostgresContainer(tt.podSpec)

		numMounts := len(postgresContainer.VolumeMounts)

		addSecretVolume(tt.podSpec, additionalSecretMount, additionalSecretMountPath)

		volumeName := tt.podSpec.Volumes[tt.secretPos].Name

		if volumeName != additionalSecretMount {
			t.Errorf("%s %s: Expected volume %s was not created, have %s instead",
				t.Name(), tt.subTest, additionalSecretMount, volumeName)
		}

		for i := range tt.podSpec.Containers {
			volumeMountName := tt.podSpec.Containers[i].VolumeMounts[tt.secretPos].Name

			if volumeMountName != additionalSecretMount {
				t.Errorf("%s %s: Expected mount %s was not created, have %s instead",
					t.Name(), tt.subTest, additionalSecretMount, volumeMountName)
			}
		}

		postgresContainer = getPostgresContainer(tt.podSpec)
		numMountsCheck := len(postgresContainer.VolumeMounts)

		if numMountsCheck != numMounts+1 {
			t.Errorf("Unexpected number of VolumeMounts: got %v instead of %v",
				numMountsCheck, numMounts+1)
		}
	}
}

func TestAdditionalVolume(t *testing.T) {
	client, _ := newFakeK8sTestClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	sidecarName := "sidecar"
	additionalVolumes := []acidv1.AdditionalVolume{
		{
			Name:             "test1",
			MountPath:        "/test1",
			TargetContainers: []string{"all"},
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
		{
			Name:             "test2",
			MountPath:        "/test2",
			TargetContainers: []string{sidecarName},
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
		{
			Name:             "test3",
			MountPath:        "/test3",
			TargetContainers: []string{}, // should mount only to postgres
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
		{
			Name:             "test4",
			MountPath:        "/test4",
			TargetContainers: nil, // should mount only to postgres
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
		{
			Name:             "test5",
			MountPath:        "/test5",
			SubPath:          "subpath",
			TargetContainers: nil, // should mount only to postgres
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
		{
			Name:             "test6",
			MountPath:        "/test6",
			SubPath:          "$(POD_NAME)",
			IsSubPathExpr:    util.True(),
			TargetContainers: nil, // should mount only to postgres
			VolumeSource: v1.VolumeSource{
				EmptyDir: &v1.EmptyDirVolumeSource{},
			},
		},
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			TeamID: "myapp", NumberOfInstances: 1,
			Resources: &acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			},
			Volume: acidv1.Volume{
				Size:          "1G",
				SubPath:       "$(POD_NAME)",
				IsSubPathExpr: util.True(),
			},
			AdditionalVolumes: additionalVolumes,
			Sidecars: []acidv1.Sidecar{
				{
					Name:        sidecarName,
					DockerImage: "test-image",
				},
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:        map[string]string{"application": "spilo"},
					ClusterNameLabel:     "cluster-name",
					DefaultCPURequest:    "300m",
					DefaultCPULimit:      "300m",
					DefaultMemoryRequest: "300Mi",
					DefaultMemoryLimit:   "300Mi",
					PodRoleLabel:         "spilo-role",
				},
			},
		}, client, pg, logger, eventRecorder)

	// create a statefulset
	sts, err := cluster.createStatefulSet()
	assert.NoError(t, err)

	tests := []struct {
		subTest              string
		container            string
		expectedMounts       []string
		expectedSubPaths     []string
		expectedSubPathExprs []string
	}{
		{
			subTest:              "checking volume mounts of postgres container",
			container:            constants.PostgresContainerName,
			expectedMounts:       []string{"pgdata", "test1", "test3", "test4", "test5", "test6"},
			expectedSubPaths:     []string{"", "", "", "", "subpath", ""},
			expectedSubPathExprs: []string{"$(POD_NAME)", "", "", "", "", "$(POD_NAME)"},
		},
		{
			subTest:              "checking volume mounts of sidecar container",
			container:            "sidecar",
			expectedMounts:       []string{"pgdata", "test1", "test2"},
			expectedSubPaths:     []string{"", "", ""},
			expectedSubPathExprs: []string{"$(POD_NAME)", "", ""},
		},
	}

	for _, tt := range tests {
		for _, container := range sts.Spec.Template.Spec.Containers {
			if container.Name != tt.container {
				continue
			}
			mounts := []string{}
			subPaths := []string{}
			subPathExprs := []string{}

			for _, volumeMounts := range container.VolumeMounts {
				mounts = append(mounts, volumeMounts.Name)
				subPaths = append(subPaths, volumeMounts.SubPath)
				subPathExprs = append(subPathExprs, volumeMounts.SubPathExpr)
			}

			if !util.IsEqualIgnoreOrder(mounts, tt.expectedMounts) {
				t.Errorf("%s %s: different volume mounts: got %v, expected %v",
					t.Name(), tt.subTest, mounts, tt.expectedMounts)
			}

			if !util.IsEqualIgnoreOrder(subPaths, tt.expectedSubPaths) {
				t.Errorf("%s %s: different volume subPaths: got %v, expected %v",
					t.Name(), tt.subTest, subPaths, tt.expectedSubPaths)
			}

			if !util.IsEqualIgnoreOrder(subPathExprs, tt.expectedSubPathExprs) {
				t.Errorf("%s %s: different volume subPathExprs: got %v, expected %v",
					t.Name(), tt.subTest, subPathExprs, tt.expectedSubPathExprs)
			}
		}
	}
}

func TestVolumeSelector(t *testing.T) {
	makeSpec := func(volume acidv1.Volume) acidv1.PostgresSpec {
		return acidv1.PostgresSpec{
			TeamID:            "myapp",
			NumberOfInstances: 0,
			Resources: &acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			},
			Volume: volume,
		}
	}

	tests := []struct {
		subTest      string
		volume       acidv1.Volume
		wantSelector *metav1.LabelSelector
	}{
		{
			subTest: "PVC template has no selector",
			volume: acidv1.Volume{
				Size: "1G",
			},
			wantSelector: nil,
		},
		{
			subTest: "PVC template has simple label selector",
			volume: acidv1.Volume{
				Size: "1G",
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"environment": "unittest"},
				},
			},
			wantSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"environment": "unittest"},
			},
		},
		{
			subTest: "PVC template has full selector",
			volume: acidv1.Volume{
				Size: "1G",
				Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"environment": "unittest"},
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "flavour",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"banana", "chocolate"},
						},
					},
				},
			},
			wantSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"environment": "unittest"},
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "flavour",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{"banana", "chocolate"},
					},
				},
			},
		},
	}

	cluster := New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				ProtectedRoles:      []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	for _, tt := range tests {
		pgSpec := makeSpec(tt.volume)
		sts, err := cluster.generateStatefulSet(&pgSpec)
		if err != nil {
			t.Fatalf("%s %s: no statefulset created %v", t.Name(), tt.subTest, err)
		}

		volIdx := len(sts.Spec.VolumeClaimTemplates)
		for i, ct := range sts.Spec.VolumeClaimTemplates {
			if ct.ObjectMeta.Name == constants.DataVolumeName {
				volIdx = i
				break
			}
		}
		if volIdx == len(sts.Spec.VolumeClaimTemplates) {
			t.Errorf("%s %s: no datavolume found in sts", t.Name(), tt.subTest)
		}

		selector := sts.Spec.VolumeClaimTemplates[volIdx].Spec.Selector
		if !reflect.DeepEqual(selector, tt.wantSelector) {
			t.Errorf("%s %s: expected: %#v but got: %#v", t.Name(), tt.subTest, tt.wantSelector, selector)
		}
	}
}

// inject sidecars through all available mechanisms and check the resulting container specs
func TestSidecars(t *testing.T) {
	var err error
	var spec acidv1.PostgresSpec
	var cluster *Cluster

	generateKubernetesResources := func(cpuRequest string, cpuLimit string, memoryRequest string, memoryLimit string) v1.ResourceRequirements {
		parsedCPURequest, err := resource.ParseQuantity(cpuRequest)
		assert.NoError(t, err)
		parsedCPULimit, err := resource.ParseQuantity(cpuLimit)
		assert.NoError(t, err)
		parsedMemoryRequest, err := resource.ParseQuantity(memoryRequest)
		assert.NoError(t, err)
		parsedMemoryLimit, err := resource.ParseQuantity(memoryLimit)
		assert.NoError(t, err)
		return v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU:    parsedCPURequest,
				v1.ResourceMemory: parsedMemoryRequest,
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU:    parsedCPULimit,
				v1.ResourceMemory: parsedMemoryLimit,
			},
		}
	}

	spec = acidv1.PostgresSpec{
		PostgresqlParam: acidv1.PostgresqlParam{
			PgVersion: "18",
			Parameters: map[string]string{
				"max_connections": "100",
			},
		},
		TeamID: "myapp", NumberOfInstances: 1,
		Resources: &acidv1.Resources{
			ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
		},
		Volume: acidv1.Volume{
			Size: "1G",
		},
		Sidecars: []acidv1.Sidecar{
			{
				Name:        "cluster-specific-sidecar",
				DockerImage: "test-image",
			},
			{
				Name:        "cluster-specific-sidecar-with-resources",
				DockerImage: "test-image",
				Resources: &acidv1.Resources{
					ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("210m"), Memory: k8sutil.StringToPointer("0.8Gi")},
					ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("510m"), Memory: k8sutil.StringToPointer("1.4Gi")},
				},
			},
			{
				Name:        "replace-sidecar",
				DockerImage: "override-image",
			},
		},
	}

	cluster = New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				ProtectedRoles:      []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				Resources: config.Resources{
					DefaultCPURequest:    "200m",
					MaxCPURequest:        "300m",
					DefaultCPULimit:      "500m",
					DefaultMemoryRequest: "0.7Gi",
					MaxMemoryRequest:     "1.0Gi",
					DefaultMemoryLimit:   "1.3Gi",
				},
				SidecarImages: map[string]string{
					"deprecated-global-sidecar": "image:123",
				},
				SidecarContainers: []v1.Container{
					{
						Name:  "global-sidecar",
						Image: "test-image",
					},
					// will be replaced by a cluster specific sidecar with the same name
					{
						Name:  "replace-sidecar",
						Image: "replaced-image",
					},
				},
				Scalyr: config.Scalyr{
					ScalyrAPIKey:        "abc",
					ScalyrImage:         "scalyr-image",
					ScalyrCPURequest:    "220m",
					ScalyrCPULimit:      "520m",
					ScalyrMemoryRequest: "0.9Gi",
					// ise default memory limit
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	s, err := cluster.generateStatefulSet(&spec)
	assert.NoError(t, err)

	env := []v1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.namespace",
				},
			},
		},
		{
			Name:  "POSTGRES_USER",
			Value: superUserName,
		},
		{
			Name: "POSTGRES_PASSWORD",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: "",
					},
					Key: "password",
				},
			},
		},
	}
	mounts := []v1.VolumeMount{
		{
			Name:      "pgdata",
			MountPath: "/home/postgres/pgdata",
		},
	}

	// deduplicated sidecars and Patroni
	assert.Equal(t, 7, len(s.Spec.Template.Spec.Containers), "wrong number of containers")

	// cluster specific sidecar
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:            "cluster-specific-sidecar",
		Image:           "test-image",
		Env:             env,
		Resources:       generateKubernetesResources("200m", "500m", "0.7Gi", "1.3Gi"),
		ImagePullPolicy: v1.PullIfNotPresent,
		VolumeMounts:    mounts,
	})

	// container specific resources
	expectedResources := generateKubernetesResources("210m", "510m", "0.8Gi", "1.4Gi")
	assert.Equal(t, expectedResources.Requests[v1.ResourceCPU], s.Spec.Template.Spec.Containers[2].Resources.Requests[v1.ResourceCPU])
	assert.Equal(t, expectedResources.Limits[v1.ResourceCPU], s.Spec.Template.Spec.Containers[2].Resources.Limits[v1.ResourceCPU])
	assert.Equal(t, expectedResources.Requests[v1.ResourceMemory], s.Spec.Template.Spec.Containers[2].Resources.Requests[v1.ResourceMemory])
	assert.Equal(t, expectedResources.Limits[v1.ResourceMemory], s.Spec.Template.Spec.Containers[2].Resources.Limits[v1.ResourceMemory])

	// deprecated global sidecar
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:            "deprecated-global-sidecar",
		Image:           "image:123",
		Env:             env,
		Resources:       generateKubernetesResources("200m", "500m", "0.7Gi", "1.3Gi"),
		ImagePullPolicy: v1.PullIfNotPresent,
		VolumeMounts:    mounts,
	})

	// global sidecar
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:         "global-sidecar",
		Image:        "test-image",
		Env:          env,
		VolumeMounts: mounts,
	})

	// replaced sidecar
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:            "replace-sidecar",
		Image:           "override-image",
		Resources:       generateKubernetesResources("200m", "500m", "0.7Gi", "1.3Gi"),
		ImagePullPolicy: v1.PullIfNotPresent,
		Env:             env,
		VolumeMounts:    mounts,
	})

	// replaced sidecar
	// the order in env is important
	scalyrEnv := append(env, v1.EnvVar{Name: "SCALYR_API_KEY", Value: "abc"}, v1.EnvVar{Name: "SCALYR_SERVER_HOST", Value: ""})
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:            "scalyr-sidecar",
		Image:           "scalyr-image",
		Resources:       generateKubernetesResources("220m", "520m", "0.9Gi", "1.3Gi"),
		ImagePullPolicy: v1.PullIfNotPresent,
		Env:             scalyrEnv,
		VolumeMounts:    mounts,
	})

}

func TestContainerValidation(t *testing.T) {
	testCases := []struct {
		name          string
		spec          acidv1.PostgresSpec
		clusterConfig Config
		expectedError string
	}{
		{
			name: "init container without image",
			spec: acidv1.PostgresSpec{
				PostgresqlParam: acidv1.PostgresqlParam{
					PgVersion: "18",
				},
				TeamID:            "myapp",
				NumberOfInstances: 1,
				Volume: acidv1.Volume{
					Size: "1G",
				},
				InitContainers: []v1.Container{
					{
						Name: "invalid-initcontainer",
					},
				},
			},
			clusterConfig: Config{
				OpConfig: config.Config{
					PodManagementPolicy: "ordered_ready",
					ProtectedRoles:      []string{"admin"},
					Auth: config.Auth{
						SuperUsername:       superUserName,
						ReplicationUsername: replicationUserName,
					},
				},
			},
			expectedError: "image is required",
		},
		{
			name: "sidecar without name",
			spec: acidv1.PostgresSpec{
				PostgresqlParam: acidv1.PostgresqlParam{
					PgVersion: "18",
				},
				TeamID:            "myapp",
				NumberOfInstances: 1,
				Volume: acidv1.Volume{
					Size: "1G",
				},
			},
			clusterConfig: Config{
				OpConfig: config.Config{
					PodManagementPolicy: "ordered_ready",
					ProtectedRoles:      []string{"admin"},
					Auth: config.Auth{
						SuperUsername:       superUserName,
						ReplicationUsername: replicationUserName,
					},
					SidecarContainers: []v1.Container{
						{
							Image: "test-image",
						},
					},
				},
			},
			expectedError: "name is required",
		},
		{
			name: "sidecar without image",
			spec: acidv1.PostgresSpec{
				PostgresqlParam: acidv1.PostgresqlParam{
					PgVersion: "18",
				},
				TeamID:            "myapp",
				NumberOfInstances: 1,
				Volume: acidv1.Volume{
					Size: "1G",
				},
				Sidecars: []acidv1.Sidecar{
					{
						Name: "invalid-sidecar",
					},
				},
			},
			clusterConfig: Config{
				OpConfig: config.Config{
					PodManagementPolicy: "ordered_ready",
					ProtectedRoles:      []string{"admin"},
					Auth: config.Auth{
						SuperUsername:       superUserName,
						ReplicationUsername: replicationUserName,
					},
				},
			},
			expectedError: "image is required",
		},
		{
			name: "valid containers pass validation",
			spec: acidv1.PostgresSpec{
				PostgresqlParam: acidv1.PostgresqlParam{
					PgVersion: "18",
				},
				TeamID:            "myapp",
				NumberOfInstances: 1,
				Volume: acidv1.Volume{
					Size: "1G",
				},
				Sidecars: []acidv1.Sidecar{
					{
						Name:        "valid-sidecar",
						DockerImage: "busybox:latest",
					},
				},
				InitContainers: []v1.Container{
					{
						Name:  "valid-initcontainer",
						Image: "alpine:latest",
					},
				},
			},
			clusterConfig: Config{
				OpConfig: config.Config{
					PodManagementPolicy: "ordered_ready",
					ProtectedRoles:      []string{"admin"},
					Auth: config.Auth{
						SuperUsername:       superUserName,
						ReplicationUsername: replicationUserName,
					},
				},
			},
			expectedError: "",
		},
		{
			name: "multiple invalid sidecars",
			spec: acidv1.PostgresSpec{
				Sidecars: []acidv1.Sidecar{
					{
						Name: "sidecar1",
					},
					{
						Name: "sidecar2",
					},
				},
			},
			expectedError: "image is required",
		},
		{
			name: "empty container name and image",
			spec: acidv1.PostgresSpec{
				InitContainers: []v1.Container{
					{
						Name:  "",
						Image: "",
					},
				},
			},
			expectedError: "name is required",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := New(tc.clusterConfig, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

			_, err := cluster.generateStatefulSet(&tc.spec)

			if tc.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGeneratePodDisruptionBudget(t *testing.T) {
	testName := "Test PodDisruptionBudget spec generation"

	hasName := func(pdbName string) func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
		return func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
			if pdbName != podDisruptionBudget.ObjectMeta.Name {
				return fmt.Errorf("PodDisruptionBudget name is incorrect, got %s, expected %s",
					podDisruptionBudget.ObjectMeta.Name, pdbName)
			}
			return nil
		}
	}

	hasMaxUnavailable := func(expected string) func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
		return func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
			actual := podDisruptionBudget.Spec.MaxUnavailable.String()
			if actual != expected {
				return fmt.Errorf("PodDisruptionBudget MaxUnavailable is incorrect, got %s, expected %s",
					actual, expected)
			}
			return nil
		}
	}

	hasMinAvailable := func(expectedMinAvailable int) func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
		return func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
			actual := podDisruptionBudget.Spec.MinAvailable.IntVal
			if actual != int32(expectedMinAvailable) {
				return fmt.Errorf("PodDisruptionBudget MinAvailable is incorrect, got %d, expected %d",
					actual, expectedMinAvailable)
			}
			return nil
		}
	}

	testLabelsAndSelectors := func(isPrimary bool) func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
		return func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
			masterLabelSelectorDisabled := cluster.OpConfig.PDBMasterLabelSelector != nil && !*cluster.OpConfig.PDBMasterLabelSelector
			if podDisruptionBudget.ObjectMeta.Namespace != "myapp" {
				return fmt.Errorf("Object Namespace incorrect.")
			}
			expectedLabels := map[string]string{"team": "myapp", "cluster-name": "myapp-database"}
			if !reflect.DeepEqual(podDisruptionBudget.Labels, expectedLabels) {
				return fmt.Errorf("Labels incorrect, got %#v, expected %#v", podDisruptionBudget.Labels, expectedLabels)
			}
			if !masterLabelSelectorDisabled {
				if isPrimary {
					expectedLabels := &metav1.LabelSelector{
						MatchLabels: map[string]string{"spilo-role": "master", "cluster-name": "myapp-database"}}
					if !reflect.DeepEqual(podDisruptionBudget.Spec.Selector, expectedLabels) {
						return fmt.Errorf("MatchLabels incorrect, got %#v, expected %#v", podDisruptionBudget.Spec.Selector, expectedLabels)
					}
				} else {
					expectedLabels := &metav1.LabelSelector{
						MatchLabels: map[string]string{"cluster-name": "myapp-database", "critical-operation": "true"}}
					if !reflect.DeepEqual(podDisruptionBudget.Spec.Selector, expectedLabels) {
						return fmt.Errorf("MatchLabels incorrect, got %#v, expected %#v", podDisruptionBudget.Spec.Selector, expectedLabels)
					}
				}
			}

			return nil
		}
	}

	testPodDisruptionBudgetOwnerReference := func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error {
		if len(podDisruptionBudget.ObjectMeta.OwnerReferences) == 0 {
			return nil
		}
		owner := podDisruptionBudget.ObjectMeta.OwnerReferences[0]

		if owner.Name != cluster.Postgresql.ObjectMeta.Name {
			return fmt.Errorf("Owner reference is incorrect, got %s, expected %s",
				owner.Name, cluster.Postgresql.ObjectMeta.Name)
		}

		return nil
	}

	tests := []struct {
		scenario string
		spec     *Cluster
		check    []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error
	}{
		{
			scenario: "With multiple instances",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb"}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-pdb"),
				hasMinAvailable(1),
				testLabelsAndSelectors(true),
			},
		},
		{
			scenario: "With zero instances",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb"}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 0}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-pdb"),
				hasMinAvailable(0),
				testLabelsAndSelectors(true),
			},
		},
		{
			scenario: "With PodDisruptionBudget disabled",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb", EnablePodDisruptionBudget: util.False()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-pdb"),
				hasMinAvailable(0),
				testLabelsAndSelectors(true),
			},
		},
		{
			scenario: "With non-default PDBNameFormat and PodDisruptionBudget explicitly enabled",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-databass-budget", EnablePodDisruptionBudget: util.True()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-databass-budget"),
				hasMinAvailable(1),
				testLabelsAndSelectors(true),
			},
		},
		{
			scenario: "With PDBMasterLabelSelector disabled",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb", EnablePodDisruptionBudget: util.True(), PDBMasterLabelSelector: util.False()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3, Patroni: acidv1.Patroni{SynchronousModeStrict: true, SynchronousNodeCount: 1}}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-pdb"),
				hasMinAvailable(2),
				testLabelsAndSelectors(true),
			},
		},
		{
			scenario: "With OwnerReference enabled",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role", EnableOwnerReferences: util.True()}, PDBNameFormat: "postgres-{cluster}-pdb", EnablePodDisruptionBudget: util.True()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-pdb"),
				hasMinAvailable(1),
				testLabelsAndSelectors(true),
			},
		},
	}

	for _, tt := range tests {
		result := tt.spec.generatePrimaryPodDisruptionBudget()
		for _, check := range tt.check {
			err := check(tt.spec, result)
			if err != nil {
				t.Errorf("%s [%s]: PodDisruptionBudget spec is incorrect, %+v",
					testName, tt.scenario, err)
			}
		}
	}

	testCriticalOp := []struct {
		scenario string
		spec     *Cluster
		check    []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error
	}{
		{
			scenario: "With multiple instances",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb"}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-critical-op-pdb"),
				hasMaxUnavailable("0"),
				testLabelsAndSelectors(false),
			},
		},
		{
			scenario: "With zero instances",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb"}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 0}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-critical-op-pdb"),
				hasMaxUnavailable("100%"),
				testLabelsAndSelectors(false),
			},
		},
		{
			scenario: "With PodDisruptionBudget disabled",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb", EnablePodDisruptionBudget: util.False()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-critical-op-pdb"),
				hasMaxUnavailable("100%"),
				testLabelsAndSelectors(false),
			},
		},
		{
			scenario: "With OwnerReference enabled",
			spec: New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role", EnableOwnerReferences: util.True()}, PDBNameFormat: "postgres-{cluster}-pdb", EnablePodDisruptionBudget: util.True()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			check: []func(cluster *Cluster, podDisruptionBudget *policyv1.PodDisruptionBudget) error{
				testPodDisruptionBudgetOwnerReference,
				hasName("postgres-myapp-database-critical-op-pdb"),
				hasMaxUnavailable("0"),
				testLabelsAndSelectors(false),
			},
		},
	}

	for _, tt := range testCriticalOp {
		result := tt.spec.generateCriticalOpPodDisruptionBudget()
		for _, check := range tt.check {
			err := check(tt.spec, result)
			if err != nil {
				t.Errorf("%s [%s]: PodDisruptionBudget spec is incorrect, %+v",
					testName, tt.scenario, err)
			}
		}
	}
}

func TestGenerateSingleUserSecret_OwnerReferences(t *testing.T) {
	testName := "Test generateSingleUserSecret owner references"

	newCluster := func(ownerRefs, secretsDeletion *bool, crossNamespaceSecret bool) *Cluster {
		cfg := Config{
			OpConfig: config.Config{
				Resources: config.Resources{
					ClusterNameLabel:      "cluster-name",
					PodRoleLabel:          "spilo-role",
					EnableOwnerReferences: ownerRefs,
				},
				EnableSecretsDeletion:      secretsDeletion,
				EnableCrossNamespaceSecret: crossNamespaceSecret,
			},
		}
		pg := acidv1.Postgresql{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myapp-database",
				Namespace: "myapp",
				UID:       types.UID("myapp-database-uid"),
			},
			Spec: acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 1},
		}
		return New(cfg, k8sutil.KubernetesClient{}, pg, logger, eventRecorder)
	}

	newPgUser := func(namespace string) spec.PgUser {
		return spec.PgUser{
			Name:      "app_user",
			Namespace: namespace,
			Password:  "secret",
		}
	}

	hasControllerOwnerRef := func(cluster *Cluster) func(*v1.Secret) error {
		return func(secret *v1.Secret) error {
			for _, ref := range secret.OwnerReferences {
				if ref.UID == cluster.Postgresql.ObjectMeta.UID &&
					ref.Name == cluster.Postgresql.ObjectMeta.Name &&
					ref.Controller != nil && *ref.Controller {
					return nil
				}
			}
			return fmt.Errorf("expected a controller owner reference pointing at the Postgresql CR, got %#v",
				secret.OwnerReferences)
		}
	}

	hasNoControllerOwnerRef := func(cluster *Cluster) func(*v1.Secret) error {
		return func(secret *v1.Secret) error {
			for _, ref := range secret.OwnerReferences {
				if ref.UID == cluster.Postgresql.ObjectMeta.UID && ref.Controller != nil && *ref.Controller {
					return fmt.Errorf("expected no controller owner reference, got %#v", secret.OwnerReferences)
				}
			}
			return nil
		}
	}

	tests := []struct {
		scenario              string
		cluster               *Cluster
		pgUser                spec.PgUser
		expectControllerOwner bool
	}{
		{
			scenario:              "owner refs + secrets deletion enabled (default)",
			cluster:               newCluster(util.True(), util.True(), false),
			pgUser:                newPgUser("myapp"),
			expectControllerOwner: true,
		},
		{
			scenario:              "owner refs enabled, secrets deletion disabled (skip owner ref)",
			cluster:               newCluster(util.True(), util.False(), false),
			pgUser:                newPgUser("myapp"),
			expectControllerOwner: false,
		},
		{
			scenario:              "owner refs enabled, secrets deletion unset (default true)",
			cluster:               newCluster(util.True(), nil, false),
			pgUser:                newPgUser("myapp"),
			expectControllerOwner: true,
		},
		{
			scenario:              "owner refs disabled, secrets deletion enabled",
			cluster:               newCluster(util.False(), util.True(), false),
			pgUser:                newPgUser("myapp"),
			expectControllerOwner: false,
		},
		{
			scenario:              "owner refs disabled, secrets deletion disabled",
			cluster:               newCluster(util.False(), util.False(), false),
			pgUser:                newPgUser("myapp"),
			expectControllerOwner: false,
		},
		{
			scenario:              "cross-namespace secret, owner refs + secrets deletion enabled",
			cluster:               newCluster(util.True(), util.True(), true),
			pgUser:                newPgUser("other-ns"),
			expectControllerOwner: false,
		},
		{
			scenario:              "cross-namespace secret, owner refs enabled, secrets deletion disabled",
			cluster:               newCluster(util.True(), util.False(), true),
			pgUser:                newPgUser("other-ns"),
			expectControllerOwner: false,
		},
	}

	for _, tt := range tests {
		secret := tt.cluster.generateSingleUserSecret(tt.pgUser)
		if secret == nil {
			t.Errorf("%s [%s]: expected a non-nil secret", testName, tt.scenario)
			continue
		}

		var check func(*v1.Secret) error
		if tt.expectControllerOwner {
			check = hasControllerOwnerRef(tt.cluster)
		} else {
			check = hasNoControllerOwnerRef(tt.cluster)
		}
		if err := check(secret); err != nil {
			t.Errorf("%s [%s]: %+v", testName, tt.scenario, err)
		}
	}
}

func TestGenerateService(t *testing.T) {
	var spec acidv1.PostgresSpec
	var cluster *Cluster
	var enableLB bool = true
	spec = acidv1.PostgresSpec{
		TeamID: "myapp", NumberOfInstances: 1,
		Resources: &acidv1.Resources{
			ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
		},
		Volume: acidv1.Volume{
			Size: "1G",
		},
		Sidecars: []acidv1.Sidecar{
			{
				Name: "cluster-specific-sidecar",
			},
			{
				Name:        "cluster-specific-sidecar-with-resources",
				DockerImage: "test-image",
				Resources: &acidv1.Resources{
					ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("210m"), Memory: k8sutil.StringToPointer("0.8Gi")},
					ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("510m"), Memory: k8sutil.StringToPointer("1.4Gi")},
				},
			},
			{
				Name:        "replace-sidecar",
				DockerImage: "override-image",
			},
		},
		EnableMasterLoadBalancer: &enableLB,
	}

	cluster = New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				ProtectedRoles:      []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				Resources: config.Resources{
					DefaultCPURequest:    "200m",
					MaxCPURequest:        "300m",
					DefaultCPULimit:      "500m",
					DefaultMemoryRequest: "0.7Gi",
					MaxMemoryRequest:     "1.0Gi",
					DefaultMemoryLimit:   "1.3Gi",
				},
				SidecarImages: map[string]string{
					"deprecated-global-sidecar": "image:123",
				},
				SidecarContainers: []v1.Container{
					{
						Name: "global-sidecar",
					},
					// will be replaced by a cluster specific sidecar with the same name
					{
						Name:  "replace-sidecar",
						Image: "replaced-image",
					},
				},
				Scalyr: config.Scalyr{
					ScalyrAPIKey:        "abc",
					ScalyrImage:         "scalyr-image",
					ScalyrCPURequest:    "220m",
					ScalyrCPULimit:      "520m",
					ScalyrMemoryRequest: "0.9Gi",
					// ise default memory limit
				},
				ExternalTrafficPolicy: "Cluster",
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	service := cluster.generateService(Master, &spec)
	assert.Equal(t, v1.ServiceExternalTrafficPolicyTypeCluster, service.Spec.ExternalTrafficPolicy)
	cluster.OpConfig.ExternalTrafficPolicy = "Local"
	service = cluster.generateService(Master, &spec)
	assert.Equal(t, v1.ServiceExternalTrafficPolicyTypeLocal, service.Spec.ExternalTrafficPolicy)

}

func TestCreateLoadBalancerLogic(t *testing.T) {
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	tests := []struct {
		subtest  string
		role     PostgresRole
		spec     *acidv1.PostgresSpec
		opConfig config.Config
		result   bool
	}{
		{
			subtest:  "new format, load balancer is enabled for replica",
			role:     Replica,
			spec:     &acidv1.PostgresSpec{EnableReplicaLoadBalancer: util.True()},
			opConfig: config.Config{},
			result:   true,
		},
		{
			subtest:  "new format, load balancer is disabled for replica",
			role:     Replica,
			spec:     &acidv1.PostgresSpec{EnableReplicaLoadBalancer: util.False()},
			opConfig: config.Config{},
			result:   false,
		},
		{
			subtest:  "new format, load balancer isn't specified for replica",
			role:     Replica,
			spec:     &acidv1.PostgresSpec{EnableReplicaLoadBalancer: nil},
			opConfig: config.Config{EnableReplicaLoadBalancer: true},
			result:   true,
		},
		{
			subtest:  "new format, load balancer isn't specified for replica",
			role:     Replica,
			spec:     &acidv1.PostgresSpec{EnableReplicaLoadBalancer: nil},
			opConfig: config.Config{EnableReplicaLoadBalancer: false},
			result:   false,
		},
	}
	for _, tt := range tests {
		cluster.OpConfig = tt.opConfig
		result := cluster.shouldCreateLoadBalancerForService(tt.role, tt.spec)
		if tt.result != result {
			t.Errorf("%s %s: Load balancer is %t, expect %t for role %#v and spec %#v",
				t.Name(), tt.subtest, result, tt.result, tt.role, tt.spec)
		}
	}
}

func newLBFakeClient() (k8sutil.KubernetesClient, *fake.Clientset) {
	clientSet := fake.NewSimpleClientset()

	return k8sutil.KubernetesClient{
		DeploymentsGetter: clientSet.AppsV1(),
		PodsGetter:        clientSet.CoreV1(),
		ServicesGetter:    clientSet.CoreV1(),
		SecretsGetter:     clientSet.CoreV1(),
	}, clientSet
}

func getServices(serviceType v1.ServiceType, sourceRanges []string, extTrafficPolicy, clusterName string, nodePort int32) []v1.ServiceSpec {
	return []v1.ServiceSpec{
		{
			ExternalTrafficPolicy:    v1.ServiceExternalTrafficPolicyType(extTrafficPolicy),
			LoadBalancerSourceRanges: sourceRanges,
			Ports:                    []v1.ServicePort{{Name: "postgresql", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}, NodePort: nodePort}},
			Selector:                 map[string]string{"spilo-role": "master", "application": "spilo", "cluster-name": clusterName},
			Type:                     serviceType,
		},
		{
			ExternalTrafficPolicy:    v1.ServiceExternalTrafficPolicyType(extTrafficPolicy),
			LoadBalancerSourceRanges: sourceRanges,
			Ports:                    []v1.ServicePort{{Name: clusterName + "-pooler", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}, NodePort: nodePort}},
			Selector:                 map[string]string{"connection-pooler": clusterName + "-pooler"},
			Type:                     serviceType,
		},
		{
			ExternalTrafficPolicy:    v1.ServiceExternalTrafficPolicyType(extTrafficPolicy),
			LoadBalancerSourceRanges: sourceRanges,
			Ports:                    []v1.ServicePort{{Name: "postgresql", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}, NodePort: nodePort}},
			Selector:                 map[string]string{"spilo-role": "replica", "application": "spilo", "cluster-name": clusterName},
			Type:                     serviceType,
		},
		{
			ExternalTrafficPolicy:    v1.ServiceExternalTrafficPolicyType(extTrafficPolicy),
			LoadBalancerSourceRanges: sourceRanges,
			Ports:                    []v1.ServicePort{{Name: clusterName + "-pooler-repl", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}, NodePort: nodePort}},
			Selector:                 map[string]string{"connection-pooler": clusterName + "-pooler-repl"},
			Type:                     serviceType,
		},
	}
}

func TestEnableLoadBalancers(t *testing.T) {
	client, _ := newLBFakeClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	clusterNameLabel := "cluster-name"
	roleLabel := "spilo-role"
	roles := []PostgresRole{Master, Replica}
	sourceRanges := []string{"192.186.1.2/22"}
	extTrafficPolicy := "Cluster"

	tests := []struct {
		subTest          string
		config           config.Config
		pgSpec           acidv1.Postgresql
		expectedServices []v1.ServiceSpec
	}{
		{
			subTest: "LBs enabled in config, disabled in manifest",
			config: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				EnableMasterLoadBalancer:        true,
				EnableMasterPoolerLoadBalancer:  true,
				EnableReplicaLoadBalancer:       true,
				EnableReplicaPoolerLoadBalancer: true,
				ExternalTrafficPolicy:           extTrafficPolicy,
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: clusterNameLabel,
					PodRoleLabel:     roleLabel,
				},
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					AllowedSourceRanges:             sourceRanges,
					EnableConnectionPooler:          util.True(),
					EnableReplicaConnectionPooler:   util.True(),
					EnableMasterLoadBalancer:        util.False(),
					EnableMasterPoolerLoadBalancer:  util.False(),
					EnableReplicaLoadBalancer:       util.False(),
					EnableReplicaPoolerLoadBalancer: util.False(),
					NumberOfInstances:               1,
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedServices: getServices(v1.ServiceTypeClusterIP, nil, "", clusterName, 0),
		},
		{
			subTest: "LBs enabled in manifest, disabled in config",
			config: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				EnableMasterLoadBalancer:        false,
				EnableMasterPoolerLoadBalancer:  false,
				EnableReplicaLoadBalancer:       false,
				EnableReplicaPoolerLoadBalancer: false,
				ExternalTrafficPolicy:           extTrafficPolicy,
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: clusterNameLabel,
					PodRoleLabel:     roleLabel,
				},
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					AllowedSourceRanges:             sourceRanges,
					EnableConnectionPooler:          util.True(),
					EnableReplicaConnectionPooler:   util.True(),
					EnableMasterLoadBalancer:        util.True(),
					EnableMasterPoolerLoadBalancer:  util.True(),
					EnableReplicaLoadBalancer:       util.True(),
					EnableReplicaPoolerLoadBalancer: util.True(),
					NumberOfInstances:               1,
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedServices: getServices(v1.ServiceTypeLoadBalancer, sourceRanges, extTrafficPolicy, clusterName, 0),
		},
	}

	for _, tt := range tests {
		var cluster = New(
			Config{
				OpConfig: tt.config,
			}, client, tt.pgSpec, logger, eventRecorder)

		cluster.Name = clusterName
		cluster.Namespace = namespace
		cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{}
		generatedServices := make([]v1.ServiceSpec, 0)
		for _, role := range roles {
			cluster.syncService(role)
			cluster.ConnectionPooler[role] = &ConnectionPoolerObjects{
				Name:        cluster.connectionPoolerName(role),
				ClusterName: cluster.Name,
				Namespace:   cluster.Namespace,
				Role:        role,
			}
			cluster.syncConnectionPoolerWorker(&tt.pgSpec, &tt.pgSpec, role)
			generatedServices = append(generatedServices, cluster.Services[role].Spec)
			generatedServices = append(generatedServices, cluster.ConnectionPooler[role].Service.Spec)
		}
		if !reflect.DeepEqual(tt.expectedServices, generatedServices) {
			t.Errorf("%s %s: expected %#v but got %#v", t.Name(), tt.subTest, tt.expectedServices, generatedServices)
		}
	}
}

func TestEnableNodePorts(t *testing.T) {
	clusterName := "acid-test-cluster"
	namespace := "default"
	clusterNameLabel := "cluster-name"
	roleLabel := "spilo-role"
	roles := []PostgresRole{Master, Replica}
	extTrafficPolicy := "Cluster"
	port := int32(1337)

	tests := []struct {
		subTest          string
		config           config.Config
		pgSpec           acidv1.Postgresql
		expectedServices []v1.ServiceSpec
	}{
		{
			subTest: "NodePorts enabled in config, disabled in manifest",
			config: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				EnableMasterNodePort:        true,
				EnableMasterPoolerNodePort:  true,
				EnableReplicaNodePort:       true,
				EnableReplicaPoolerNodePort: true,
				ExternalTrafficPolicy:       extTrafficPolicy,
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: clusterNameLabel,
					PodRoleLabel:     roleLabel,
				},
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler:        util.True(),
					EnableReplicaConnectionPooler: util.True(),
					EnableMasterNodePort:          util.False(),
					EnableMasterPoolerNodePort:    util.False(),
					EnableReplicaNodePort:         util.False(),
					EnableReplicaPoolerNodePort:   util.False(),
					NumberOfInstances:             1,
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedServices: getServices(v1.ServiceTypeClusterIP, nil, "", clusterName, 0),
		},
		{
			subTest: "NodePorts configured in manifest, disabled in config",
			config: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				EnableMasterNodePort:        false,
				EnableMasterPoolerNodePort:  false,
				EnableReplicaNodePort:       false,
				EnableReplicaPoolerNodePort: false,
				ExternalTrafficPolicy:       extTrafficPolicy,
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: clusterNameLabel,
					PodRoleLabel:     roleLabel,
				},
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler:        util.True(),
					EnableReplicaConnectionPooler: util.True(),
					EnableMasterNodePort:          util.True(),
					EnableMasterPoolerNodePort:    util.True(),
					EnableReplicaNodePort:         util.True(),
					EnableReplicaPoolerNodePort:   util.True(),
					NumberOfInstances:             1,
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedServices: getServices(v1.ServiceTypeNodePort, nil, extTrafficPolicy, clusterName, 0),
		},
		{
			subTest: "NodePorts configured in manifest, disabled in config, custom port specified",
			config: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				EnableMasterNodePort:        false,
				EnableMasterPoolerNodePort:  false,
				EnableReplicaNodePort:       false,
				EnableReplicaPoolerNodePort: false,
				ExternalTrafficPolicy:       extTrafficPolicy,
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: clusterNameLabel,
					PodRoleLabel:     roleLabel,
				},
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler:        util.True(),
					EnableReplicaConnectionPooler: util.True(),
					EnableMasterNodePort:          util.True(),
					MasterNodePort:                &port,
					EnableMasterPoolerNodePort:    util.True(),
					MasterPoolerNodePort:          &port,
					EnableReplicaNodePort:         util.True(),
					ReplicaNodePort:               &port,
					EnableReplicaPoolerNodePort:   util.True(),
					ReplicaPoolerNodePort:         &port,
					NumberOfInstances:             1,
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedServices: getServices(v1.ServiceTypeNodePort, nil, extTrafficPolicy, clusterName, port),
		},
	}

	for _, tt := range tests {
		client, _ := newLBFakeClient()

		var cluster = New(
			Config{
				OpConfig: tt.config,
			}, client, tt.pgSpec, logger, eventRecorder)

		cluster.Name = clusterName
		cluster.Namespace = namespace
		cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{}
		generatedServices := make([]v1.ServiceSpec, 0)
		for _, role := range roles {
			cluster.syncService(role)
			cluster.ConnectionPooler[role] = &ConnectionPoolerObjects{
				Name:        cluster.connectionPoolerName(role),
				ClusterName: cluster.Name,
				Namespace:   cluster.Namespace,
				Role:        role,
			}
			cluster.syncConnectionPoolerWorker(&tt.pgSpec, &tt.pgSpec, role)
			generatedServices = append(generatedServices, cluster.Services[role].Spec)
			generatedServices = append(generatedServices, cluster.ConnectionPooler[role].Service.Spec)
		}
		if !reflect.DeepEqual(tt.expectedServices, generatedServices) {
			t.Errorf("%s %s: expected %#v but got %#v", t.Name(), tt.subTest, tt.expectedServices, generatedServices)
		}
	}
}

func TestGenerateResourceRequirements(t *testing.T) {
	client, _ := newFakeK8sTestClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	clusterNameLabel := "cluster-name"
	sidecarName := "postgres-exporter"
	dockerImage := "test-image"

	// enforceMinResourceLimits will be called 2 times emitting 4 events (2x cpu, 2x memory raise)
	// enforceMaxResourceRequests will be called 4 times emitting 6 events (2x cpu, 4x memory cap)
	// hence event bufferSize of 10 is required
	newEventRecorder := record.NewFakeRecorder(10)

	configResources := config.Resources{
		ClusterLabels:        map[string]string{"application": "spilo"},
		ClusterNameLabel:     clusterNameLabel,
		DefaultCPURequest:    "100m",
		DefaultCPULimit:      "1",
		MaxCPURequest:        "500m",
		MinCPULimit:          "250m",
		DefaultMemoryRequest: "100Mi",
		DefaultMemoryLimit:   "500Mi",
		MaxMemoryRequest:     "1Gi",
		MinMemoryLimit:       "250Mi",
		PodRoleLabel:         "spilo-role",
	}

	configWithEnabledIgnoreResourcesLimits := configResources
	configWithEnabledIgnoreResourcesLimits.IgnoreResourcesLimitsAnnotationKey = "zalando.org/ignore-resources-limits"

	tests := []struct {
		subTest           string
		config            config.Config
		pgSpec            acidv1.Postgresql
		expectedResources acidv1.Resources
	}{
		{
			subTest: "test generation of default resources when empty in manifest",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("500Mi")},
			},
		},
		{
			subTest: "test generation of default resources for sidecar",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Sidecars: []acidv1.Sidecar{
						{
							Name:        sidecarName,
							DockerImage: dockerImage,
						},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("500Mi")},
			},
		},
		{
			subTest: "test generation of resources when only requests are defined in manifest",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("50m"), Memory: k8sutil.StringToPointer("50Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("50m"), Memory: k8sutil.StringToPointer("50Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("500Mi")},
			},
		},
		{
			subTest: "test generation of resources when only memory is defined in manifest",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("100Mi")},
						ResourceLimits:   acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("1Gi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("1Gi")},
			},
		},
		{
			subTest: "test generation of resources when default is not defined",
			config: config.Config{
				Resources: config.Resources{
					ClusterLabels:        map[string]string{"application": "spilo"},
					ClusterNameLabel:     clusterNameLabel,
					DefaultCPURequest:    "100m",
					DefaultMemoryRequest: "100Mi",
					PodRoleLabel:         "spilo-role",
				},
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
			},
		},
		{
			subTest: "test generation of resources when min limits are all set to zero",
			config: config.Config{
				Resources: config.Resources{
					ClusterLabels:        map[string]string{"application": "spilo"},
					ClusterNameLabel:     clusterNameLabel,
					DefaultCPURequest:    "0",
					DefaultCPULimit:      "0",
					MaxCPURequest:        "0",
					MinCPULimit:          "0",
					DefaultMemoryRequest: "0",
					DefaultMemoryLimit:   "0",
					MaxMemoryRequest:     "0",
					MinMemoryLimit:       "0",
					PodRoleLabel:         "spilo-role",
				},
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceLimits: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("5m"), Memory: k8sutil.StringToPointer("5Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceLimits: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("5m"), Memory: k8sutil.StringToPointer("5Mi")},
			},
		},
		{
			subTest: "test matchLimitsWithRequestsIfSmaller",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("750Mi")},
						ResourceLimits:   acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("300Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("750Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("750Mi")},
			},
		},
		{
			subTest: "defaults are not defined but minimum limit is",
			config: config.Config{
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: clusterNameLabel,
					MinMemoryLimit:   "250Mi",
					PodRoleLabel:     "spilo-role",
				},
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("500Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("500Mi")},
				ResourceLimits:   acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("500Mi")},
			},
		},
		{
			subTest: "test SetMemoryRequestToLimit flag",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: true,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("200Mi")},
						ResourceLimits:   acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("300Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("300Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("300Mi")},
			},
		},
		{
			subTest: "test SetMemoryRequestToLimit flag for sidecar container, too",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: true,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Sidecars: []acidv1.Sidecar{
						{
							Name:        sidecarName,
							DockerImage: dockerImage,
							Resources: &acidv1.Resources{
								ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("10Mi")},
								ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
							},
						},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
			},
		},
		{
			subTest: "test generating resources from manifest",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("250Mi")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("400m"), Memory: k8sutil.StringToPointer("800Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("250Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("400m"), Memory: k8sutil.StringToPointer("800Mi")},
			},
		},
		{
			subTest: "test enforcing min cpu and memory limit",
			config: config.Config{
				Resources:               configWithEnabledIgnoreResourcesLimits,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:        clusterName,
					Namespace:   namespace,
					Annotations: map[string]string{"zalando.org/ignore-resources-limits": "false"},
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("200m"), Memory: k8sutil.StringToPointer("200Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("250m"), Memory: k8sutil.StringToPointer("250Mi")},
			},
		},
		{
			subTest: "ingnore min cpu and memory limit threshold",
			config: config.Config{
				Resources:               configWithEnabledIgnoreResourcesLimits,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:        clusterName,
					Namespace:   namespace,
					Annotations: map[string]string{"zalando.org/ignore-resources-limits": "true"},
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("200m"), Memory: k8sutil.StringToPointer("200Mi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("200m"), Memory: k8sutil.StringToPointer("200Mi")},
			},
		},
		{
			subTest: "test min cpu and memory limit are not enforced on sidecar",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Sidecars: []acidv1.Sidecar{
						{
							Name:        sidecarName,
							DockerImage: dockerImage,
							Resources: &acidv1.Resources{
								ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("10Mi")},
								ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
							},
						},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("10Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
			},
		},
		{
			subTest: "test enforcing max cpu and memory requests",
			config: config.Config{
				Resources:               configWithEnabledIgnoreResourcesLimits,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:        clusterName,
					Namespace:   namespace,
					Annotations: map[string]string{"zalando.org/ignore-resources-limits": "yes"},
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("2Gi")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("2"), Memory: k8sutil.StringToPointer("4Gi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("500m"), Memory: k8sutil.StringToPointer("1Gi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("2"), Memory: k8sutil.StringToPointer("4Gi")},
			},
		},
		{
			subTest: "ignore max cpu and memory requests limit",
			config: config.Config{
				Resources:               configWithEnabledIgnoreResourcesLimits,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:        clusterName,
					Namespace:   namespace,
					Annotations: map[string]string{"zalando.org/ignore-resources-limits": "true"},
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("2Gi")},
						ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("2"), Memory: k8sutil.StringToPointer("4Gi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("2Gi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("2"), Memory: k8sutil.StringToPointer("4Gi")},
			},
		},
		{
			subTest: "test SetMemoryRequestToLimit flag but raise only until max memory request",
			config: config.Config{
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: true,
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("500Mi")},
						ResourceLimits:   acidv1.ResourceDescription{Memory: k8sutil.StringToPointer("2Gi")},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("1Gi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("2Gi")},
			},
		},
		{
			subTest: "test HugePages are not set on container when not requested in manifest",
			config: config.Config{
				Resources:           configResources,
				PodManagementPolicy: "ordered_ready",
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{},
						ResourceLimits:   acidv1.ResourceDescription{},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{
					CPU:    k8sutil.StringToPointer("100m"),
					Memory: k8sutil.StringToPointer("100Mi"),
				},
				ResourceLimits: acidv1.ResourceDescription{
					CPU:    k8sutil.StringToPointer("1"),
					Memory: k8sutil.StringToPointer("500Mi"),
				},
			},
		},
		{
			subTest: "test HugePages are passed through to the postgres container",
			config: config.Config{
				Resources:           configResources,
				PodManagementPolicy: "ordered_ready",
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Resources: &acidv1.Resources{
						ResourceRequests: acidv1.ResourceDescription{
							HugePages2Mi: k8sutil.StringToPointer("128Mi"),
							HugePages1Gi: k8sutil.StringToPointer("1Gi"),
						},
						ResourceLimits: acidv1.ResourceDescription{
							HugePages2Mi: k8sutil.StringToPointer("256Mi"),
							HugePages1Gi: k8sutil.StringToPointer("2Gi"),
						},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{
					CPU:          k8sutil.StringToPointer("100m"),
					Memory:       k8sutil.StringToPointer("100Mi"),
					HugePages2Mi: k8sutil.StringToPointer("128Mi"),
					HugePages1Gi: k8sutil.StringToPointer("1Gi"),
				},
				ResourceLimits: acidv1.ResourceDescription{
					CPU:          k8sutil.StringToPointer("1"),
					Memory:       k8sutil.StringToPointer("500Mi"),
					HugePages2Mi: k8sutil.StringToPointer("256Mi"),
					HugePages1Gi: k8sutil.StringToPointer("2Gi"),
				},
			},
		},
		{
			subTest: "test HugePages are passed through on sidecars",
			config: config.Config{
				Resources:           configResources,
				PodManagementPolicy: "ordered_ready",
			},
			pgSpec: acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:      clusterName,
					Namespace: namespace,
				},
				Spec: acidv1.PostgresSpec{
					Sidecars: []acidv1.Sidecar{
						{
							Name:        "test-sidecar",
							DockerImage: "test-image",
							Resources: &acidv1.Resources{
								ResourceRequests: acidv1.ResourceDescription{
									HugePages2Mi: k8sutil.StringToPointer("128Mi"),
									HugePages1Gi: k8sutil.StringToPointer("1Gi"),
								},
								ResourceLimits: acidv1.ResourceDescription{
									HugePages2Mi: k8sutil.StringToPointer("256Mi"),
									HugePages1Gi: k8sutil.StringToPointer("2Gi"),
								},
							},
						},
					},
					TeamID: "acid",
					Volume: acidv1.Volume{
						Size: "1G",
					},
				},
			},
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{
					CPU:          k8sutil.StringToPointer("100m"),
					Memory:       k8sutil.StringToPointer("100Mi"),
					HugePages2Mi: k8sutil.StringToPointer("128Mi"),
					HugePages1Gi: k8sutil.StringToPointer("1Gi"),
				},
				ResourceLimits: acidv1.ResourceDescription{
					CPU:          k8sutil.StringToPointer("1"),
					Memory:       k8sutil.StringToPointer("500Mi"),
					HugePages2Mi: k8sutil.StringToPointer("256Mi"),
					HugePages1Gi: k8sutil.StringToPointer("2Gi"),
				},
			},
		},
	}

	for _, tt := range tests {
		var cluster = New(
			Config{
				OpConfig: tt.config,
			}, client, tt.pgSpec, logger, newEventRecorder)

		cluster.Name = clusterName
		cluster.Namespace = namespace
		_, err := cluster.createStatefulSet()
		if k8sutil.ResourceAlreadyExists(err) {
			err = cluster.syncStatefulSet()
		}
		assert.NoError(t, err)

		containers := cluster.Statefulset.Spec.Template.Spec.Containers
		clusterResources, err := parseResourceRequirements(containers[0].Resources)
		if len(containers) > 1 {
			clusterResources, err = parseResourceRequirements(containers[1].Resources)
		}
		assert.NoError(t, err)
		if !reflect.DeepEqual(tt.expectedResources, clusterResources) {
			t.Errorf("%s - %s: expected %#v but got %#v", t.Name(), tt.subTest, tt.expectedResources, clusterResources)
		}
	}
}

func TestGenerateLogicalBackupJob(t *testing.T) {
	clusterName := "acid-test-cluster"
	teamId := "test"
	configResources := config.Resources{
		ClusterNameLabel:     "cluster-name",
		DefaultCPURequest:    "100m",
		DefaultCPULimit:      "1",
		DefaultMemoryRequest: "100Mi",
		DefaultMemoryLimit:   "500Mi",
	}

	tests := []struct {
		subTest            string
		config             config.Config
		specSchedule       string
		expectedSchedule   string
		expectedJobName    string
		expectedResources  acidv1.Resources
		expectedAnnotation map[string]string
		expectedLabel      map[string]string
	}{
		{
			subTest: "test generation of logical backup pod resources when not configured",
			config: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupJobPrefix: "logical-backup-",
					LogicalBackupSchedule:  "30 00 * * *",
				},
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			specSchedule:     "",
			expectedSchedule: "30 00 * * *",
			expectedJobName:  "logical-backup-acid-test-cluster",
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("500Mi")},
			},
			expectedLabel:      map[string]string{"application": "spilo-logical-backup", configResources.ClusterNameLabel: clusterName, "team": teamId},
			expectedAnnotation: nil,
		},
		{
			subTest: "test generation of logical backup pod resources when configured",
			config: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupCPURequest:    "10m",
					LogicalBackupCPULimit:      "300m",
					LogicalBackupMemoryRequest: "50Mi",
					LogicalBackupMemoryLimit:   "300Mi",
					LogicalBackupJobPrefix:     "lb-",
					LogicalBackupSchedule:      "30 00 * * *",
				},
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			specSchedule:     "30 00 * * 7",
			expectedSchedule: "30 00 * * 7",
			expectedJobName:  "lb-acid-test-cluster",
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("10m"), Memory: k8sutil.StringToPointer("50Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("300m"), Memory: k8sutil.StringToPointer("300Mi")},
			},
			expectedLabel:      map[string]string{"application": "spilo-logical-backup", configResources.ClusterNameLabel: clusterName, "team": teamId},
			expectedAnnotation: nil,
		},
		{
			subTest: "test generation of logical backup pod resources when partly configured",
			config: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupCPURequest: "50m",
					LogicalBackupCPULimit:   "250m",
					LogicalBackupJobPrefix:  "",
					LogicalBackupSchedule:   "30 00 * * *",
				},
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: false,
			},
			specSchedule:     "",
			expectedSchedule: "30 00 * * *",
			expectedJobName:  "acid-test-cluster",
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("50m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("250m"), Memory: k8sutil.StringToPointer("500Mi")},
			},
			expectedLabel:      map[string]string{"application": "spilo-logical-backup", configResources.ClusterNameLabel: clusterName, "team": teamId},
			expectedAnnotation: nil,
		},
		{
			subTest: "test generation of logical backup pod resources with SetMemoryRequestToLimit enabled",
			config: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupMemoryRequest: "80Mi",
					LogicalBackupMemoryLimit:   "200Mi",
					LogicalBackupJobPrefix:     "test-long-prefix-so-name-must-be-trimmed-",
					LogicalBackupSchedule:      "30 00 * * *",
				},
				Resources:               configResources,
				PodManagementPolicy:     "ordered_ready",
				SetMemoryRequestToLimit: true,
			},
			specSchedule:     "",
			expectedSchedule: "30 00 * * *",
			expectedJobName:  "test-long-prefix-so-name-must-be-trimmed-acid-test-c",
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("200Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("200Mi")},
			},
			expectedLabel:      map[string]string{"application": "spilo-logical-backup", configResources.ClusterNameLabel: clusterName, "team": teamId},
			expectedAnnotation: nil,
		},
		{
			subTest: "test generation of pod annotations when cluster InheritedLabel is set",
			config: config.Config{
				Resources: config.Resources{
					ClusterNameLabel:     "cluster-name",
					InheritedLabels:      []string{"labelKey"},
					DefaultCPURequest:    "100m",
					DefaultCPULimit:      "1",
					DefaultMemoryRequest: "100Mi",
					DefaultMemoryLimit:   "500Mi",
				},
			},
			specSchedule:     "",
			expectedJobName:  "acid-test-cluster",
			expectedSchedule: "",
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("500Mi")},
			},
			expectedLabel:      map[string]string{"application": "spilo-logical-backup", "labelKey": "labelValue", "cluster-name": clusterName, "team": teamId},
			expectedAnnotation: nil,
		},
		{
			subTest: "test generation of pod annotations when cluster InheritedAnnotations is set",
			config: config.Config{
				Resources: config.Resources{
					ClusterNameLabel:     "cluster-name",
					InheritedAnnotations: []string{"annotationKey"},
					DefaultCPURequest:    "100m",
					DefaultCPULimit:      "1",
					DefaultMemoryRequest: "100Mi",
					DefaultMemoryLimit:   "500Mi",
				},
			},
			specSchedule:     "",
			expectedJobName:  "acid-test-cluster",
			expectedSchedule: "",
			expectedResources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("100m"), Memory: k8sutil.StringToPointer("100Mi")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("500Mi")},
			},
			expectedLabel:      map[string]string{"application": "spilo-logical-backup", configResources.ClusterNameLabel: clusterName, "team": teamId},
			expectedAnnotation: map[string]string{"annotationKey": "annotationValue"},
		},
	}

	for _, tt := range tests {
		var cluster = New(
			Config{
				OpConfig: tt.config,
			}, k8sutil.NewMockKubernetesClient(), acidv1.Postgresql{}, logger, eventRecorder)
		cluster.ObjectMeta.Name = clusterName
		cluster.Spec.TeamID = teamId
		if cluster.ObjectMeta.Labels == nil {
			cluster.ObjectMeta.Labels = make(map[string]string)
		}
		if cluster.ObjectMeta.Annotations == nil {
			cluster.ObjectMeta.Annotations = make(map[string]string)
		}
		cluster.ObjectMeta.Labels["labelKey"] = "labelValue"
		cluster.ObjectMeta.Annotations["annotationKey"] = "annotationValue"
		cluster.Spec.LogicalBackupSchedule = tt.specSchedule
		cronJob, err := cluster.generateLogicalBackupJob()
		assert.NoError(t, err)

		if !reflect.DeepEqual(cronJob.ObjectMeta.OwnerReferences, cluster.ownerReferences()) {
			t.Errorf("%s - %s: expected owner references %#v, got %#v", t.Name(), tt.subTest, cluster.ownerReferences(), cronJob.ObjectMeta.OwnerReferences)
		}

		if cronJob.Spec.Schedule != tt.expectedSchedule {
			t.Errorf("%s - %s: expected schedule %s, got %s", t.Name(), tt.subTest, tt.expectedSchedule, cronJob.Spec.Schedule)
		}

		if cronJob.Name != tt.expectedJobName {
			t.Errorf("%s - %s: expected job name %s, got %s", t.Name(), tt.subTest, tt.expectedJobName, cronJob.Name)
		}

		if !reflect.DeepEqual(cronJob.Labels, tt.expectedLabel) {
			t.Errorf("%s - %s: expected labels %s, got %s", t.Name(), tt.subTest, tt.expectedLabel, cronJob.Labels)
		}

		if !reflect.DeepEqual(cronJob.Annotations, tt.expectedAnnotation) {
			t.Errorf("%s - %s: expected annotations %s, got %s", t.Name(), tt.subTest, tt.expectedAnnotation, cronJob.Annotations)
		}

		containers := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers
		clusterResources, err := parseResourceRequirements(containers[0].Resources)
		assert.NoError(t, err)
		if !reflect.DeepEqual(tt.expectedResources, clusterResources) {
			t.Errorf("%s - %s: expected resources %#v, got %#v", t.Name(), tt.subTest, tt.expectedResources, clusterResources)
		}

		expectedSuccessfulJobsHistoryLimit := int32(3)
		if cluster.OpConfig.LogicalBackup.LogicalBackupSuccessfulJobsHistoryLimit != nil {
			expectedSuccessfulJobsHistoryLimit = *cluster.OpConfig.LogicalBackup.LogicalBackupSuccessfulJobsHistoryLimit
		}
		if *cronJob.Spec.SuccessfulJobsHistoryLimit != expectedSuccessfulJobsHistoryLimit {
			t.Errorf("%s - %s: expected successfulJobsHistoryLimit %d, got %d", t.Name(), tt.subTest, expectedSuccessfulJobsHistoryLimit, *cronJob.Spec.SuccessfulJobsHistoryLimit)
		}

		expectedFailedJobsHistoryLimit := int32(3)
		if cluster.OpConfig.LogicalBackup.LogicalBackupFailedJobsHistoryLimit != nil {
			expectedFailedJobsHistoryLimit = *cluster.OpConfig.LogicalBackup.LogicalBackupFailedJobsHistoryLimit
		}
		if *cronJob.Spec.FailedJobsHistoryLimit != expectedFailedJobsHistoryLimit {
			t.Errorf("%s - %s: expected failedJobsHistoryLimit %d, got %d", t.Name(), tt.subTest, expectedFailedJobsHistoryLimit, *cronJob.Spec.FailedJobsHistoryLimit)
		}

		expectedTTL := int32(86400)
		if cluster.OpConfig.LogicalBackup.LogicalBackupTTLSecondsAfterFinished != nil {
			expectedTTL = *cluster.OpConfig.LogicalBackup.LogicalBackupTTLSecondsAfterFinished
		}
		if cronJob.Spec.JobTemplate.Spec.TTLSecondsAfterFinished == nil {
			t.Errorf("%s - %s: expected TTLSecondsAfterFinished to be set", t.Name(), tt.subTest)
		} else if *cronJob.Spec.JobTemplate.Spec.TTLSecondsAfterFinished != expectedTTL {
			t.Errorf("%s - %s: expected TTLSecondsAfterFinished %d, got %d", t.Name(), tt.subTest, expectedTTL, *cronJob.Spec.JobTemplate.Spec.TTLSecondsAfterFinished)
		}
	}
}

func TestGenerateLogicalBackupPodEnvVars(t *testing.T) {
	var (
		dummyUUID   = "efd12e58-5786-11e8-b5a7-06148230260c"
		dummyBucket = "dummy-backup-location"
	)

	expectedLogicalBackupS3Bucket := []ExpectedValue{
		{
			envIndex:       9,
			envVarConstant: "LOGICAL_BACKUP_PROVIDER",
			envVarValue:    "s3",
		},
		{
			envIndex:       10,
			envVarConstant: "LOGICAL_BACKUP_S3_BUCKET",
			envVarValue:    dummyBucket,
		},
		{
			envIndex:       11,
			envVarConstant: "LOGICAL_BACKUP_S3_BUCKET_PREFIX",
			envVarValue:    "spilo",
		},
		{
			envIndex:       12,
			envVarConstant: "LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX",
			envVarValue:    "/" + dummyUUID,
		},
		{
			envIndex:       13,
			envVarConstant: "LOGICAL_BACKUP_S3_REGION",
			envVarValue:    "eu-central-1",
		},
		{
			envIndex:       14,
			envVarConstant: "LOGICAL_BACKUP_S3_ENDPOINT",
			envVarValue:    "",
		},
		{
			envIndex:       15,
			envVarConstant: "LOGICAL_BACKUP_S3_SSE",
			envVarValue:    "",
		},
		{
			envIndex:       16,
			envVarConstant: "LOGICAL_BACKUP_S3_RETENTION_TIME",
			envVarValue:    "1 month",
		},
	}

	expectedLogicalBackupGCPCreds := []ExpectedValue{
		{
			envIndex:       9,
			envVarConstant: "LOGICAL_BACKUP_PROVIDER",
			envVarValue:    "gcs",
		},
		{
			envIndex:       13,
			envVarConstant: "LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS",
			envVarValue:    "some-path-to-credentials",
		},
	}

	expectedLogicalBackupAzureStorage := []ExpectedValue{
		{
			envIndex:       9,
			envVarConstant: "LOGICAL_BACKUP_PROVIDER",
			envVarValue:    "az",
		},
		{
			envIndex:       13,
			envVarConstant: "LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_NAME",
			envVarValue:    "some-azure-storage-account-name",
		},
		{
			envIndex:       14,
			envVarConstant: "LOGICAL_BACKUP_AZURE_STORAGE_CONTAINER",
			envVarValue:    "some-azure-storage-container",
		},
		{
			envIndex:       15,
			envVarConstant: "LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_KEY",
			envVarValue:    "some-azure-storage-account-key",
		},
	}

	expectedLogicalBackupRetentionTime := []ExpectedValue{
		{
			envIndex:       16,
			envVarConstant: "LOGICAL_BACKUP_S3_RETENTION_TIME",
			envVarValue:    "3 months",
		},
	}

	tests := []struct {
		subTest        string
		opConfig       config.Config
		expectedValues []ExpectedValue
		pgsql          acidv1.Postgresql
	}{
		{
			subTest: "logical backup with provider: s3",
			opConfig: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupProvider:        "s3",
					LogicalBackupS3Bucket:        dummyBucket,
					LogicalBackupS3BucketPrefix:  "spilo",
					LogicalBackupS3Region:        "eu-central-1",
					LogicalBackupS3RetentionTime: "1 month",
				},
			},
			expectedValues: expectedLogicalBackupS3Bucket,
		},
		{
			subTest: "logical backup with provider: gcs",
			opConfig: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupProvider:                     "gcs",
					LogicalBackupS3Bucket:                     dummyBucket,
					LogicalBackupGoogleApplicationCredentials: "some-path-to-credentials",
				},
			},
			expectedValues: expectedLogicalBackupGCPCreds,
		},
		{
			subTest: "logical backup with provider: az",
			opConfig: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupProvider:                "az",
					LogicalBackupS3Bucket:                dummyBucket,
					LogicalBackupAzureStorageAccountName: "some-azure-storage-account-name",
					LogicalBackupAzureStorageContainer:   "some-azure-storage-container",
					LogicalBackupAzureStorageAccountKey:  "some-azure-storage-account-key",
				},
			},
			expectedValues: expectedLogicalBackupAzureStorage,
		},
		{
			subTest: "will override retention time parameter",
			opConfig: config.Config{
				LogicalBackup: config.LogicalBackup{
					LogicalBackupProvider:        "s3",
					LogicalBackupS3RetentionTime: "1 month",
				},
			},
			expectedValues: expectedLogicalBackupRetentionTime,
			pgsql: acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					LogicalBackupRetention: "3 months",
				},
			},
		},
	}

	for _, tt := range tests {
		c := newMockCluster(tt.opConfig)
		pgsql := tt.pgsql
		c.Postgresql = pgsql
		c.UID = types.UID(dummyUUID)

		actualEnvs := c.generateLogicalBackupPodEnvVars()

		for _, ev := range tt.expectedValues {
			env := actualEnvs[ev.envIndex]

			if env.Name != ev.envVarConstant {
				t.Errorf("%s %s: expected env name %s, have %s instead",
					t.Name(), tt.subTest, ev.envVarConstant, env.Name)
			}

			if ev.envVarValueRef != nil {
				if !reflect.DeepEqual(env.ValueFrom, ev.envVarValueRef) {
					t.Errorf("%s %s: expected env value reference %#v, have %#v instead",
						t.Name(), tt.subTest, ev.envVarValueRef, env.ValueFrom)
				}
				continue
			}

			if env.Value != ev.envVarValue {
				t.Errorf("%s %s: expected env value %s, have %s instead",
					t.Name(), tt.subTest, ev.envVarValue, env.Value)
			}
		}
	}
}

func TestGenerateCapabilities(t *testing.T) {
	tests := []struct {
		subTest      string
		configured   []string
		capabilities *v1.Capabilities
		err          error
	}{
		{
			subTest:      "no capabilities",
			configured:   nil,
			capabilities: nil,
			err:          fmt.Errorf("could not parse capabilities configuration of nil"),
		},
		{
			subTest:      "empty capabilities",
			configured:   []string{},
			capabilities: nil,
			err:          fmt.Errorf("could not parse empty capabilities configuration"),
		},
		{
			subTest:    "configured capability",
			configured: []string{"SYS_NICE"},
			capabilities: &v1.Capabilities{
				Add: []v1.Capability{"SYS_NICE"},
			},
			err: fmt.Errorf("could not generate one configured capability"),
		},
		{
			subTest:    "configured capabilities",
			configured: []string{"SYS_NICE", "CHOWN"},
			capabilities: &v1.Capabilities{
				Add: []v1.Capability{"SYS_NICE", "CHOWN"},
			},
			err: fmt.Errorf("could not generate multiple configured capabilities"),
		},
	}
	for _, tt := range tests {
		caps := generateCapabilities(tt.configured)
		if !reflect.DeepEqual(caps, tt.capabilities) {
			t.Errorf("%s %s: expected `%v` but got `%v`",
				t.Name(), tt.subTest, tt.capabilities, caps)
		}
	}
}

func TestGenerateContainerWithEnvFrom(t *testing.T) {
	dockerImage := "test-image"
	resourceRequirements := &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("100m"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	envVars := []v1.EnvVar{{Name: "TEST_VAR", Value: "test-value"}}
	envFrom := []v1.EnvFromSource{
		{
			ConfigMapRef: &v1.ConfigMapEnvSource{
				LocalObjectReference: v1.LocalObjectReference{Name: "test-configmap"},
			},
		},
		{
			SecretRef: &v1.SecretEnvSource{
				LocalObjectReference: v1.LocalObjectReference{Name: "test-secret"},
			},
		},
	}

	container := generateContainer(
		constants.PostgresContainerName,
		&dockerImage,
		resourceRequirements,
		envVars,
		envFrom,
		[]v1.VolumeMount{},
		false,
		util.False(),
		nil,
		nil,
	)

	if !reflect.DeepEqual(container.Env, envVars) {
		t.Errorf("expected env %v, got %v", envVars, container.Env)
	}
	if !reflect.DeepEqual(container.EnvFrom, envFrom) {
		t.Errorf("expected envFrom %v, got %v", envFrom, container.EnvFrom)
	}
}

func TestTopologySpreadConstraints(t *testing.T) {
	clusterName := "acid-test-cluster"
	namespace := "default"
	labelSelector := &metav1.LabelSelector{
		MatchLabels: cluster.labelsSet(true),
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			NumberOfInstances: 1,
			Resources: &acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
				ResourceLimits:   acidv1.ResourceDescription{CPU: k8sutil.StringToPointer("1"), Memory: k8sutil.StringToPointer("10")},
			},
			Volume: acidv1.Volume{
				Size: "1G",
			},
			TopologySpreadConstraints: []v1.TopologySpreadConstraint{
				{
					MaxSkew:           1,
					TopologyKey:       "topology.kubernetes.io/zone",
					WhenUnsatisfiable: v1.DoNotSchedule,
					LabelSelector:     labelSelector,
				},
			},
		},
	}

	cluster := New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)
	cluster.Name = clusterName
	cluster.Namespace = namespace
	cluster.labelsSet(true)

	s, err := cluster.generateStatefulSet(&pg.Spec)
	assert.NoError(t, err)
	assert.Contains(t, s.Spec.Template.Spec.TopologySpreadConstraints, v1.TopologySpreadConstraint{
		MaxSkew:           int32(1),
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: v1.DoNotSchedule,
		LabelSelector:     labelSelector,
	},
	)
}
