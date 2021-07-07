package cluster

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"testing"

	"github.com/stretchr/testify/assert"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
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
}

func toIntStr(val int) *intstr.IntOrString {
	b := intstr.FromInt(val)
	return &b
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

	testName := "TestGenerateSpiloConfig"
	tests := []struct {
		subtest  string
		pgParam  *acidv1.PostgresqlParam
		patroni  *acidv1.Patroni
		role     string
		opConfig config.Config
		result   string
	}{
		{
			subtest:  "Patroni default configuration",
			pgParam:  &acidv1.PostgresqlParam{PgVersion: "9.6"},
			patroni:  &acidv1.Patroni{},
			role:     "zalandos",
			opConfig: config.Config{},
			result:   `{"postgresql":{"bin_dir":"/usr/lib/postgresql/9.6/bin"},"bootstrap":{"initdb":[{"auth-host":"md5"},{"auth-local":"trust"}],"users":{"zalandos":{"password":"","options":["CREATEDB","NOLOGIN"]}},"dcs":{}}}`,
		},
		{
			subtest: "Patroni configured",
			pgParam: &acidv1.PostgresqlParam{PgVersion: "11"},
			patroni: &acidv1.Patroni{
				InitDB: map[string]string{
					"encoding":       "UTF8",
					"locale":         "en_US.UTF-8",
					"data-checksums": "true",
				},
				PgHba:                 []string{"hostssl all all 0.0.0.0/0 md5", "host    all all 0.0.0.0/0 md5"},
				TTL:                   30,
				LoopWait:              10,
				RetryTimeout:          10,
				MaximumLagOnFailover:  33554432,
				SynchronousMode:       true,
				SynchronousModeStrict: true,
				Slots:                 map[string]map[string]string{"permanent_logical_1": {"type": "logical", "database": "foo", "plugin": "pgoutput"}},
			},
			role:     "zalandos",
			opConfig: config.Config{},
			result:   `{"postgresql":{"bin_dir":"/usr/lib/postgresql/11/bin","pg_hba":["hostssl all all 0.0.0.0/0 md5","host    all all 0.0.0.0/0 md5"]},"bootstrap":{"initdb":[{"auth-host":"md5"},{"auth-local":"trust"},"data-checksums",{"encoding":"UTF8"},{"locale":"en_US.UTF-8"}],"users":{"zalandos":{"password":"","options":["CREATEDB","NOLOGIN"]}},"dcs":{"ttl":30,"loop_wait":10,"retry_timeout":10,"maximum_lag_on_failover":33554432,"synchronous_mode":true,"synchronous_mode_strict":true,"slots":{"permanent_logical_1":{"database":"foo","plugin":"pgoutput","type":"logical"}}}}}`,
		},
	}
	for _, tt := range tests {
		cluster.OpConfig = tt.opConfig
		result, err := generateSpiloJSONConfiguration(tt.pgParam, tt.patroni, tt.role, false, logger)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if tt.result != result {
			t.Errorf("%s %s: Spilo Config is %v, expected %v for role %#v and param %#v",
				testName, tt.subtest, result, tt.result, tt.role, tt.pgParam)
		}
	}
}

func TestGenerateSpiloPodEnvVars(t *testing.T) {
	var cluster = New(
		Config{
			OpConfig: config.Config{
				WALGSBucket:    "wale-gs-bucket",
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	expectedValuesGSBucket := []ExpectedValue{
		ExpectedValue{
			envIndex:       15,
			envVarConstant: "WAL_GS_BUCKET",
			envVarValue:    "wale-gs-bucket",
		},
		ExpectedValue{
			envIndex:       16,
			envVarConstant: "WAL_BUCKET_SCOPE_SUFFIX",
			envVarValue:    "/SomeUUID",
		},
		ExpectedValue{
			envIndex:       17,
			envVarConstant: "WAL_BUCKET_SCOPE_PREFIX",
			envVarValue:    "",
		},
	}

	expectedValuesGCPCreds := []ExpectedValue{
		ExpectedValue{
			envIndex:       15,
			envVarConstant: "WAL_GS_BUCKET",
			envVarValue:    "wale-gs-bucket",
		},
		ExpectedValue{
			envIndex:       16,
			envVarConstant: "WAL_BUCKET_SCOPE_SUFFIX",
			envVarValue:    "/SomeUUID",
		},
		ExpectedValue{
			envIndex:       17,
			envVarConstant: "WAL_BUCKET_SCOPE_PREFIX",
			envVarValue:    "",
		},
		ExpectedValue{
			envIndex:       18,
			envVarConstant: "GOOGLE_APPLICATION_CREDENTIALS",
			envVarValue:    "some_path_to_credentials",
		},
	}

	testName := "TestGenerateSpiloPodEnvVars"
	tests := []struct {
		subTest            string
		opConfig           config.Config
		uid                types.UID
		spiloConfig        string
		cloneDescription   *acidv1.CloneDescription
		standbyDescription *acidv1.StandbyDescription
		customEnvList      []v1.EnvVar
		expectedValues     []ExpectedValue
		enableWALArchiving bool
	}{
		{
			subTest: "Will set WAL_GS_BUCKET env",
			opConfig: config.Config{
				WALGSBucket: "wale-gs-bucket",
			},
			uid:                "SomeUUID",
			spiloConfig:        "someConfig",
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			customEnvList:      []v1.EnvVar{},
			expectedValues:     expectedValuesGSBucket,
			enableWALArchiving: true,
		},
		{
			subTest: "Will set GOOGLE_APPLICATION_CREDENTIALS env",
			opConfig: config.Config{
				WALGSBucket:    "wale-gs-bucket",
				GCPCredentials: "some_path_to_credentials",
			},
			uid:                "SomeUUID",
			spiloConfig:        "someConfig",
			cloneDescription:   &acidv1.CloneDescription{},
			standbyDescription: &acidv1.StandbyDescription{},
			customEnvList:      []v1.EnvVar{},
			expectedValues:     expectedValuesGCPCreds,
			enableWALArchiving: true,
		},
	}

	for _, tt := range tests {
		cluster.OpConfig = tt.opConfig

		actualEnvs := cluster.generateSpiloPodEnvVars(tt.uid, tt.spiloConfig, tt.cloneDescription, tt.standbyDescription, tt.enableWALArchiving, tt.customEnvList)

		for _, ev := range tt.expectedValues {
			env := actualEnvs[ev.envIndex]

			if env.Name != ev.envVarConstant {
				t.Errorf("%s %s: Expected env name %s, have %s instead",
					testName, tt.subTest, ev.envVarConstant, env.Name)
			}

			if env.Value != ev.envVarValue {
				t.Errorf("%s %s: Expected env value %s, have %s instead",
					testName, tt.subTest, ev.envVarValue, env.Value)
			}
		}
	}
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

	testName := "TestCreateLoadBalancerLogic"
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
				testName, tt.subtest, result, tt.result, tt.role, tt.spec)
		}
	}
}

func TestGeneratePodDisruptionBudget(t *testing.T) {
	tests := []struct {
		c   *Cluster
		out policyv1beta1.PodDisruptionBudget
	}{
		// With multiple instances.
		{
			New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb"}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			policyv1beta1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "postgres-myapp-database-pdb",
					Namespace: "myapp",
					Labels:    map[string]string{"team": "myapp", "cluster-name": "myapp-database"},
				},
				Spec: policyv1beta1.PodDisruptionBudgetSpec{
					MinAvailable: toIntStr(1),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"spilo-role": "master", "cluster-name": "myapp-database"},
					},
				},
			},
		},
		// With zero instances.
		{
			New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb"}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 0}},
				logger,
				eventRecorder),
			policyv1beta1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "postgres-myapp-database-pdb",
					Namespace: "myapp",
					Labels:    map[string]string{"team": "myapp", "cluster-name": "myapp-database"},
				},
				Spec: policyv1beta1.PodDisruptionBudgetSpec{
					MinAvailable: toIntStr(0),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"spilo-role": "master", "cluster-name": "myapp-database"},
					},
				},
			},
		},
		// With PodDisruptionBudget disabled.
		{
			New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-pdb", EnablePodDisruptionBudget: util.False()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			policyv1beta1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "postgres-myapp-database-pdb",
					Namespace: "myapp",
					Labels:    map[string]string{"team": "myapp", "cluster-name": "myapp-database"},
				},
				Spec: policyv1beta1.PodDisruptionBudgetSpec{
					MinAvailable: toIntStr(0),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"spilo-role": "master", "cluster-name": "myapp-database"},
					},
				},
			},
		},
		// With non-default PDBNameFormat and PodDisruptionBudget explicitly enabled.
		{
			New(
				Config{OpConfig: config.Config{Resources: config.Resources{ClusterNameLabel: "cluster-name", PodRoleLabel: "spilo-role"}, PDBNameFormat: "postgres-{cluster}-databass-budget", EnablePodDisruptionBudget: util.True()}},
				k8sutil.KubernetesClient{},
				acidv1.Postgresql{
					ObjectMeta: metav1.ObjectMeta{Name: "myapp-database", Namespace: "myapp"},
					Spec:       acidv1.PostgresSpec{TeamID: "myapp", NumberOfInstances: 3}},
				logger,
				eventRecorder),
			policyv1beta1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "postgres-myapp-database-databass-budget",
					Namespace: "myapp",
					Labels:    map[string]string{"team": "myapp", "cluster-name": "myapp-database"},
				},
				Spec: policyv1beta1.PodDisruptionBudgetSpec{
					MinAvailable: toIntStr(1),
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{"spilo-role": "master", "cluster-name": "myapp-database"},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		result := tt.c.generatePodDisruptionBudget()
		if !reflect.DeepEqual(*result, tt.out) {
			t.Errorf("Expected PodDisruptionBudget: %#v, got %#v", tt.out, *result)
		}
	}
}

func TestShmVolume(t *testing.T) {
	testName := "TestShmVolume"
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
				testName, tt.subTest, constants.ShmVolumeName, volumeName)
		}
		if volumeMountName != constants.ShmVolumeName {
			t.Errorf("%s %s: Expected mount %s was not created, have %s instead",
				testName, tt.subTest, constants.ShmVolumeName, volumeMountName)
		}
	}
}

func TestCloneEnv(t *testing.T) {
	testName := "TestCloneEnv"
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
				testName, tt.subTest, tt.env.Name, env.Name)
		}

		if env.Value != tt.env.Value {
			t.Errorf("%s %s: Expected env value %s, have %s instead",
				testName, tt.subTest, tt.env.Value, env.Value)
		}
	}
}

func TestExtractPgVersionFromBinPath(t *testing.T) {
	testName := "TestExtractPgVersionFromBinPath"
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
			binPath:  "/usr/lib/postgresql/12/bin",
			template: pgBinariesLocationTemplate,
			expected: "12",
		},
		{
			subTest:  "test alternative bin path against a matching template",
			binPath:  "/usr/pgsql-12/bin",
			template: "/usr/pgsql-%v/bin",
			expected: "12",
		},
	}

	for _, tt := range tests {
		pgVersion, err := extractPgVersionFromBinPath(tt.binPath, tt.template)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if pgVersion != tt.expected {
			t.Errorf("%s %s: Expected version %s, have %s instead",
				testName, tt.subTest, tt.expected, pgVersion)
		}
	}
}

func TestSecretVolume(t *testing.T) {
	testName := "TestSecretVolume"
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
				testName, tt.subTest, additionalSecretMount, volumeName)
		}

		for i := range tt.podSpec.Containers {
			volumeMountName := tt.podSpec.Containers[i].VolumeMounts[tt.secretPos].Name

			if volumeMountName != additionalSecretMount {
				t.Errorf("%s %s: Expected mount %s was not created, have %s instead",
					testName, tt.subTest, additionalSecretMount, volumeMountName)
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

const (
	testPodEnvironmentConfigMapName = "pod_env_cm"
	testPodEnvironmentSecretName    = "pod_env_sc"
)

type mockSecret struct {
	v1core.SecretInterface
}

type mockConfigMap struct {
	v1core.ConfigMapInterface
}

func (c *mockSecret) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.Secret, error) {
	if name != testPodEnvironmentSecretName {
		return nil, fmt.Errorf("Secret PodEnvironmentSecret not found")
	}
	secret := &v1.Secret{}
	secret.Name = testPodEnvironmentSecretName
	secret.Data = map[string][]byte{
		"minio_access_key": []byte("alpha"),
		"minio_secret_key": []byte("beta"),
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
		"foo1": "bar1",
		"foo2": "bar2",
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
	}
	return cluster
}

func TestPodEnvironmentConfigMapVariables(t *testing.T) {
	testName := "TestPodEnvironmentConfigMapVariables"
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
						Name: "idonotexist",
					},
				},
			},
			err: fmt.Errorf("could not read PodEnvironmentConfigMap: NotFound"),
		},
		{
			subTest: "simple PodEnvironmentConfigMap",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentConfigMap: spec.NamespacedName{
						Name: testPodEnvironmentConfigMapName,
					},
				},
			},
			envVars: []v1.EnvVar{
				{
					Name:  "foo1",
					Value: "bar1",
				},
				{
					Name:  "foo2",
					Value: "bar2",
				},
			},
		},
	}
	for _, tt := range tests {
		c := newMockCluster(tt.opConfig)
		vars, err := c.getPodEnvironmentConfigMapVariables()
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

// Test if the keys of an existing secret are properly referenced
func TestPodEnvironmentSecretVariables(t *testing.T) {
	testName := "TestPodEnvironmentSecretVariables"
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
					PodEnvironmentSecret: "idonotexist",
				},
			},
			err: fmt.Errorf("could not read Secret PodEnvironmentSecretName: Secret PodEnvironmentSecret not found"),
		},
		{
			subTest: "Pod environment vars reference all keys from secret configured by PodEnvironmentSecret",
			opConfig: config.Config{
				Resources: config.Resources{
					PodEnvironmentSecret: testPodEnvironmentSecretName,
				},
			},
			envVars: []v1.EnvVar{
				{
					Name: "minio_access_key",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "minio_access_key",
						},
					},
				},
				{
					Name: "minio_secret_key",
					ValueFrom: &v1.EnvVarSource{
						SecretKeyRef: &v1.SecretKeySelector{
							LocalObjectReference: v1.LocalObjectReference{
								Name: testPodEnvironmentSecretName,
							},
							Key: "minio_secret_key",
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
		"PGHOST":                 false,
		"PGPORT":                 false,
		"PGUSER":                 false,
		"PGSCHEMA":               false,
		"PGPASSWORD":             false,
		"CONNECTION_POOLER_MODE": false,
		"CONNECTION_POOLER_PORT": false,
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
			Resources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: "1", Memory: "10"},
				ResourceLimits:   acidv1.ResourceDescription{CPU: "1", Memory: "10"},
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
				v1.NodeSelectorTerm{
					MatchExpressions: []v1.NodeSelectorRequirement{
						v1.NodeSelectorRequirement{
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

func testDeploymentOwnerReference(cluster *Cluster, deployment *appsv1.Deployment) error {
	owner := deployment.ObjectMeta.OwnerReferences[0]

	if owner.Name != cluster.Statefulset.ObjectMeta.Name {
		return fmt.Errorf("Ownere reference is incorrect, got %s, expected %s",
			owner.Name, cluster.Statefulset.ObjectMeta.Name)
	}

	return nil
}

func testServiceOwnerReference(cluster *Cluster, service *v1.Service, role PostgresRole) error {
	owner := service.ObjectMeta.OwnerReferences[0]

	if owner.Name != cluster.Statefulset.ObjectMeta.Name {
		return fmt.Errorf("Ownere reference is incorrect, got %s, expected %s",
			owner.Name, cluster.Statefulset.ObjectMeta.Name)
	}

	return nil
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
			Resources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: "1", Memory: "10"},
				ResourceLimits:   acidv1.ResourceDescription{CPU: "1", Memory: "10"},
			},
			Volume: acidv1.Volume{
				Size: "1G",
			},
			TLS: &acidv1.TLSDescription{
				SecretName: tlsSecretName, CAFile: "ca.crt"},
			AdditionalVolumes: []acidv1.AdditionalVolume{
				acidv1.AdditionalVolume{
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

func TestAdditionalVolume(t *testing.T) {
	testName := "TestAdditionalVolume"

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
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			TeamID: "myapp", NumberOfInstances: 1,
			Resources: acidv1.Resources{
				ResourceRequests: acidv1.ResourceDescription{CPU: "1", Memory: "10"},
				ResourceLimits:   acidv1.ResourceDescription{CPU: "1", Memory: "10"},
			},
			Volume: acidv1.Volume{
				Size: "1G",
			},
			AdditionalVolumes: additionalVolumes,
			Sidecars: []acidv1.Sidecar{
				{
					Name: sidecarName,
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
		subTest        string
		container      string
		expectedMounts []string
	}{
		{
			subTest:        "checking volume mounts of postgres container",
			container:      constants.PostgresContainerName,
			expectedMounts: []string{"pgdata", "test1", "test3", "test4"},
		},
		{
			subTest:        "checking volume mounts of sidecar container",
			container:      "sidecar",
			expectedMounts: []string{"pgdata", "test1", "test2"},
		},
	}

	for _, tt := range tests {
		for _, container := range sts.Spec.Template.Spec.Containers {
			if container.Name != tt.container {
				continue
			}
			mounts := []string{}
			for _, volumeMounts := range container.VolumeMounts {
				mounts = append(mounts, volumeMounts.Name)
			}

			if !util.IsEqualIgnoreOrder(mounts, tt.expectedMounts) {
				t.Errorf("%s %s: different volume mounts: got %v, epxected %v",
					testName, tt.subTest, mounts, tt.expectedMounts)
			}
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
			PgVersion: "12.1",
			Parameters: map[string]string{
				"max_connections": "100",
			},
		},
		TeamID: "myapp", NumberOfInstances: 1,
		Resources: acidv1.Resources{
			ResourceRequests: acidv1.ResourceDescription{CPU: "1", Memory: "10"},
			ResourceLimits:   acidv1.ResourceDescription{CPU: "1", Memory: "10"},
		},
		Volume: acidv1.Volume{
			Size: "1G",
		},
		Sidecars: []acidv1.Sidecar{
			acidv1.Sidecar{
				Name: "cluster-specific-sidecar",
			},
			acidv1.Sidecar{
				Name: "cluster-specific-sidecar-with-resources",
				Resources: acidv1.Resources{
					ResourceRequests: acidv1.ResourceDescription{CPU: "210m", Memory: "0.8Gi"},
					ResourceLimits:   acidv1.ResourceDescription{CPU: "510m", Memory: "1.4Gi"},
				},
			},
			acidv1.Sidecar{
				Name:        "replace-sidecar",
				DockerImage: "overwrite-image",
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
					DefaultCPULimit:      "500m",
					DefaultMemoryRequest: "0.7Gi",
					DefaultMemoryLimit:   "1.3Gi",
				},
				SidecarImages: map[string]string{
					"deprecated-global-sidecar": "image:123",
				},
				SidecarContainers: []v1.Container{
					v1.Container{
						Name: "global-sidecar",
					},
					// will be replaced by a cluster specific sidecar with the same name
					v1.Container{
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
		v1.VolumeMount{
			Name:      "pgdata",
			MountPath: "/home/postgres/pgdata",
		},
	}

	// deduplicated sidecars and Patroni
	assert.Equal(t, 7, len(s.Spec.Template.Spec.Containers), "wrong number of containers")

	// cluster specific sidecar
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:            "cluster-specific-sidecar",
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
		Env:          env,
		VolumeMounts: mounts,
	})

	// replaced sidecar
	assert.Contains(t, s.Spec.Template.Spec.Containers, v1.Container{
		Name:            "replace-sidecar",
		Image:           "overwrite-image",
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

func TestGenerateService(t *testing.T) {
	var spec acidv1.PostgresSpec
	var cluster *Cluster
	var enableLB bool = true
	spec = acidv1.PostgresSpec{
		TeamID: "myapp", NumberOfInstances: 1,
		Resources: acidv1.Resources{
			ResourceRequests: acidv1.ResourceDescription{CPU: "1", Memory: "10"},
			ResourceLimits:   acidv1.ResourceDescription{CPU: "1", Memory: "10"},
		},
		Volume: acidv1.Volume{
			Size: "1G",
		},
		Sidecars: []acidv1.Sidecar{
			acidv1.Sidecar{
				Name: "cluster-specific-sidecar",
			},
			acidv1.Sidecar{
				Name: "cluster-specific-sidecar-with-resources",
				Resources: acidv1.Resources{
					ResourceRequests: acidv1.ResourceDescription{CPU: "210m", Memory: "0.8Gi"},
					ResourceLimits:   acidv1.ResourceDescription{CPU: "510m", Memory: "1.4Gi"},
				},
			},
			acidv1.Sidecar{
				Name:        "replace-sidecar",
				DockerImage: "overwrite-image",
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
					DefaultCPULimit:      "500m",
					DefaultMemoryRequest: "0.7Gi",
					DefaultMemoryLimit:   "1.3Gi",
				},
				SidecarImages: map[string]string{
					"deprecated-global-sidecar": "image:123",
				},
				SidecarContainers: []v1.Container{
					v1.Container{
						Name: "global-sidecar",
					},
					// will be replaced by a cluster specific sidecar with the same name
					v1.Container{
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

func TestGenerateCapabilities(t *testing.T) {

	testName := "TestGenerateCapabilities"
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
				testName, tt.subTest, tt.capabilities, caps)
		}
	}
}
