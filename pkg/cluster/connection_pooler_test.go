package cluster

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func newFakeK8sPoolerTestClient() (k8sutil.KubernetesClient, *fake.Clientset) {
	acidClientSet := fakeacidv1.NewSimpleClientset()
	clientSet := fake.NewSimpleClientset()

	return k8sutil.KubernetesClient{
		PodsGetter:         clientSet.CoreV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		StatefulSetsGetter: clientSet.AppsV1(),
		DeploymentsGetter:  clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		SecretsGetter:      clientSet.CoreV1(),
	}, clientSet
}

func mockInstallLookupFunction(schema string, user string) error {
	return nil
}

func boolToPointer(value bool) *bool {
	return &value
}

func deploymentUpdated(cluster *Cluster, err error, reason SyncReason) error {
	for _, role := range [2]PostgresRole{Master, Replica} {

		poolerLabels := cluster.labelsSet(false)
		poolerLabels["application"] = "db-connection-pooler"
		poolerLabels["connection-pooler"] = cluster.connectionPoolerName(role)

		if cluster.ConnectionPooler[role] != nil && cluster.ConnectionPooler[role].Deployment != nil &&
			util.MapContains(cluster.ConnectionPooler[role].Deployment.Labels, poolerLabels) &&
			(cluster.ConnectionPooler[role].Deployment.Spec.Replicas == nil ||
				*cluster.ConnectionPooler[role].Deployment.Spec.Replicas != 2) {
			return fmt.Errorf("Wrong number of instances")
		}
	}
	return nil
}

func objectsAreSaved(cluster *Cluster, err error, reason SyncReason) error {
	if cluster.ConnectionPooler == nil {
		return fmt.Errorf("Connection pooler resources are empty")
	}

	for _, role := range []PostgresRole{Master, Replica} {
		poolerLabels := cluster.labelsSet(false)
		poolerLabels["application"] = "db-connection-pooler"
		poolerLabels["connection-pooler"] = cluster.connectionPoolerName(role)

		if cluster.ConnectionPooler[role].Deployment == nil || !util.MapContains(cluster.ConnectionPooler[role].Deployment.Labels, poolerLabels) {
			return fmt.Errorf("Deployment was not saved or labels not attached %s %s", role, cluster.ConnectionPooler[role].Deployment.Labels)
		}

		if cluster.ConnectionPooler[role].Service == nil || !util.MapContains(cluster.ConnectionPooler[role].Service.Labels, poolerLabels) {
			return fmt.Errorf("Service was not saved or labels not attached %s %s", role, cluster.ConnectionPooler[role].Service.Labels)
		}
	}

	return nil
}

func MasterObjectsAreSaved(cluster *Cluster, err error, reason SyncReason) error {
	if cluster.ConnectionPooler == nil {
		return fmt.Errorf("Connection pooler resources are empty")
	}

	poolerLabels := cluster.labelsSet(false)
	poolerLabels["application"] = "db-connection-pooler"
	poolerLabels["connection-pooler"] = cluster.connectionPoolerName(Master)

	if cluster.ConnectionPooler[Master].Deployment == nil || !util.MapContains(cluster.ConnectionPooler[Master].Deployment.Labels, poolerLabels) {
		return fmt.Errorf("Deployment was not saved or labels not attached %s", cluster.ConnectionPooler[Master].Deployment.Labels)
	}

	if cluster.ConnectionPooler[Master].Service == nil || !util.MapContains(cluster.ConnectionPooler[Master].Service.Labels, poolerLabels) {
		return fmt.Errorf("Service was not saved or labels not attached %s", cluster.ConnectionPooler[Master].Service.Labels)
	}

	return nil
}

func ReplicaObjectsAreSaved(cluster *Cluster, err error, reason SyncReason) error {
	if cluster.ConnectionPooler == nil {
		return fmt.Errorf("Connection pooler resources are empty")
	}

	poolerLabels := cluster.labelsSet(false)
	poolerLabels["application"] = "db-connection-pooler"
	poolerLabels["connection-pooler"] = cluster.connectionPoolerName(Replica)

	if cluster.ConnectionPooler[Replica].Deployment == nil || !util.MapContains(cluster.ConnectionPooler[Replica].Deployment.Labels, poolerLabels) {
		return fmt.Errorf("Deployment was not saved or labels not attached %s", cluster.ConnectionPooler[Replica].Deployment.Labels)
	}

	if cluster.ConnectionPooler[Replica].Service == nil || !util.MapContains(cluster.ConnectionPooler[Replica].Service.Labels, poolerLabels) {
		return fmt.Errorf("Service was not saved or labels not attached %s", cluster.ConnectionPooler[Replica].Service.Labels)
	}

	return nil
}

func objectsAreDeleted(cluster *Cluster, err error, reason SyncReason) error {
	for _, role := range [2]PostgresRole{Master, Replica} {
		if cluster.ConnectionPooler[role] != nil &&
			(cluster.ConnectionPooler[role].Deployment != nil || cluster.ConnectionPooler[role].Service != nil) {
			return fmt.Errorf("Connection pooler was not deleted for role %v", role)
		}
	}

	return nil
}

func OnlyMasterDeleted(cluster *Cluster, err error, reason SyncReason) error {

	if cluster.ConnectionPooler[Master] != nil &&
		(cluster.ConnectionPooler[Master].Deployment != nil || cluster.ConnectionPooler[Master].Service != nil) {
		return fmt.Errorf("Connection pooler master was not deleted")
	}
	return nil
}

func OnlyReplicaDeleted(cluster *Cluster, err error, reason SyncReason) error {

	if cluster.ConnectionPooler[Replica] != nil &&
		(cluster.ConnectionPooler[Replica].Deployment != nil || cluster.ConnectionPooler[Replica].Service != nil) {
		return fmt.Errorf("Connection pooler replica was not deleted")
	}
	return nil
}

func noEmptySync(cluster *Cluster, err error, reason SyncReason) error {
	for _, msg := range reason {
		if strings.HasPrefix(msg, "update [] from '<nil>' to '") {
			return fmt.Errorf("There is an empty reason, %s", msg)
		}
	}

	return nil
}

func TestNeedConnectionPooler(t *testing.T) {
	testName := "Test how connection pooler can be enabled"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.NewMockKubernetesClient(), acidv1.Postgresql{}, logger, eventRecorder)

	cluster.Spec = acidv1.PostgresSpec{
		ConnectionPooler: &acidv1.ConnectionPooler{},
	}

	if !needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is not enabled with full definition",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler: boolToPointer(true),
	}

	if !needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is not enabled with flag",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler: boolToPointer(false),
		ConnectionPooler:       &acidv1.ConnectionPooler{},
	}

	if needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is still enabled with flag being false",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler: boolToPointer(true),
		ConnectionPooler:       &acidv1.ConnectionPooler{},
	}

	if !needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is not enabled with flag and full",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableConnectionPooler:        boolToPointer(false),
		EnableReplicaConnectionPooler: boolToPointer(false),
		ConnectionPooler:              nil,
	}

	if needMasterConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Connection pooler is enabled with flag false and nil",
			testName)
	}

	// Test for replica connection pooler
	cluster.Spec = acidv1.PostgresSpec{
		ConnectionPooler: &acidv1.ConnectionPooler{},
	}

	if needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is not enabled with full definition",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableReplicaConnectionPooler: boolToPointer(true),
	}

	if !needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is not enabled with flag",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableReplicaConnectionPooler: boolToPointer(false),
		ConnectionPooler:              &acidv1.ConnectionPooler{},
	}

	if needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is still enabled with flag being false",
			testName)
	}

	cluster.Spec = acidv1.PostgresSpec{
		EnableReplicaConnectionPooler: boolToPointer(true),
		ConnectionPooler:              &acidv1.ConnectionPooler{},
	}

	if !needReplicaConnectionPooler(&cluster.Spec) {
		t.Errorf("%s: Replica Connection pooler is not enabled with flag and full",
			testName)
	}
}

func TestEffectiveConnectionPoolerSpec(t *testing.T) {
	masterCPU := "100m"
	replicaCPU := "200m"
	masterResources := &acidv1.Resources{
		ResourceRequests: acidv1.ResourceDescription{CPU: &masterCPU},
	}
	replicaResources := &acidv1.Resources{
		ResourceRequests: acidv1.ResourceDescription{CPU: &replicaCPU},
	}

	master := &acidv1.ConnectionPooler{
		NumberOfInstances: k8sutil.Int32ToPointer(2),
		Schema:            "pooler",
		User:              "pooler",
		Mode:              "transaction",
		DockerImage:       "master-image",
		MaxDBConnections:  k8sutil.Int32ToPointer(60),
		Resources:         masterResources,
	}

	tests := []struct {
		subTest  string
		spec     acidv1.PostgresSpec
		role     PostgresRole
		expected *acidv1.ConnectionPooler
	}{
		{
			subTest:  "no pooler section at all still yields a non-nil spec",
			spec:     acidv1.PostgresSpec{},
			role:     Master,
			expected: &acidv1.ConnectionPooler{},
		},
		{
			subTest: "master ignores replicaConnectionPooler entirely",
			spec: acidv1.PostgresSpec{
				ConnectionPooler: master,
				ReplicaConnectionPooler: &acidv1.ReplicaConnectionPooler{
					NumberOfInstances: k8sutil.Int32ToPointer(5),
					Mode:              "session",
				},
			},
			role:     Master,
			expected: master,
		},
		{
			subTest: "replica without an override matches connectionPooler",
			spec: acidv1.PostgresSpec{
				ConnectionPooler: master,
			},
			role:     Replica,
			expected: master,
		},
		{
			subTest: "replica inherits the fields it does not override",
			spec: acidv1.PostgresSpec{
				ConnectionPooler: master,
				ReplicaConnectionPooler: &acidv1.ReplicaConnectionPooler{
					NumberOfInstances: k8sutil.Int32ToPointer(5),
				},
			},
			role: Replica,
			expected: &acidv1.ConnectionPooler{
				NumberOfInstances: k8sutil.Int32ToPointer(5),
				Schema:            "pooler",
				User:              "pooler",
				Mode:              "transaction",
				DockerImage:       "master-image",
				MaxDBConnections:  k8sutil.Int32ToPointer(60),
				Resources:         masterResources,
			},
		},
		{
			subTest: "replica overrides every field it carries, user and schema still inherited",
			spec: acidv1.PostgresSpec{
				ConnectionPooler: master,
				ReplicaConnectionPooler: &acidv1.ReplicaConnectionPooler{
					NumberOfInstances: k8sutil.Int32ToPointer(5),
					Mode:              "session",
					DockerImage:       "replica-image",
					MaxDBConnections:  k8sutil.Int32ToPointer(10),
					Resources:         replicaResources,
				},
			},
			role: Replica,
			expected: &acidv1.ConnectionPooler{
				NumberOfInstances: k8sutil.Int32ToPointer(5),
				Schema:            "pooler",
				User:              "pooler",
				Mode:              "session",
				DockerImage:       "replica-image",
				MaxDBConnections:  k8sutil.Int32ToPointer(10),
				Resources:         replicaResources,
			},
		},
		{
			subTest: "replicaConnectionPooler alone, with no connectionPooler section",
			spec: acidv1.PostgresSpec{
				ReplicaConnectionPooler: &acidv1.ReplicaConnectionPooler{
					NumberOfInstances: k8sutil.Int32ToPointer(5),
				},
			},
			role: Replica,
			expected: &acidv1.ConnectionPooler{
				NumberOfInstances: k8sutil.Int32ToPointer(5),
			},
		},
	}

	for _, tt := range tests {
		spec := tt.spec
		assert.Equal(t, tt.expected, effectiveConnectionPoolerSpec(&spec, tt.role), tt.subTest)
	}
}

func TestReplicaConnectionPoolerSyncOnOverrideChange(t *testing.T) {
	testName := "changing replicaConnectionPooler rolls the replica pooler only"
	namespace := "default"

	clientSet := fake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()
	client := k8sutil.KubernetesClient{
		StatefulSetsGetter: clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		PodsGetter:         clientSet.CoreV1(),
		DeploymentsGetter:  clientSet.AppsV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		SecretsGetter:      clientSet.CoreV1(),
	}

	poolerSpec := func(replicaInstances int32) acidv1.PostgresSpec {
		return acidv1.PostgresSpec{
			Volume:                        acidv1.Volume{Size: "1Gi"},
			EnableConnectionPooler:        boolToPointer(true),
			EnableReplicaConnectionPooler: boolToPointer(true),
			ConnectionPooler: &acidv1.ConnectionPooler{
				NumberOfInstances: k8sutil.Int32ToPointer(2),
			},
			ReplicaConnectionPooler: &acidv1.ReplicaConnectionPooler{
				NumberOfInstances: k8sutil.Int32ToPointer(replicaInstances),
			},
		}
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acid-fake-cluster",
			Namespace: namespace,
		},
		Spec: poolerSpec(2),
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
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

	cluster.Name = "acid-fake-cluster"
	cluster.Namespace = namespace

	_, err := cluster.createService(Master)
	assert.NoError(t, err)
	_, err = cluster.createStatefulSet()
	assert.NoError(t, err)

	_, err = cluster.createConnectionPooler(mockInstallLookupFunction)
	assert.NoError(t, err)

	assert.Equal(t, int32(2), *cluster.ConnectionPooler[Master].Deployment.Spec.Replicas,
		testName+": master pooler starts with the connectionPooler instance count")
	assert.Equal(t, int32(2), *cluster.ConnectionPooler[Replica].Deployment.Spec.Replicas,
		testName+": replica pooler starts with the replicaConnectionPooler instance count")

	// only replicaConnectionPooler changes between the two specs
	oldSpec := &acidv1.Postgresql{Spec: poolerSpec(2)}
	newSpec := &acidv1.Postgresql{Spec: poolerSpec(4)}
	cluster.Spec = newSpec.Spec

	_, err = cluster.syncConnectionPooler(oldSpec, newSpec, mockInstallLookupFunction)
	assert.NoError(t, err)

	// if the sync comparison is not role-aware it sees no change here, the
	// deployment is never regenerated and the replica pooler silently keeps
	// running the old instance count
	assert.Equal(t, int32(4), *cluster.ConnectionPooler[Replica].Deployment.Spec.Replicas,
		testName+": replica pooler must pick up the new replicaConnectionPooler instance count")
	assert.Equal(t, int32(2), *cluster.ConnectionPooler[Master].Deployment.Spec.Replicas,
		testName+": master pooler must be unaffected by a replicaConnectionPooler change")
}

func TestConnectionPoolerCreateDeletion(t *testing.T) {

	testName := "test connection pooler creation and deletion"
	clientSet := fake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()
	namespace := "default"

	client := k8sutil.KubernetesClient{
		StatefulSetsGetter: clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		PodsGetter:         clientSet.CoreV1(),
		DeploymentsGetter:  clientSet.AppsV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		SecretsGetter:      clientSet.CoreV1(),
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acid-fake-cluster",
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			EnableConnectionPooler:        boolToPointer(true),
			EnableReplicaConnectionPooler: boolToPointer(true),
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
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

	cluster.Name = "acid-fake-cluster"
	cluster.Namespace = "default"

	_, err := cluster.createService(Master)
	assert.NoError(t, err)
	_, err = cluster.createStatefulSet()
	assert.NoError(t, err)

	reason, err := cluster.createConnectionPooler(mockInstallLookupFunction)

	if err != nil {
		t.Errorf("%s: Cannot create connection pooler, %s, %+v",
			testName, err, reason)
	}
	for _, role := range [2]PostgresRole{Master, Replica} {
		poolerLabels := cluster.labelsSet(false)
		poolerLabels["application"] = "db-connection-pooler"
		poolerLabels["connection-pooler"] = cluster.connectionPoolerName(role)

		if cluster.ConnectionPooler[role] != nil {
			if cluster.ConnectionPooler[role].Deployment == nil && util.MapContains(cluster.ConnectionPooler[role].Deployment.Labels, poolerLabels) {
				t.Errorf("%s: Connection pooler deployment is empty for role %s", testName, role)
			}

			if cluster.ConnectionPooler[role].Service == nil && util.MapContains(cluster.ConnectionPooler[role].Service.Labels, poolerLabels) {
				t.Errorf("%s: Connection pooler service is empty for role %s", testName, role)
			}
		}
	}

	oldSpec := &acidv1.Postgresql{
		Spec: acidv1.PostgresSpec{
			EnableConnectionPooler:        boolToPointer(true),
			EnableReplicaConnectionPooler: boolToPointer(true),
		},
	}
	newSpec := &acidv1.Postgresql{
		Spec: acidv1.PostgresSpec{
			EnableConnectionPooler:        boolToPointer(false),
			EnableReplicaConnectionPooler: boolToPointer(false),
		},
	}

	// Delete connection pooler via sync
	_, err = cluster.syncConnectionPooler(oldSpec, newSpec, mockInstallLookupFunction)
	if err != nil {
		t.Errorf("%s: Cannot sync connection pooler, %s", testName, err)
	}

	for _, role := range [2]PostgresRole{Master, Replica} {
		err = cluster.deleteConnectionPooler(role)
		if err != nil {
			t.Errorf("%s: Cannot delete connection pooler, %s", testName, err)
		}
	}
}

func TestConnectionPoolerSync(t *testing.T) {

	testName := "test connection pooler synchronization"
	clientSet := fake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()
	namespace := "default"

	client := k8sutil.KubernetesClient{
		StatefulSetsGetter: clientSet.AppsV1(),
		ServicesGetter:     clientSet.CoreV1(),
		PodsGetter:         clientSet.CoreV1(),
		DeploymentsGetter:  clientSet.AppsV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		SecretsGetter:      clientSet.CoreV1(),
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "acid-fake-cluster",
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
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

	cluster.Name = "acid-fake-cluster"
	cluster.Namespace = "default"

	_, err := cluster.createService(Master)
	assert.NoError(t, err)
	_, err = cluster.createStatefulSet()
	assert.NoError(t, err)

	reason, err := cluster.createConnectionPooler(mockInstallLookupFunction)

	if err != nil {
		t.Errorf("%s: Cannot create connection pooler, %s, %+v",
			testName, err, reason)
	}

	tests := []struct {
		subTest          string
		oldSpec          *acidv1.Postgresql
		newSpec          *acidv1.Postgresql
		cluster          *Cluster
		defaultImage     string
		defaultInstances int32
		check            func(cluster *Cluster, err error, reason SyncReason) error
	}{
		{
			subTest: "create from scratch",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            MasterObjectsAreSaved,
		},
		{
			subTest: "create if doesn't exist",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            MasterObjectsAreSaved,
		},
		{
			subTest: "create if doesn't exist with a flag",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler: boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            MasterObjectsAreSaved,
		},
		{
			subTest: "create no replica with flag",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(false),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            objectsAreDeleted,
		},
		{
			subTest: "create replica if doesn't exist with a flag",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:              &acidv1.ConnectionPooler{},
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            ReplicaObjectsAreSaved,
		},
		{
			subTest: "create both master and replica",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:              &acidv1.ConnectionPooler{},
					EnableReplicaConnectionPooler: boolToPointer(true),
					EnableConnectionPooler:        boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            objectsAreSaved,
		},
		{
			subTest: "delete only replica if not needed",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:              &acidv1.ConnectionPooler{},
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            OnlyReplicaDeleted,
		},
		{
			subTest: "delete only master if not needed",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler:       &acidv1.ConnectionPooler{},
					EnableConnectionPooler: boolToPointer(true),
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableReplicaConnectionPooler: boolToPointer(true),
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            OnlyMasterDeleted,
		},
		{
			subTest: "delete if not needed",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            objectsAreDeleted,
		},
		{
			subTest: "cleanup if still there",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            objectsAreDeleted,
		},
		{
			subTest: "update image from changed defaults",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					ConnectionPooler: &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:2.0",
			defaultInstances: 2,
			check:            deploymentUpdated,
		},
		{
			subTest: "there is no sync from nil to an empty spec",
			oldSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler: boolToPointer(true),
					ConnectionPooler:       nil,
				},
			},
			newSpec: &acidv1.Postgresql{
				Spec: acidv1.PostgresSpec{
					EnableConnectionPooler: boolToPointer(true),
					ConnectionPooler:       &acidv1.ConnectionPooler{},
				},
			},
			cluster:          cluster,
			defaultImage:     "pooler:1.0",
			defaultInstances: 1,
			check:            noEmptySync,
		},
	}
	for _, tt := range tests {
		tt.cluster.OpConfig.ConnectionPooler.Image = tt.defaultImage
		tt.cluster.OpConfig.ConnectionPooler.NumberOfInstances =
			k8sutil.Int32ToPointer(tt.defaultInstances)

		t.Logf("running test for %s [%s]", testName, tt.subTest)

		reason, err := tt.cluster.syncConnectionPooler(tt.oldSpec,
			tt.newSpec, mockInstallLookupFunction)

		if err := tt.check(tt.cluster, err, reason); err != nil {
			t.Errorf("%s [%s]: Could not synchronize, %+v",
				testName, tt.subTest, err)
		}
	}
}

func TestConnectionPoolerPodSpec(t *testing.T) {
	testName := "Test connection pooler pod template generation"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				PodServiceAccountName: "postgres-pod",
				ConnectionPooler: config.ConnectionPooler{
					MaxDBConnections:                     k8sutil.Int32ToPointer(60),
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	cluster.Spec = acidv1.PostgresSpec{
		ConnectionPooler:              &acidv1.ConnectionPooler{},
		EnableReplicaConnectionPooler: boolToPointer(true),
	}
	var clusterNoDefaultRes = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	clusterNoDefaultRes.Spec = acidv1.PostgresSpec{
		ConnectionPooler:              &acidv1.ConnectionPooler{},
		EnableReplicaConnectionPooler: boolToPointer(true),
	}

	noCheck := func(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole) error { return nil }

	tests := []struct {
		subTest string
		spec    *acidv1.PostgresSpec
		cluster *Cluster
		check   func(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole) error
	}{
		{
			subTest: "default configuration",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			check:   noCheck,
		},
		{
			subTest: "pooler uses pod service account",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			check:   testServiceAccount,
		},
		{
			subTest: "no default resources",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: clusterNoDefaultRes,
			check:   noCheck,
		},
		{
			subTest: "default resources are set",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			check:   testResources,
		},
		{
			subTest: "labels for service",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			cluster: cluster,
			check:   testLabels,
		},
		{
			subTest: "required envs",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			check:   testEnvs,
		},
	}
	for _, role := range [2]PostgresRole{Master, Replica} {
		for _, tt := range tests {
			podSpec, _ := tt.cluster.generateConnectionPoolerPodTemplate(role)

			err := tt.check(cluster, podSpec, role)
			if err != nil {
				t.Errorf("%s [%s]: Pod spec is incorrect, %+v",
					testName, tt.subTest, err)
			}
		}
	}
}

func TestConnectionPoolerDeploymentSpec(t *testing.T) {
	testName := "Test connection pooler deployment spec generation"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)
	cluster.Statefulset = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-sts",
		},
	}
	cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{
		Master: {
			AuthSecret:     nil,
			Deployment:     nil,
			Service:        nil,
			LookupFunction: true,
			Name:           "",
			Role:           Master,
		},
	}

	noCheck := func(cluster *Cluster, deployment *appsv1.Deployment) error {
		return nil
	}

	tests := []struct {
		subTest  string
		spec     *acidv1.PostgresSpec
		expected error
		cluster  *Cluster
		check    func(cluster *Cluster, deployment *appsv1.Deployment) error
	}{
		{
			subTest: "default configuration",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			expected: nil,
			cluster:  cluster,
			check:    noCheck,
		},
		{
			subTest: "owner reference",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			expected: nil,
			cluster:  cluster,
			check:    testDeploymentOwnerReference,
		},
		{
			subTest: "selector",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			expected: nil,
			cluster:  cluster,
			check:    testSelector,
		},
	}
	for _, tt := range tests {
		deployment, err := tt.cluster.generateConnectionPoolerDeployment(cluster.ConnectionPooler[Master])

		if err != tt.expected && err.Error() != tt.expected.Error() {
			t.Errorf("%s [%s]: Could not generate deployment spec,\n %+v, expected\n %+v",
				testName, tt.subTest, err, tt.expected)
		}

		err = tt.check(cluster, deployment)
		if err != nil {
			t.Errorf("%s [%s]: Deployment spec is incorrect, %+v",
				testName, tt.subTest, err)
		}
	}
}

func testServiceAccount(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole) error {
	poolerServiceAccount := podSpec.Spec.ServiceAccountName

	if poolerServiceAccount != cluster.OpConfig.PodServiceAccountName {
		return fmt.Errorf("Pooler service account does not match, got %+v, expected %+v",
			poolerServiceAccount, cluster.OpConfig.PodServiceAccountName)
	}

	return nil
}

func testResources(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole) error {
	cpuReq := podSpec.Spec.Containers[0].Resources.Requests["cpu"]
	if cpuReq.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPURequest {
		return fmt.Errorf("CPU request does not match, got %s, expected %s",
			cpuReq.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPURequest)
	}

	memReq := podSpec.Spec.Containers[0].Resources.Requests["memory"]
	if memReq.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest {
		return fmt.Errorf("Memory request does not match, got %s, expected %s",
			memReq.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest)
	}

	cpuLim := podSpec.Spec.Containers[0].Resources.Limits["cpu"]
	if cpuLim.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPULimit {
		return fmt.Errorf("CPU limit does not match, got %s, expected %s",
			cpuLim.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultCPULimit)
	}

	memLim := podSpec.Spec.Containers[0].Resources.Limits["memory"]
	if memLim.String() != cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit {
		return fmt.Errorf("Memory limit does not match, got %s, expected %s",
			memLim.String(), cluster.OpConfig.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit)
	}

	return nil
}

func testLabels(cluster *Cluster, podSpec *v1.PodTemplateSpec, role PostgresRole) error {
	poolerLabels := podSpec.ObjectMeta.Labels["connection-pooler"]

	if poolerLabels != cluster.connectionPoolerLabels(role, true).MatchLabels["connection-pooler"] {
		return fmt.Errorf("Pod labels do not match, got %+v, expected %+v",
			podSpec.ObjectMeta.Labels, cluster.connectionPoolerLabels(role, true).MatchLabels)
	}

	return nil
}

func testSelector(cluster *Cluster, deployment *appsv1.Deployment) error {
	labels := deployment.Spec.Selector.MatchLabels
	expected := cluster.connectionPoolerLabels(Master, true).MatchLabels

	if labels["connection-pooler"] != expected["connection-pooler"] {
		return fmt.Errorf("Labels are incorrect, got %+v, expected %+v",
			labels, expected)
	}

	return nil
}

func testServiceSelector(cluster *Cluster, service *v1.Service, role PostgresRole) error {
	selector := service.Spec.Selector

	if selector["connection-pooler"] != cluster.connectionPoolerName(role) {
		return fmt.Errorf("Selector is incorrect, got %s, expected %s",
			selector["connection-pooler"], cluster.connectionPoolerName(role))
	}

	return nil
}

func TestPoolerTLS(t *testing.T) {
	client, _ := newFakeK8sPoolerTestClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	tlsSecretName := "my-secret"
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
			EnableConnectionPooler: util.True(),
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
					ClusterLabels:        map[string]string{"application": "spilo"},
					ClusterNameLabel:     "cluster-name",
					DefaultCPURequest:    "300m",
					DefaultCPULimit:      "300m",
					DefaultMemoryRequest: "300Mi",
					DefaultMemoryLimit:   "300Mi",
					PodRoleLabel:         "spilo-role",
					SpiloFSGroup:         &spiloFSGroup,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
				PodServiceAccountName: "postgres-pod",
			},
		}, client, pg, logger, eventRecorder)

	// create a statefulset
	_, err := cluster.createStatefulSet()
	assert.NoError(t, err)

	// create pooler resources
	cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{}
	cluster.ConnectionPooler[Master] = &ConnectionPoolerObjects{
		AuthSecret:     nil,
		Deployment:     nil,
		Service:        nil,
		Name:           cluster.connectionPoolerName(Master),
		ClusterName:    clusterName,
		Namespace:      namespace,
		LookupFunction: false,
		Role:           Master,
	}

	_, err = cluster.syncConnectionPoolerWorker(nil, &pg, Master)
	assert.NoError(t, err)

	deploy, err := client.Deployments(namespace).Get(context.TODO(), cluster.connectionPoolerName(Master), metav1.GetOptions{})
	assert.NoError(t, err)

	fsGroup := int64(103)
	assert.Equal(t, &fsGroup, deploy.Spec.Template.Spec.SecurityContext.FSGroup, "has a default FSGroup assigned")

	assert.Equal(t, "postgres-pod", deploy.Spec.Template.Spec.ServiceAccountName, "need to add a service account name")

	volume := v1.Volume{
		Name: "my-secret",
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName:  "my-secret",
				DefaultMode: &defaultMode,
			},
		},
	}
	assert.Contains(t, deploy.Spec.Template.Spec.Volumes, volume, "the pod gets a secret volume")

	poolerContainer := deploy.Spec.Template.Spec.Containers[constants.ConnectionPoolerContainer]
	assert.Contains(t, poolerContainer.VolumeMounts, v1.VolumeMount{
		MountPath: "/tls",
		Name:      "my-secret",
	}, "the volume gets mounted in /tls")

	assert.Contains(t, poolerContainer.Env, v1.EnvVar{Name: "CONNECTION_POOLER_CLIENT_TLS_CRT", Value: "/tls/tls.crt"})
	assert.Contains(t, poolerContainer.Env, v1.EnvVar{Name: "CONNECTION_POOLER_CLIENT_TLS_KEY", Value: "/tls/tls.key"})
	assert.Contains(t, poolerContainer.Env, v1.EnvVar{Name: "CONNECTION_POOLER_CLIENT_CA_FILE", Value: "/tls/ca.crt"})
}

func TestConnectionPoolerServiceSpec(t *testing.T) {
	testName := "Test connection pooler service spec generation"
	var cluster = New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
				Resources: config.Resources{
					EnableOwnerReferences: util.True(),
				},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)
	cluster.Statefulset = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-sts",
		},
	}
	cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{
		Master: {
			AuthSecret:     nil,
			Deployment:     nil,
			Service:        nil,
			LookupFunction: false,
			Role:           Master,
		},
		Replica: {
			AuthSecret:     nil,
			Deployment:     nil,
			Service:        nil,
			LookupFunction: false,
			Role:           Replica,
		},
	}

	noCheck := func(cluster *Cluster, deployment *v1.Service, role PostgresRole) error {
		return nil
	}

	tests := []struct {
		subTest string
		spec    *acidv1.PostgresSpec
		cluster *Cluster
		check   func(cluster *Cluster, deployment *v1.Service, role PostgresRole) error
	}{
		{
			subTest: "default configuration",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			check:   noCheck,
		},
		{
			subTest: "owner reference",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			check:   testServiceOwnerReference,
		},
		{
			subTest: "selector",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:              &acidv1.ConnectionPooler{},
				EnableReplicaConnectionPooler: boolToPointer(true),
			},
			cluster: cluster,
			check:   testServiceSelector,
		},
	}
	for _, role := range [2]PostgresRole{Master, Replica} {
		for _, tt := range tests {
			service := tt.cluster.generateConnectionPoolerService(tt.cluster.ConnectionPooler[role])

			if err := tt.check(cluster, service, role); err != nil {
				t.Errorf("%s [%s]: Service spec is incorrect, %+v",
					testName, tt.subTest, err)
			}
		}
	}
}

func TestConnectionPoolerServiceType(t *testing.T) {
	testName := "Test connection pooler service type selection"

	cluster := New(
		Config{
			OpConfig: config.Config{
				ProtectedRoles: []string{"admin"},
				Auth: config.Auth{
					SuperUsername:       superUserName,
					ReplicationUsername: replicationUserName,
				},
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
				},
				Resources: config.Resources{
					EnableOwnerReferences: util.True(),
				},
			},
		},
		k8sutil.KubernetesClient{},
		acidv1.Postgresql{},
		logger,
		eventRecorder,
	)

	cluster.Statefulset = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-sts",
		},
	}

	cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{
		Master: {
			Deployment:     nil,
			Service:        nil,
			LookupFunction: false,
			Role:           Master,
		},
		Replica: {
			Deployment:     nil,
			Service:        nil,
			LookupFunction: false,
			Role:           Replica,
		},
	}

	tests := []struct {
		subTest      string
		spec         *acidv1.PostgresSpec
		cluster      *Cluster
		expectedType map[PostgresRole]v1.ServiceType
	}{
		{
			subTest: "default configuration -> ClusterIP for both",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler: &acidv1.ConnectionPooler{},
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeClusterIP,
				Replica: v1.ServiceTypeClusterIP,
			},
		},
		{
			subTest: "LoadBalancer for both roles",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:                &acidv1.ConnectionPooler{},
				EnableMasterPoolerLoadBalancer:  boolToPointer(true),
				EnableReplicaPoolerLoadBalancer: boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeLoadBalancer,
				Replica: v1.ServiceTypeLoadBalancer,
			},
		},
		{
			subTest: "LoadBalancer for master",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:               &acidv1.ConnectionPooler{},
				EnableMasterPoolerLoadBalancer: boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeLoadBalancer,
				Replica: v1.ServiceTypeClusterIP,
			},
		},
		{
			subTest: "LoadBalancer for replica",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:                &acidv1.ConnectionPooler{},
				EnableReplicaPoolerLoadBalancer: boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeClusterIP,
				Replica: v1.ServiceTypeLoadBalancer,
			},
		},
		{
			subTest: "NodePort for both roles",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:            &acidv1.ConnectionPooler{},
				EnableMasterPoolerNodePort:  boolToPointer(true),
				EnableReplicaPoolerNodePort: boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeNodePort,
				Replica: v1.ServiceTypeNodePort,
			},
		},
		{
			subTest: "NodePort for master",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:           &acidv1.ConnectionPooler{},
				EnableMasterPoolerNodePort: boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeNodePort,
				Replica: v1.ServiceTypeClusterIP,
			},
		},
		{
			subTest: "NodePort for replica",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:            &acidv1.ConnectionPooler{},
				EnableReplicaPoolerNodePort: boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeClusterIP,
				Replica: v1.ServiceTypeNodePort,
			},
		},
		{
			subTest: "NodePort overrides LoadBalancer for both roles",
			spec: &acidv1.PostgresSpec{
				ConnectionPooler:                &acidv1.ConnectionPooler{},
				EnableMasterPoolerLoadBalancer:  boolToPointer(true),
				EnableReplicaPoolerLoadBalancer: boolToPointer(true),
				EnableMasterPoolerNodePort:      boolToPointer(true),
				EnableReplicaPoolerNodePort:     boolToPointer(true),
			},
			cluster: cluster,
			expectedType: map[PostgresRole]v1.ServiceType{
				Master:  v1.ServiceTypeNodePort,
				Replica: v1.ServiceTypeNodePort,
			},
		},
	}

	roles := []PostgresRole{Master, Replica}

	for _, tt := range tests {
		tt.cluster.Spec = *tt.spec

		for _, role := range roles {
			svc := tt.cluster.generateConnectionPoolerService(tt.cluster.ConnectionPooler[role])

			expected, ok := tt.expectedType[role]
			if !ok {
				t.Fatalf("%s [%s]: missing expectedType for role %v", testName, tt.subTest, role)
			}

			if svc.Spec.Type != expected {
				t.Errorf("%s [%s] role=%s: service Type is incorrect, got %s, expected %s",
					testName, tt.subTest, role, svc.Spec.Type, expected)
			}
		}
	}
}
