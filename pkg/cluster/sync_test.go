package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/zalando/postgres-operator/v2/mocks"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/patroni"
	"k8s.io/client-go/kubernetes/fake"
)

var patroniLogger = logrus.New().WithField("test", "patroni")
var acidClientSet = fakeacidv1.NewSimpleClientset()
var clientSet = fake.NewSimpleClientset()

func newMockPod(ip string) *v1.Pod {
	return &v1.Pod{
		Status: v1.PodStatus{
			PodIP: ip,
		},
	}
}

func newFakeK8sSyncClient() (k8sutil.KubernetesClient, *fake.Clientset) {
	return k8sutil.KubernetesClient{
		PodsGetter:         clientSet.CoreV1(),
		PostgresqlsGetter:  acidClientSet.AcidV1(),
		StatefulSetsGetter: clientSet.AppsV1(),
	}, clientSet
}

func newFakeK8sSyncSecretsClient() (k8sutil.KubernetesClient, *fake.Clientset) {
	// add a reactor that checks namespace existence before creating secrets
	clientSet.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction := action.(k8stesting.CreateAction)
		secret := createAction.GetObject().(*v1.Secret)
		if secret.Namespace != "default" {
			return true, nil, errors.New("namespace does not exist")
		}
		return false, nil, nil
	})

	return k8sutil.KubernetesClient{
		SecretsGetter: clientSet.CoreV1(),
	}, clientSet
}

func TestSyncStatefulSetsAnnotations(t *testing.T) {
	testName := "test syncing statefulsets annotations"
	client, _ := newFakeK8sSyncClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	inheritedAnnotation := "environment"

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:        clusterName,
			Namespace:   namespace,
			Annotations: map[string]string{inheritedAnnotation: "test"},
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
				PodManagementPolicy: "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:         map[string]string{"application": "spilo"},
					ClusterNameLabel:      "cluster-name",
					DefaultCPURequest:     "300m",
					DefaultCPULimit:       "300m",
					DefaultMemoryRequest:  "300Mi",
					DefaultMemoryLimit:    "300Mi",
					InheritedAnnotations:  []string{inheritedAnnotation},
					PodRoleLabel:          "spilo-role",
					ResourceCheckInterval: &metav1.Duration{Duration: 3 * time.Second},
					ResourceCheckTimeout:  &metav1.Duration{Duration: 10 * time.Minute},
				},
			},
		}, client, pg, logger, eventRecorder)

	cluster.Name = clusterName
	cluster.Namespace = namespace

	// create a statefulset
	_, err := cluster.createStatefulSet()
	assert.NoError(t, err)

	// patch statefulset and add annotation
	patchData, err := metaAnnotationsPatch(map[string]string{"test-anno": "true"})
	assert.NoError(t, err)

	newSts, err := cluster.KubeClient.StatefulSets(namespace).Patch(
		context.TODO(),
		clusterName,
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"")
	assert.NoError(t, err)

	cluster.Statefulset = newSts

	// first compare running with desired statefulset - they should not match
	// because no inherited annotations or downscaler annotations are configured
	desiredSts, err := cluster.generateStatefulSet(&cluster.Postgresql.Spec)
	assert.NoError(t, err)

	cmp := cluster.compareStatefulSetWith(desiredSts)
	if cmp.match {
		t.Errorf("%s: match between current and desired statefulsets albeit differences: %#v", testName, cmp)
	}

	// now sync statefulset - the diff will trigger a replacement of the statefulset
	cluster.syncStatefulSet()

	// compare again after the SYNC - must be identical to the desired state
	cmp = cluster.compareStatefulSetWith(desiredSts)
	if !cmp.match {
		t.Errorf("%s: current and desired statefulsets are not matching %#v", testName, cmp)
	}

	// check if inherited annotation exists
	if _, exists := desiredSts.Annotations[inheritedAnnotation]; !exists {
		t.Errorf("%s: inherited annotation not found in desired statefulset: %#v", testName, desiredSts.Annotations)
	}
}

func TestPodAnnotationsSync(t *testing.T) {
	clusterName := "acid-test-cluster-2"
	namespace := "default"
	podAnnotation := "no-scale-down"
	podAnnotations := map[string]string{podAnnotation: "true"}
	customPodAnnotation := "foo"
	customPodAnnotations := map[string]string{customPodAnnotation: "true"}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := mocks.NewMockHTTPClient(ctrl)
	client, _ := newFakeK8sAnnotationsClient()

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
			EnableConnectionPooler:        boolToPointer(true),
			EnableLogicalBackup:           true,
			EnableReplicaConnectionPooler: boolToPointer(true),
			PodAnnotations:                podAnnotations,
			NumberOfInstances:             2,
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				PatroniAPICheckInterval: &metav1.Duration{Duration: 1 * time.Second},
				PatroniAPICheckTimeout:  &metav1.Duration{Duration: 5 * time.Second},
				PodManagementPolicy:     "ordered_ready",
				CustomPodAnnotations:    customPodAnnotations,
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				Resources: config.Resources{
					ClusterLabels:         map[string]string{"application": "spilo"},
					ClusterNameLabel:      "cluster-name",
					DefaultCPURequest:     "300m",
					DefaultCPULimit:       "300m",
					DefaultMemoryRequest:  "300Mi",
					DefaultMemoryLimit:    "300Mi",
					MaxInstances:          -1,
					PodRoleLabel:          "spilo-role",
					ResourceCheckInterval: &metav1.Duration{Duration: 3 * time.Second},
					ResourceCheckTimeout:  &metav1.Duration{Duration: 10 * time.Minute},
				},
			},
		}, client, pg, logger, eventRecorder)

	configJson := `{"postgresql": {"parameters": {"log_min_duration_statement": 200, "max_connections": 50}}}, "ttl": 20}`
	response := http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader([]byte(configJson))),
	}

	mockClient.EXPECT().Do(gomock.Any()).Return(&response, nil).AnyTimes()
	cluster.patroni = patroni.New(patroniLogger, mockClient)
	cluster.Name = clusterName
	cluster.Namespace = namespace
	clusterOptions := clusterLabelsOptions(cluster)

	// create a statefulset
	_, err := cluster.createStatefulSet()
	assert.NoError(t, err)
	// create a pods
	podsList := createPods(cluster)
	for _, pod := range podsList {
		_, err = cluster.KubeClient.Pods(namespace).Create(context.TODO(), &pod, metav1.CreateOptions{})
		assert.NoError(t, err)
	}
	// create connection pooler
	_, err = cluster.createConnectionPooler(mockInstallLookupFunction)
	assert.NoError(t, err)

	// create cron job
	err = cluster.createLogicalBackupJob()
	assert.NoError(t, err)

	annotateResources(cluster)
	err = cluster.Sync(&cluster.Postgresql)
	assert.NoError(t, err)

	// 1. PodAnnotations set
	stsList, err := cluster.KubeClient.StatefulSets(namespace).List(context.TODO(), clusterOptions)
	assert.NoError(t, err)
	for _, sts := range stsList.Items {
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.Contains(t, sts.Spec.Template.Annotations, annotation)
		}
	}

	for _, role := range []PostgresRole{Master, Replica} {
		deploy, err := cluster.KubeClient.Deployments(namespace).Get(context.TODO(), cluster.connectionPoolerName(role), metav1.GetOptions{})
		assert.NoError(t, err)
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.Contains(t, deploy.Spec.Template.Annotations, annotation,
				fmt.Sprintf("pooler deployment pod template %s should contain annotation %s, found %#v",
					deploy.Name, annotation, deploy.Spec.Template.Annotations))
		}
	}

	podList, err := cluster.KubeClient.Pods(namespace).List(context.TODO(), clusterOptions)
	assert.NoError(t, err)
	for _, pod := range podList.Items {
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.Contains(t, pod.Annotations, annotation,
				fmt.Sprintf("pod %s should contain annotation %s, found %#v", pod.Name, annotation, pod.Annotations))
		}
	}

	cronJobList, err := cluster.KubeClient.CronJobs(namespace).List(context.TODO(), clusterOptions)
	assert.NoError(t, err)
	for _, cronJob := range cronJobList.Items {
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.Contains(t, cronJob.Spec.JobTemplate.Spec.Template.Annotations, annotation,
				fmt.Sprintf("logical backup cron job's pod template should contain annotation %s, found %#v",
					annotation, cronJob.Spec.JobTemplate.Spec.Template.Annotations))
		}
	}

	// 2 PodAnnotations removed
	newSpec := cluster.Postgresql.DeepCopy()
	newSpec.Spec.PodAnnotations = nil
	cluster.OpConfig.CustomPodAnnotations = nil
	err = cluster.Sync(newSpec)
	assert.NoError(t, err)

	stsList, err = cluster.KubeClient.StatefulSets(namespace).List(context.TODO(), clusterOptions)
	assert.NoError(t, err)
	for _, sts := range stsList.Items {
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.NotContains(t, sts.Spec.Template.Annotations, annotation)
		}
	}

	for _, role := range []PostgresRole{Master, Replica} {
		deploy, err := cluster.KubeClient.Deployments(namespace).Get(context.TODO(), cluster.connectionPoolerName(role), metav1.GetOptions{})
		assert.NoError(t, err)
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.NotContains(t, deploy.Spec.Template.Annotations, annotation,
				fmt.Sprintf("pooler deployment pod template %s should not contain annotation %s, found %#v",
					deploy.Name, annotation, deploy.Spec.Template.Annotations))
		}
	}

	podList, err = cluster.KubeClient.Pods(namespace).List(context.TODO(), clusterOptions)
	assert.NoError(t, err)
	for _, pod := range podList.Items {
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.NotContains(t, pod.Annotations, annotation,
				fmt.Sprintf("pod %s should not contain annotation %s, found %#v", pod.Name, annotation, pod.Annotations))
		}
	}

	cronJobList, err = cluster.KubeClient.CronJobs(namespace).List(context.TODO(), clusterOptions)
	assert.NoError(t, err)
	for _, cronJob := range cronJobList.Items {
		for _, annotation := range []string{podAnnotation, customPodAnnotation} {
			assert.NotContains(t, cronJob.Spec.JobTemplate.Spec.Template.Annotations, annotation,
				fmt.Sprintf("logical backup cron job's pod template should not contain annotation %s, found %#v",
					annotation, cronJob.Spec.JobTemplate.Spec.Template.Annotations))
		}
	}
}

func TestCheckAndSetGlobalPostgreSQLConfiguration(t *testing.T) {
	testName := "test config comparison"
	client, _ := newFakeK8sSyncClient()
	clusterName := "acid-test-cluster"
	namespace := "default"
	testSlots := map[string]map[string]string{
		"slot1": {
			"type":     "logical",
			"plugin":   "wal2json",
			"database": "foo",
		},
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	defaultPgParameters := map[string]string{
		"log_min_duration_statement": "200",
		"max_connections":            "50",
	}
	defaultPatroniParameters := acidv1.Patroni{
		TTL: 20,
	}

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			Patroni: defaultPatroniParameters,
			PostgresqlParam: acidv1.PostgresqlParam{
				Parameters: defaultPgParameters,
			},
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				PodManagementPolicy: "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:         map[string]string{"application": "spilo"},
					ClusterNameLabel:      "cluster-name",
					DefaultCPURequest:     "300m",
					DefaultCPULimit:       "300m",
					DefaultMemoryRequest:  "300Mi",
					DefaultMemoryLimit:    "300Mi",
					PodRoleLabel:          "spilo-role",
					ResourceCheckInterval: &metav1.Duration{Duration: 3 * time.Second},
					ResourceCheckTimeout:  &metav1.Duration{Duration: 10 * time.Minute},
				},
			},
		}, client, pg, logger, eventRecorder)

	// mocking a config after setConfig is called
	configJson := `{"postgresql": {"parameters": {"log_min_duration_statement": 200, "max_connections": 50}}}, "ttl": 20}`
	r := io.NopCloser(bytes.NewReader([]byte(configJson)))

	response := http.Response{
		StatusCode: 200,
		Body:       r,
	}

	mockClient := mocks.NewMockHTTPClient(ctrl)
	mockClient.EXPECT().Do(gomock.Any()).Return(&response, nil).AnyTimes()

	p := patroni.New(patroniLogger, mockClient)
	cluster.patroni = p
	mockPod := newMockPod("192.168.100.1")

	// simulate existing config that differs from cluster.Spec
	tests := []struct {
		subtest         string
		patroni         acidv1.Patroni
		desiredSlots    map[string]map[string]string
		removedSlots    map[string]map[string]string
		pgParams        map[string]string
		shouldBePatched bool
		restartPrimary  bool
	}{
		{
			subtest: "Patroni and Postgresql.Parameters do not differ",
			patroni: acidv1.Patroni{
				TTL: 20,
			},
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "50",
			},
			shouldBePatched: false,
			restartPrimary:  false,
		},
		{
			subtest: "Patroni and Postgresql.Parameters differ - restart replica first",
			patroni: acidv1.Patroni{
				TTL: 30, // desired 20
			},
			pgParams: map[string]string{
				"log_min_duration_statement": "500", // desired 200
				"max_connections":            "100", // desired 50
			},
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest: "multiple Postgresql.Parameters differ - restart replica first",
			patroni: defaultPatroniParameters,
			pgParams: map[string]string{
				"log_min_duration_statement": "500", // desired 200
				"max_connections":            "100", // desired 50
			},
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest: "desired max_connections bigger - restart replica first",
			patroni: defaultPatroniParameters,
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "30", // desired 50
			},
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest: "desired max_connections smaller - restart master first",
			patroni: defaultPatroniParameters,
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "100", // desired 50
			},
			shouldBePatched: true,
			restartPrimary:  true,
		},
		{
			subtest: "slot does not exist but is desired",
			patroni: acidv1.Patroni{
				TTL: 20,
			},
			desiredSlots: testSlots,
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "50",
			},
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest: "slot exist, nothing specified in manifest",
			patroni: acidv1.Patroni{
				TTL: 20,
				Slots: map[string]map[string]string{
					"slot1": {
						"type":     "logical",
						"plugin":   "pgoutput",
						"database": "foo",
					},
				},
			},
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "50",
			},
			shouldBePatched: false,
			restartPrimary:  false,
		},
		{
			subtest: "slot is removed from manifest",
			patroni: acidv1.Patroni{
				TTL: 20,
				Slots: map[string]map[string]string{
					"slot1": {
						"type":     "logical",
						"plugin":   "pgoutput",
						"database": "foo",
					},
				},
			},
			removedSlots: testSlots,
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "50",
			},
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest: "slot plugin differs",
			patroni: acidv1.Patroni{
				TTL: 20,
				Slots: map[string]map[string]string{
					"slot1": {
						"type":     "logical",
						"plugin":   "pgoutput",
						"database": "foo",
					},
				},
			},
			desiredSlots: testSlots,
			pgParams: map[string]string{
				"log_min_duration_statement": "200",
				"max_connections":            "50",
			},
			shouldBePatched: true,
			restartPrimary:  false,
		},
	}

	for _, tt := range tests {
		if len(tt.desiredSlots) > 0 {
			cluster.Spec.Patroni.Slots = tt.desiredSlots
		}
		if len(tt.removedSlots) > 0 {
			for slotName, removedSlot := range tt.removedSlots {
				cluster.replicationSlots[slotName] = removedSlot
			}
		}

		configPatched, requirePrimaryRestart, err := cluster.checkAndSetGlobalPostgreSQLConfiguration(mockPod, tt.patroni, cluster.Spec.Patroni, tt.pgParams, cluster.Spec.Parameters)
		assert.NoError(t, err)
		if configPatched != tt.shouldBePatched {
			t.Errorf("%s - %s: expected config update did not happen", testName, tt.subtest)
		}
		if requirePrimaryRestart != tt.restartPrimary {
			t.Errorf("%s - %s: wrong master restart strategy, got restart %v, expected restart %v", testName, tt.subtest, requirePrimaryRestart, tt.restartPrimary)
		}

		// reset slots for next tests
		cluster.Spec.Patroni.Slots = nil
		cluster.replicationSlots = make(map[string]interface{})
	}

	testsFailsafe := []struct {
		subtest         string
		operatorVal     *bool
		effectiveVal    *bool
		desiredVal      bool
		shouldBePatched bool
		restartPrimary  bool
	}{
		{
			subtest:         "Not set in operator config, not set for pg cluster. Set to true in the pg config.",
			operatorVal:     nil,
			effectiveVal:    nil,
			desiredVal:      true,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Not set in operator config, disabled for pg cluster. Set to true in the pg config.",
			operatorVal:     nil,
			effectiveVal:    util.False(),
			desiredVal:      true,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Not set in operator config, not set for pg cluster. Set to false in the pg config.",
			operatorVal:     nil,
			effectiveVal:    nil,
			desiredVal:      false,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Not set in operator config, enabled for pg cluster. Set to false in the pg config.",
			operatorVal:     nil,
			effectiveVal:    util.True(),
			desiredVal:      false,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Enabled in operator config, not set for pg cluster. Set to false in the pg config.",
			operatorVal:     util.True(),
			effectiveVal:    nil,
			desiredVal:      false,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Enabled in operator config, disabled for pg cluster. Set to true in the pg config.",
			operatorVal:     util.True(),
			effectiveVal:    util.False(),
			desiredVal:      true,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Disabled in operator config, not set for pg cluster. Set to true in the pg config.",
			operatorVal:     util.False(),
			effectiveVal:    nil,
			desiredVal:      true,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Disabled in operator config, enabled for pg cluster. Set to false in the pg config.",
			operatorVal:     util.False(),
			effectiveVal:    util.True(),
			desiredVal:      false,
			shouldBePatched: true,
			restartPrimary:  false,
		},
		{
			subtest:         "Disabled in operator config, enabled for pg cluster. Set to true in the pg config.",
			operatorVal:     util.False(),
			effectiveVal:    util.True(),
			desiredVal:      true,
			shouldBePatched: false, // should not require patching
			restartPrimary:  false,
		},
	}

	for _, tt := range testsFailsafe {
		patroniConf := defaultPatroniParameters

		if tt.operatorVal != nil {
			cluster.OpConfig.EnablePatroniFailsafeMode = tt.operatorVal
		}
		if tt.effectiveVal != nil {
			patroniConf.FailsafeMode = tt.effectiveVal
		}
		cluster.Spec.Patroni.FailsafeMode = &tt.desiredVal

		configPatched, requirePrimaryRestart, err := cluster.checkAndSetGlobalPostgreSQLConfiguration(mockPod, patroniConf, cluster.Spec.Patroni, defaultPgParameters, cluster.Spec.Parameters)
		assert.NoError(t, err)
		if configPatched != tt.shouldBePatched {
			t.Errorf("%s - %s: expected update went wrong", testName, tt.subtest)
		}
		if requirePrimaryRestart != tt.restartPrimary {
			t.Errorf("%s - %s: wrong master restart strategy, got restart %v, expected restart %v", testName, tt.subtest, requirePrimaryRestart, tt.restartPrimary)
		}
	}
}

func TestSyncStandbyClusterConfiguration(t *testing.T) {
	client, _ := newFakeK8sSyncClient()
	clusterName := "acid-standby-cluster"
	applicationLabel := "spilo"
	namespace := "default"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			NumberOfInstances: int32(1),
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				PatroniAPICheckInterval: &metav1.Duration{Duration: 1 * time.Second},
				PatroniAPICheckTimeout:  &metav1.Duration{Duration: 5 * time.Second},
				PodManagementPolicy:     "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:         map[string]string{"application": applicationLabel},
					ClusterNameLabel:      "cluster-name",
					DefaultCPURequest:     "300m",
					DefaultCPULimit:       "300m",
					DefaultMemoryRequest:  "300Mi",
					DefaultMemoryLimit:    "300Mi",
					MinInstances:          int32(-1),
					MaxInstances:          int32(-1),
					PodRoleLabel:          "spilo-role",
					ResourceCheckInterval: &metav1.Duration{Duration: 3 * time.Second},
					ResourceCheckTimeout:  &metav1.Duration{Duration: 10 * time.Minute},
				},
			},
		}, client, pg, logger, eventRecorder)

	cluster.Name = clusterName
	cluster.Namespace = namespace

	// mocking a config after getConfig is called
	mockClient := mocks.NewMockHTTPClient(ctrl)
	configJson := `{"ttl": 20}`
	r := io.NopCloser(bytes.NewReader([]byte(configJson)))
	response := http.Response{
		StatusCode: 200,
		Body:       r,
	}
	mockClient.EXPECT().Get(gomock.Any()).Return(&response, nil).AnyTimes()

	// mocking a config after setConfig is called
	standbyJson := `{"standby_cluster":{"create_replica_methods":["bootstrap_standby_with_wale","basebackup_fast_xlog"],"restore_command":"envdir \"/run/etc/wal-e.d/env-standby\" /scripts/restore_command.sh \"%f\" \"%p\""}}`
	r = io.NopCloser(bytes.NewReader([]byte(standbyJson)))
	response = http.Response{
		StatusCode: 200,
		Body:       r,
	}
	mockClient.EXPECT().Do(gomock.Any()).Return(&response, nil).AnyTimes()
	p := patroni.New(patroniLogger, mockClient)
	cluster.patroni = p

	mockPod := newMockPod("192.168.100.1")
	mockPod.Name = fmt.Sprintf("%s-0", clusterName)
	mockPod.Namespace = namespace
	podLabels := map[string]string{
		"cluster-name": clusterName,
		"application":  applicationLabel,
		"spilo-role":   "master",
	}
	mockPod.Labels = podLabels
	client.PodsGetter.Pods(namespace).Create(context.TODO(), mockPod, metav1.CreateOptions{})

	// create a statefulset
	sts, err := cluster.createStatefulSet()
	assert.NoError(t, err)

	// check that pods do not have a STANDBY_* environment variable
	assert.NotContains(t, sts.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_METHOD", Value: "STANDBY_WITH_WALE"})

	// add standby section
	cluster.Spec.StandbyCluster = &acidv1.StandbyDescription{
		S3WalPath: "s3://custom/path/to/bucket/",
	}
	cluster.syncStatefulSet()
	updatedSts := cluster.Statefulset

	// check that pods do not have a STANDBY_* environment variable
	assert.Contains(t, updatedSts.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_METHOD", Value: "STANDBY_WITH_WALE"})

	// this should update the Patroni config
	err = cluster.syncStandbyClusterConfiguration()
	assert.NoError(t, err)

	configJson = `{"standby_cluster":{"create_replica_methods":["bootstrap_standby_with_wale","basebackup_fast_xlog"],"restore_command":"envdir \"/run/etc/wal-e.d/env-standby\" /scripts/restore_command.sh \"%f\" \"%p\""}, "ttl": 20}`
	r = io.NopCloser(bytes.NewReader([]byte(configJson)))
	response = http.Response{
		StatusCode: 200,
		Body:       r,
	}
	mockClient.EXPECT().Get(gomock.Any()).Return(&response, nil).AnyTimes()

	pods, err := cluster.listPods()
	assert.NoError(t, err)

	_, _, err = cluster.patroni.GetConfig(&pods[0])
	assert.NoError(t, err)
	// ToDo extend GetConfig to return standy_cluster setting to compare
	/*
		defaultStandbyParameters := map[string]interface{}{
			"create_replica_methods": []string{"bootstrap_standby_with_wale", "basebackup_fast_xlog"},
			"restore_command":        "envdir \"/run/etc/wal-e.d/env-standby\" /scripts/restore_command.sh \"%f\" \"%p\"",
		}
		assert.True(t, reflect.DeepEqual(defaultStandbyParameters, standbyCluster))
	*/
	// remove standby section
	cluster.Spec.StandbyCluster = &acidv1.StandbyDescription{}
	cluster.syncStatefulSet()
	updatedSts2 := cluster.Statefulset

	// check that pods do not have a STANDBY_* environment variable
	assert.NotContains(t, updatedSts2.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_METHOD", Value: "STANDBY_WITH_WALE"})

	// this should update the Patroni config again
	err = cluster.syncStandbyClusterConfiguration()
	assert.NoError(t, err)

	// test with standby_host, standby_port and standby_primary_slot_name
	cluster.Spec.StandbyCluster = &acidv1.StandbyDescription{
		StandbyHost:            "remote-primary.example.com",
		StandbyPort:            "5433",
		StandbyPrimarySlotName: "standby_slot",
	}
	cluster.syncStatefulSet()
	updatedSts4 := cluster.Statefulset

	// check that pods have all three STANDBY_* environment variables
	assert.Contains(t, updatedSts4.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_HOST", Value: "remote-primary.example.com"})
	assert.Contains(t, updatedSts4.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_PORT", Value: "5433"})
	assert.Contains(t, updatedSts4.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_PRIMARY_SLOT_NAME", Value: "standby_slot"})

	// this should update the Patroni config with host, port and primary_slot_name
	err = cluster.syncStandbyClusterConfiguration()
	assert.NoError(t, err)

	// test property deletion: remove standby_primary_slot_name
	cluster.Spec.StandbyCluster = &acidv1.StandbyDescription{
		StandbyHost: "remote-primary.example.com",
		StandbyPort: "5433",
	}
	cluster.syncStatefulSet()
	updatedSts5 := cluster.Statefulset

	// check that STANDBY_PRIMARY_SLOT_NAME is not present
	assert.Contains(t, updatedSts5.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_HOST", Value: "remote-primary.example.com"})
	assert.Contains(t, updatedSts5.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_PORT", Value: "5433"})
	assert.NotContains(t, updatedSts5.Spec.Template.Spec.Containers[0].Env, v1.EnvVar{Name: "STANDBY_PRIMARY_SLOT_NAME", Value: "standby_slot"})

	// this should update the Patroni config and set primary_slot_name to nil
	err = cluster.syncStandbyClusterConfiguration()
	assert.NoError(t, err)
}

func TestUpdateSecret(t *testing.T) {
	testName := "test syncing secrets"
	client, _ := newFakeK8sSyncSecretsClient()

	clusterName := "acid-test-cluster"
	namespace := "default"
	dbname := "app"
	dbowner := "appowner"
	appUser := "foo"
	secretTemplate := config.StringTemplate("{username}.{cluster}.credentials")
	retentionUsers := make([]string, 0)

	// define manifest users and enable rotation for dbowner
	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			Databases:                      map[string]string{dbname: dbowner},
			Users:                          map[string]acidv1.UserFlags{appUser: {}, "bar": {}, dbowner: {}, "not-exist.test_user": {}},
			UsersIgnoringSecretRotation:    []string{"bar"},
			UsersWithInPlaceSecretRotation: []string{dbowner},
			Streams: []acidv1.Stream{
				{
					ApplicationId: appId,
					Database:      dbname,
					Tables: map[string]acidv1.StreamTable{
						"data.foo": {
							EventType: "stream-type-b",
						},
					},
				},
			},
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	// new cluster with enabled password rotation
	var cluster = New(
		Config{
			OpConfig: config.Config{
				Auth: config.Auth{
					SuperUsername:                 "postgres",
					ReplicationUsername:           "standby",
					SecretNameTemplate:            secretTemplate,
					EnablePasswordRotation:        true,
					PasswordRotationInterval:      1,
					PasswordRotationUserRetention: 3,
				},
				EnableCrossNamespaceSecret: true,
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: "cluster-name",
				},
			},
		}, client, pg, logger, eventRecorder)

	cluster.Name = clusterName
	cluster.Namespace = namespace
	cluster.pgUsers = map[string]spec.PgUser{}

	// init all users
	cluster.initUsers()
	// create secrets
	cluster.syncSecrets()
	// initialize rotation with current time
	cluster.syncSecrets()

	dayAfterTomorrow := time.Now().AddDate(0, 0, 2)

	allUsers := make(map[string]spec.PgUser)
	for _, pgUser := range cluster.pgUsers {
		if !pgUser.Degraded {
			allUsers[pgUser.Name] = pgUser
		}
	}
	for _, systemUser := range cluster.systemUsers {
		allUsers[systemUser.Name] = systemUser
	}

	for username, pgUser := range allUsers {
		// first, get the secret
		secretName := cluster.credentialSecretName(username)
		secret, err := cluster.KubeClient.Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
		assert.NoError(t, err)
		secretPassword := string(secret.Data["password"])

		// now update the secret setting a next rotation date (tomorrow + interval)
		cluster.updateSecret(username, secret, &retentionUsers, dayAfterTomorrow)
		updatedSecret, err := cluster.KubeClient.Secrets(namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
		assert.NoError(t, err)

		// check that passwords are different
		rotatedPassword := string(updatedSecret.Data["password"])
		if secretPassword == rotatedPassword {
			// passwords for system users should not have been rotated
			if pgUser.Origin != spec.RoleOriginManifest {
				continue
			}
			if slices.Contains(pg.Spec.UsersIgnoringSecretRotation, username) {
				continue
			}
			t.Errorf("%s: password unchanged in updated secret for %s", testName, username)
		}

		// check that next rotation date is tomorrow + interval, not date in secret + interval
		nextRotation := string(updatedSecret.Data["nextRotation"])
		_, nextRotationDate := cluster.getNextRotationDate(dayAfterTomorrow)
		if nextRotation != nextRotationDate {
			t.Errorf("%s: updated secret of %s does not contain correct rotation date: expected %s, got %s", testName, username, nextRotationDate, nextRotation)
		}

		// compare username, when it's dbowner they should be equal because of UsersWithInPlaceSecretRotation
		secretUsername := string(updatedSecret.Data["username"])
		if pgUser.IsDbOwner {
			if secretUsername != username {
				t.Errorf("%s: username differs in updated secret: expected %s, got %s", testName, username, secretUsername)
			}
		} else {
			rotatedUsername := username + dayAfterTomorrow.Format(constants.RotationUserDateFormat)
			if secretUsername != rotatedUsername {
				t.Errorf("%s: updated secret does not contain correct username: expected %s, got %s", testName, rotatedUsername, secretUsername)
			}
			// whenever there's a rotation the retentionUsers list is extended or updated
			if len(retentionUsers) != 1 {
				t.Errorf("%s: unexpected number of users to drop - expected only %s, found %d", testName, username, len(retentionUsers))
			}
		}
	}

	// switch rotation for foo to in-place
	inPlaceRotationUsers := []string{dbowner, appUser}
	cluster.Spec.UsersWithInPlaceSecretRotation = inPlaceRotationUsers
	cluster.initUsers()
	cluster.syncSecrets()
	updatedSecret, err := cluster.KubeClient.Secrets(namespace).Get(context.TODO(), cluster.credentialSecretName(appUser), metav1.GetOptions{})
	assert.NoError(t, err)

	// username in secret should be switched to original user
	currentUsername := string(updatedSecret.Data["username"])
	if currentUsername != appUser {
		t.Errorf("%s: updated secret does not contain correct username: expected %s, got %s", testName, appUser, currentUsername)
	}

	// switch rotation back to rotation user
	inPlaceRotationUsers = []string{dbowner}
	cluster.Spec.UsersWithInPlaceSecretRotation = inPlaceRotationUsers
	cluster.initUsers()
	cluster.syncSecrets()
	updatedSecret, err = cluster.KubeClient.Secrets(namespace).Get(context.TODO(), cluster.credentialSecretName(appUser), metav1.GetOptions{})
	assert.NoError(t, err)

	// username in secret will only be switched after next rotation date is passed
	currentUsername = string(updatedSecret.Data["username"])
	if currentUsername != appUser {
		t.Errorf("%s: updated secret does not contain expected username: expected %s, got %s", testName, appUser, currentUsername)
	}
}

func TestUpdateSecretNameConflict(t *testing.T) {
	client, _ := newFakeK8sSyncSecretsClient()

	clusterName := "acid-test-cluster"
	namespace := "default"
	secretTemplate := config.StringTemplate("{username}.{cluster}.credentials")

	// define manifest user that has the same name as a prepared database owner user except for dashes vs underscores
	// because of this the operator cannot create both secrets because underscores are not allowed in k8s secret names
	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
		},
		Spec: acidv1.PostgresSpec{
			PreparedDatabases: map[string]acidv1.PreparedDatabase{"prepared": {DefaultUsers: true}},
			Users:             map[string]acidv1.UserFlags{"prepared-owner-user": {}},
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	var cluster = New(
		Config{
			OpConfig: config.Config{
				Auth: config.Auth{
					SuperUsername:       "postgres",
					ReplicationUsername: "standby",
					SecretNameTemplate:  secretTemplate,
				},
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: "cluster-name",
				},
			},
		}, client, pg, logger, eventRecorder)

	cluster.Name = clusterName
	cluster.Namespace = namespace
	cluster.pgUsers = map[string]spec.PgUser{}

	// init all users
	cluster.initUsers()
	// create secrets and fail because of user name mismatch
	// prepared-owner-user from manifest vs prepared_owner_user from prepared database
	err := cluster.syncSecrets()
	assert.Error(t, err)

	// the order of secrets to sync is not deterministic, check only first part of the error message
	expectedError := fmt.Sprintf("syncing secret %s failed: error while checking for password rotation: could not update secret because of user name mismatch", "default/prepared-owner-user.acid-test-cluster.credentials")
	assert.Contains(t, err.Error(), expectedError)
}

func TestBuildNewUsers(t *testing.T) {
	cluster := &Cluster{
		systemUsers: map[string]spec.PgUser{
			constants.SuperuserKeyName: {Name: "postgres"},
		},
		pgUsers: map[string]spec.PgUser{
			"alice": {Name: "alice", Origin: spec.RoleOriginTeamsAPI},
			"foo":   {Name: "foo260927", Rotated: true, MemberOf: []string{"foo"}},
		},
	}
	deletedUsers := map[string]string{"alice_deleted": "alice"}

	// first attempt: only the deprecated team member role exists in the database
	dbUsers := spec.PgUserMap{
		"alice_deleted": {Name: "alice_deleted", Deleted: true},
		"foo":           {Name: "foo", Parameters: map[string]string{"search_path": "data"}},
	}
	newUsers := cluster.buildNewUsers(dbUsers, deletedUsers)

	assert.Contains(t, newUsers, "postgres")
	assert.True(t, newUsers["alice"].Deleted, "alice should be skipped while alice_deleted is renamed back")
	assert.Equal(t, map[string]string{"search_path": "data"}, newUsers["foo260927"].Parameters)
	assert.NotContains(t, dbUsers, "foo", "group role of rotation user should be removed from dbUsers")

	// cluster users must stay untouched so that retries start from the same state
	assert.False(t, cluster.pgUsers["alice"].Deleted)
	assert.Nil(t, cluster.pgUsers["foo"].Parameters)

	// retry after the rename was applied: alice must be synced again
	dbUsers = spec.PgUserMap{
		"alice": {Name: "alice"},
		"foo":   {Name: "foo"},
	}
	newUsers = cluster.buildNewUsers(dbUsers, deletedUsers)
	assert.False(t, newUsers["alice"].Deleted, "alice should not be skipped once the role exists again")
}
