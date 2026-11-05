package cluster

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/zalando/postgres-operator/v2/mocks"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/patroni"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
)

func TestMigrateSingleMasterPod(t *testing.T) {
	for _, tt := range []struct {
		name          string
		newNode       string
		deleteError   error
		expectedError string
	}{
		{name: "relocated without switchover", newNode: "new-node"},
		{name: "deletion fails", deleteError: fmt.Errorf("delete failed"), expectedError: "delete failed"},
		{name: "pod remains on old node", newNode: "old-node", expectedError: "remained on the same node"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			podName := spec.NamespacedName{Namespace: "default", Name: "acid-test-cluster-0"}
			oldPod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: podName.Name, Namespace: podName.Namespace, Labels: map[string]string{"spilo-role": "master"}},
				Spec:       v1.PodSpec{NodeName: "old-node"},
				Status:     v1.PodStatus{PodIP: "192.0.2.1"},
			}
			newPod := oldPod.DeepCopy()
			newPod.Spec.NodeName = tt.newNode
			newPod.Status.PodIP = "192.0.2.2"
			client := k8sfake.NewSimpleClientset(oldPod,
				&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "old-node"}, Spec: v1.NodeSpec{Unschedulable: true}},
				&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "new-node"}},
			)
			opConfig := config.Config{}
			opConfig.PodRoleLabel = "spilo-role"
			opConfig.PodDeletionWaitTimeout = &metav1.Duration{Duration: time.Second}
			opConfig.PodLabelWaitTimeout = &metav1.Duration{Duration: time.Second}
			c := New(Config{OpConfig: opConfig}, k8sutil.KubernetesClient{PodsGetter: client.CoreV1(), NodesGetter: client.CoreV1()},
				acidv1.Postgresql{ObjectMeta: metav1.ObjectMeta{Name: "acid-test-cluster", Namespace: podName.Namespace}}, logger, record.NewFakeRecorder(2))
			replicas := int32(1)
			c.Statefulset = &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}
			// A single-member cluster must not make any Patroni switchover request,
			// especially to the IP of the deleted pod.
			c.patroni = patroni.New(patroniLogger, mocks.NewMockHTTPClient(gomock.NewController(t)))
			deletions := 0
			client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				deletions++
				if tt.deleteError != nil {
					return true, nil, tt.deleteError
				}
				ch := c.podSubscribers[podName]
				go func() {
					ch <- PodEvent{EventType: PodEventDelete, PrevPod: oldPod}
					ch <- PodEvent{EventType: PodEventAdd, CurPod: newPod}
				}()
				return true, nil, nil
			})
			err := c.MigrateMasterPod(podName)
			if tt.expectedError == "" {
				if err != nil {
					t.Fatalf("migration failed: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
				t.Fatalf("expected error containing %q, got %v", tt.expectedError, err)
			}
			if deletions != 1 {
				t.Fatalf("expected one pod recreation, got %d deletions", deletions)
			}
			if len(c.podSubscribers) != 0 {
				t.Fatal("pod event subscription was not removed")
			}
		})
	}
}

func TestMigrateMasterPodWithReplica(t *testing.T) {
	podName := spec.NamespacedName{Namespace: "default", Name: "acid-test-cluster-0"}
	master := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName.Name, Namespace: podName.Namespace, Labels: map[string]string{"spilo-role": "master"}},
		Spec:       v1.PodSpec{NodeName: "old-node"},
		Status:     v1.PodStatus{PodIP: "192.0.2.1"},
	}
	replica := master.DeepCopy()
	replica.Name = "acid-test-cluster-1"
	replica.Labels["spilo-role"] = "replica"
	replica.Spec.NodeName = "new-node"
	replica.Status.PodIP = "192.0.2.2"
	client := k8sfake.NewSimpleClientset(master, replica,
		&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "old-node"}, Spec: v1.NodeSpec{Unschedulable: true}},
		&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "new-node"}},
	)
	opConfig := config.Config{}
	opConfig.PodRoleLabel = "spilo-role"
	opConfig.PodLabelWaitTimeout = &metav1.Duration{Duration: time.Second}
	opConfig.PatroniAPICheckInterval = &metav1.Duration{Duration: time.Millisecond}
	opConfig.PatroniAPICheckTimeout = &metav1.Duration{Duration: time.Second}
	c := New(Config{OpConfig: opConfig}, k8sutil.KubernetesClient{PodsGetter: client.CoreV1(), NodesGetter: client.CoreV1()},
		acidv1.Postgresql{ObjectMeta: metav1.ObjectMeta{Name: "acid-test-cluster", Namespace: podName.Namespace}}, logger, record.NewFakeRecorder(2))
	replicas := int32(2)
	c.Statefulset = &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &replicas}}
	mockClient := mocks.NewMockHTTPClient(gomock.NewController(t))
	c.patroni = patroni.New(patroniLogger, mockClient)
	mockClient.EXPECT().Get("http://192.0.2.1:8008/cluster").Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(`{"members":[{"name":"acid-test-cluster-1","role":"replica","state":"streaming","lag":0}]}`)),
	}, nil)
	mockClient.EXPECT().Do(gomock.Any()).DoAndReturn(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		if req.Method != http.MethodPost || req.URL.String() != "http://192.0.2.1:8008/switchover" || !strings.Contains(string(body), `"member":"acid-test-cluster-1"`) {
			t.Fatalf("unexpected switchover: %s %s %s", req.Method, req.URL, body)
		}
		ch := c.podSubscribers[spec.NamespacedName{Namespace: replica.Namespace, Name: replica.Name}]
		promoted := replica.DeepCopy()
		promoted.Labels["spilo-role"] = "master"
		go func() { ch <- PodEvent{EventType: PodEventUpdate, CurPod: promoted} }()
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	if err := c.MigrateMasterPod(podName); err != nil {
		t.Fatalf("migration failed: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			t.Fatal("a healthy replica must not be recreated")
		}
	}
}

func TestGetSwitchoverCandidate(t *testing.T) {
	testName := "test getting right switchover candidate"
	namespace := "default"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var cluster = New(
		Config{
			OpConfig: config.Config{
				PatroniAPICheckInterval: &metav1.Duration{Duration: 1 * time.Second},
				PatroniAPICheckTimeout:  &metav1.Duration{Duration: 5 * time.Second},
			},
		}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)

	// simulate different member scenarios
	tests := []struct {
		subtest           string
		clusterJson       string
		syncModeEnabled   bool
		expectedCandidate spec.NamespacedName
		expectedError     error
	}{
		{
			subtest:           "choose sync_standby over replica",
			clusterJson:       `{"members": [{"name": "acid-test-cluster-0", "role": "leader", "state": "running", "api_url": "http://192.168.100.1:8008/patroni", "host": "192.168.100.1", "port": 5432, "timeline": 1}, {"name": "acid-test-cluster-1", "role": "sync_standby", "state": "streaming", "api_url": "http://192.168.100.2:8008/patroni", "host": "192.168.100.2", "port": 5432, "timeline": 1, "lag": 0}, {"name": "acid-test-cluster-2", "role": "replica", "state": "streaming", "api_url": "http://192.168.100.3:8008/patroni", "host": "192.168.100.3", "port": 5432, "timeline": 1, "lag": 0}]}`,
			syncModeEnabled:   true,
			expectedCandidate: spec.NamespacedName{Namespace: namespace, Name: "acid-test-cluster-1"},
			expectedError:     nil,
		},
		{
			subtest:           "no running sync_standby available",
			clusterJson:       `{"members": [{"name": "acid-test-cluster-0", "role": "leader", "state": "running", "api_url": "http://192.168.100.1:8008/patroni", "host": "192.168.100.1", "port": 5432, "timeline": 1}, {"name": "acid-test-cluster-1", "role": "replica", "state": "streaming", "api_url": "http://192.168.100.2:8008/patroni", "host": "192.168.100.2", "port": 5432, "timeline": 1, "lag": 0}]}`,
			syncModeEnabled:   true,
			expectedCandidate: spec.NamespacedName{},
			expectedError:     fmt.Errorf("failed to get Patroni cluster members: unexpected end of JSON input"),
		},
		{
			subtest:           "choose replica with lowest lag",
			clusterJson:       `{"members": [{"name": "acid-test-cluster-0", "role": "leader", "state": "running", "api_url": "http://192.168.100.1:8008/patroni", "host": "192.168.100.1", "port": 5432, "timeline": 1}, {"name": "acid-test-cluster-1", "role": "replica", "state": "streaming", "api_url": "http://192.168.100.2:8008/patroni", "host": "192.168.100.2", "port": 5432, "timeline": 1, "lag": 5}, {"name": "acid-test-cluster-2", "role": "replica", "state": "streaming", "api_url": "http://192.168.100.3:8008/patroni", "host": "192.168.100.3", "port": 5432, "timeline": 1, "lag": 2}]}`,
			syncModeEnabled:   false,
			expectedCandidate: spec.NamespacedName{Namespace: namespace, Name: "acid-test-cluster-2"},
			expectedError:     nil,
		},
		{
			subtest:           "choose first replica when lag is equal everywhere",
			clusterJson:       `{"members": [{"name": "acid-test-cluster-0", "role": "leader", "state": "running", "api_url": "http://192.168.100.1:8008/patroni", "host": "192.168.100.1", "port": 5432, "timeline": 1}, {"name": "acid-test-cluster-1", "role": "replica", "state": "streaming", "api_url": "http://192.168.100.2:8008/patroni", "host": "192.168.100.2", "port": 5432, "timeline": 1, "lag": 5}, {"name": "acid-test-cluster-2", "role": "replica", "state": "running", "api_url": "http://192.168.100.3:8008/patroni", "host": "192.168.100.3", "port": 5432, "timeline": 1, "lag": 5}]}`,
			syncModeEnabled:   false,
			expectedCandidate: spec.NamespacedName{Namespace: namespace, Name: "acid-test-cluster-1"},
			expectedError:     nil,
		},
		{
			subtest:           "no running replica available",
			clusterJson:       `{"members": [{"name": "acid-test-cluster-0", "role": "leader", "state": "running", "api_url": "http://192.168.100.1:8008/patroni", "host": "192.168.100.1", "port": 5432, "timeline": 2}, {"name": "acid-test-cluster-1", "role": "replica", "state": "starting", "api_url": "http://192.168.100.2:8008/patroni", "host": "192.168.100.2", "port": 5432, "timeline": 2}]}`,
			syncModeEnabled:   false,
			expectedCandidate: spec.NamespacedName{},
			expectedError:     fmt.Errorf("failed to get Patroni cluster members: unexpected end of JSON input"),
		},
		{
			subtest:           "replicas with different status",
			clusterJson:       `{"members": [{"name": "acid-test-cluster-0", "role": "leader", "state": "running", "api_url": "http://192.168.100.1:8008/patroni", "host": "192.168.100.1", "port": 5432, "timeline": 1}, {"name": "acid-test-cluster-1", "role": "replica", "state": "streaming", "api_url": "http://192.168.100.2:8008/patroni", "host": "192.168.100.2", "port": 5432, "timeline": 1, "lag": 5}, {"name": "acid-test-cluster-2", "role": "replica", "state": "in archive recovery", "api_url": "http://192.168.100.3:8008/patroni", "host": "192.168.100.3", "port": 5432, "timeline": 1, "lag": 2}]}`,
			syncModeEnabled:   false,
			expectedCandidate: spec.NamespacedName{Namespace: namespace, Name: "acid-test-cluster-2"},
			expectedError:     nil,
		},
	}

	for _, tt := range tests {
		// mocking cluster members
		r := io.NopCloser(bytes.NewReader([]byte(tt.clusterJson)))

		response := http.Response{
			StatusCode: 200,
			Body:       r,
		}

		mockClient := mocks.NewMockHTTPClient(ctrl)
		mockClient.EXPECT().Get(gomock.Any()).Return(&response, nil).AnyTimes()

		p := patroni.New(patroniLogger, mockClient)
		cluster.patroni = p
		mockMasterPod := newMockPod("192.168.100.1")
		mockMasterPod.Namespace = namespace
		cluster.Spec.Patroni.SynchronousMode = tt.syncModeEnabled

		candidate, err := cluster.getSwitchoverCandidate(mockMasterPod)
		if err != nil && err.Error() != tt.expectedError.Error() {
			t.Errorf("%s - %s: unexpected error, %v", testName, tt.subtest, err)
		}

		if candidate != tt.expectedCandidate {
			t.Errorf("%s - %s: unexpect switchover candidate, got %s, expected %s", testName, tt.subtest, candidate, tt.expectedCandidate)
		}
	}
}

func TestPodIsNotRunning(t *testing.T) {
	tests := []struct {
		subtest  string
		pod      v1.Pod
		expected bool
	}{
		{
			subtest: "pod with no status reported yet",
			pod: v1.Pod{
				Status: v1.PodStatus{},
			},
			expected: false,
		},
		{
			subtest: "pod running with all containers ready",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			subtest: "pod in pending phase",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodPending,
				},
			},
			expected: true,
		},
		{
			subtest: "pod running but container in CreateContainerConfigError",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Waiting: &v1.ContainerStateWaiting{
									Reason:  "CreateContainerConfigError",
									Message: `secret "some-secret" not found`,
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			subtest: "pod running but container in CrashLoopBackOff",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Waiting: &v1.ContainerStateWaiting{
									Reason: "CrashLoopBackOff",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			subtest: "pod running but container terminated",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Terminated: &v1.ContainerStateTerminated{
									ExitCode: 137,
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			subtest: "pod running with mixed container states - one healthy one broken",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{},
							},
						},
						{
							State: v1.ContainerState{
								Waiting: &v1.ContainerStateWaiting{
									Reason: "CreateContainerConfigError",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			subtest: "pod in failed phase",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodFailed,
				},
			},
			expected: true,
		},
		{
			subtest: "pod running with multiple healthy containers",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{},
							},
						},
						{
							State: v1.ContainerState{
								Running: &v1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			subtest: "pod running with ImagePullBackOff",
			pod: v1.Pod{
				Status: v1.PodStatus{
					Phase: v1.PodRunning,
					ContainerStatuses: []v1.ContainerStatus{
						{
							State: v1.ContainerState{
								Waiting: &v1.ContainerStateWaiting{
									Reason: "ImagePullBackOff",
								},
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.subtest, func(t *testing.T) {
			result := podIsNotRunning(&tt.pod)
			if result != tt.expected {
				t.Errorf("podIsNotRunning() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestAllPodsRunning(t *testing.T) {
	client, _ := newFakeK8sSyncClient()

	var cluster = New(
		Config{
			OpConfig: config.Config{
				Resources: config.Resources{
					ClusterLabels:    map[string]string{"application": "spilo"},
					ClusterNameLabel: "cluster-name",
					PodRoleLabel:     "spilo-role",
				},
			},
		}, client, acidv1.Postgresql{}, logger, eventRecorder)

	tests := []struct {
		subtest  string
		pods     []v1.Pod
		expected bool
	}{
		{
			subtest: "all pods running",
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}},
						},
					},
				},
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}},
						},
					},
				},
			},
			expected: true,
		},
		{
			subtest: "one pod not running",
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}},
						},
					},
				},
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Waiting: &v1.ContainerStateWaiting{
										Reason: "CreateContainerConfigError",
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			subtest: "all pods not running",
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{
						Phase: v1.PodPending,
					},
				},
				{
					Status: v1.PodStatus{
						Phase: v1.PodRunning,
						ContainerStatuses: []v1.ContainerStatus{
							{
								State: v1.ContainerState{
									Waiting: &v1.ContainerStateWaiting{
										Reason: "CrashLoopBackOff",
									},
								},
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			subtest:  "empty pod list",
			pods:     []v1.Pod{},
			expected: true,
		},
		{
			subtest: "pods with no status reported yet",
			pods: []v1.Pod{
				{
					Status: v1.PodStatus{},
				},
				{
					Status: v1.PodStatus{},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.subtest, func(t *testing.T) {
			result := cluster.allPodsRunning(tt.pods)
			if result != tt.expected {
				t.Errorf("allPodsRunning() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestRecreatePodsReadinessCheck(t *testing.T) {
	ready := v1.PodStatus{Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Hour))}}}
	justReady := v1.PodStatus{Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue, LastTransitionTime: metav1.Now()}}}
	notReady := v1.PodStatus{Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}}
	// a crashlooping container without readiness probe is ready for a moment after every restart
	crashLooping := *ready.DeepCopy()
	crashLooping.ContainerStatuses = []v1.ContainerStatus{{Name: "exporter", RestartCount: 1}}

	for _, tt := range []struct {
		name              string
		readinessCheck    bool
		roles             []string
		firstPodStatus    v1.PodStatus
		failedGets        int
		expectedDeletions int
		expectedError     string
	}{
		{name: "rotates all pods when they become available", readinessCheck: true, roles: []string{"replica", "replica"}, firstPodStatus: ready, expectedDeletions: 2},
		{name: "retries when the pod cannot be read", readinessCheck: true, roles: []string{"replica", "replica"}, firstPodStatus: ready, failedGets: 1, expectedDeletions: 2},
		{name: "stops after a crashlooping pod", readinessCheck: true, roles: []string{"replica", "replica"}, firstPodStatus: crashLooping, expectedDeletions: 1, expectedError: `container "exporter" has restarted 1 time(s)`},
		{name: "stops after a pod that does not become ready", readinessCheck: true, roles: []string{"replica", "replica"}, firstPodStatus: notReady, expectedDeletions: 1, expectedError: "did not become available within"},
		{name: "stops after a pod that is ready for less than the min ready time", readinessCheck: true, roles: []string{"replica", "replica"}, firstPodStatus: justReady, expectedDeletions: 1, expectedError: "did not become available within"},
		{name: "reports a crashlooping old master", readinessCheck: true, roles: []string{"master"}, firstPodStatus: crashLooping, expectedDeletions: 1, expectedError: `could not recreate old master pod "default/acid-test-cluster-1"`},
		{name: "ignores readiness when disabled", readinessCheck: false, roles: []string{"replica", "replica"}, firstPodStatus: crashLooping, expectedDeletions: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pods := make([]v1.Pod, 0)
			objects := make([]runtime.Object, 0)
			for i, role := range tt.roles {
				status := ready
				if i == 0 {
					status = tt.firstPodStatus
				}
				pod := v1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("acid-test-cluster-%d", i+1), Namespace: "default", Labels: map[string]string{"spilo-role": role}},
					Status:     status,
				}
				pods = append(pods, pod)
				objects = append(objects, pod.DeepCopy())
			}
			client := k8sfake.NewSimpleClientset(objects...)
			opConfig := config.Config{}
			opConfig.PodRoleLabel = "spilo-role"
			opConfig.PodDeletionWaitTimeout = &metav1.Duration{Duration: time.Second}
			opConfig.PodLabelWaitTimeout = &metav1.Duration{Duration: time.Second}
			opConfig.ResourceCheckInterval = &metav1.Duration{Duration: 10 * time.Millisecond}
			opConfig.ResourceCheckTimeout = &metav1.Duration{Duration: 50 * time.Millisecond}
			opConfig.EnableRollingUpdateReadinessCheck = tt.readinessCheck
			opConfig.RollingUpdateMinReadyTime = &metav1.Duration{Duration: time.Minute}
			c := New(Config{OpConfig: opConfig}, k8sutil.KubernetesClient{PodsGetter: client.CoreV1()},
				acidv1.Postgresql{ObjectMeta: metav1.ObjectMeta{Name: "acid-test-cluster", Namespace: "default"}}, logger, record.NewFakeRecorder(2))
			failedGets := 0
			client.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if failedGets < tt.failedGets {
					failedGets++
					return true, nil, fmt.Errorf("connection refused")
				}
				return false, nil, nil
			})
			deletions := 0
			client.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				deletions++
				podName := spec.NamespacedName{Namespace: action.GetNamespace(), Name: action.(k8stesting.DeleteAction).GetName()}
				ch := c.podSubscribers[podName]
				for i := range pods {
					if pods[i].Name == podName.Name {
						recreated := &pods[i]
						go func() {
							ch <- PodEvent{EventType: PodEventDelete, PrevPod: recreated}
							ch <- PodEvent{EventType: PodEventAdd, CurPod: recreated}
						}()
					}
				}
				return true, nil, nil
			})

			err := c.recreatePods(pods, nil)
			if tt.expectedError == "" {
				if err != nil {
					t.Fatalf("rolling update failed: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
				t.Fatalf("expected error containing %q, got %v", tt.expectedError, err)
			}
			if deletions != tt.expectedDeletions {
				t.Fatalf("expected %d pod deletions, got %d", tt.expectedDeletions, deletions)
			}
		})
	}
}

func TestNotAvailableRotatedPods(t *testing.T) {
	opConfig := config.Config{}
	opConfig.RollingUpdateMinReadyTime = &metav1.Duration{Duration: time.Minute}
	c := New(Config{OpConfig: opConfig}, k8sutil.KubernetesClient{}, acidv1.Postgresql{}, logger, eventRecorder)
	// readyFor of 0 means the pod is not ready
	podReadyFor := func(name string, readyFor time.Duration, annotation string) v1.Pod {
		pod := v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if readyFor > 0 {
			pod.Status.Conditions = []v1.PodCondition{{
				Type:               v1.PodReady,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-readyFor)),
			}}
		}
		if annotation != "" {
			pod.Annotations = map[string]string{rollingUpdatePodAnnotationKey: annotation}
		}
		return pod
	}

	for _, tt := range []struct {
		name     string
		pods     []v1.Pod
		expected []string
	}{
		{
			name:     "all rotated pods available",
			pods:     []v1.Pod{podReadyFor("pod-0", time.Hour, ""), podReadyFor("pod-1", 0, "true")},
			expected: []string{},
		},
		{
			name:     "rotated pod not ready",
			pods:     []v1.Pod{podReadyFor("pod-0", time.Hour, "true"), podReadyFor("pod-1", 0, ""), podReadyFor("pod-2", time.Hour, "")},
			expected: []string{"pod-1"},
		},
		{
			name:     "rotated pod ready for less than the min ready time",
			pods:     []v1.Pod{podReadyFor("pod-0", 10*time.Second, ""), podReadyFor("pod-1", time.Hour, "")},
			expected: []string{"pod-0"},
		},
		{
			name:     "pods pending lazy upgrade are not rotated yet",
			pods:     []v1.Pod{podReadyFor("pod-0", 0, rollingUpdateLazyUpgradeValue)},
			expected: []string{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if result := c.notAvailableRotatedPods(tt.pods); !slices.Equal(result, tt.expected) {
				t.Errorf("notAvailableRotatedPods() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
