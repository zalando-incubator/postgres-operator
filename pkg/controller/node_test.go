package controller

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	readyLabel = "lifecycle-status"
	readyValue = "ready"
)

func newNodeTestController() *Controller {
	var controller = NewController(&spec.ControllerConfig{}, "node-test")
	return controller
}

func makeNode(labels map[string]string, isSchedulable bool, taints ...v1.Taint) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: v1.NamespaceDefault,
			Labels:    labels,
		},
		Spec: v1.NodeSpec{
			Unschedulable: !isSchedulable,
			Taints:        taints,
		},
	}
}

var nodeTestController = newNodeTestController()

func TestNodeIsReady(t *testing.T) {
	testName := "TestNodeIsReady"
	var testTable = []struct {
		in             *v1.Node
		out            bool
		readinessLabel map[string]string
	}{
		{
			in:             makeNode(map[string]string{"foo": "bar"}, true),
			out:            true,
			readinessLabel: map[string]string{readyLabel: readyValue},
		},
		{
			in:             makeNode(map[string]string{"foo": "bar"}, false),
			out:            false,
			readinessLabel: map[string]string{readyLabel: readyValue},
		},
		{
			in:             makeNode(map[string]string{readyLabel: readyValue}, false),
			out:            true,
			readinessLabel: map[string]string{readyLabel: readyValue},
		},
		{
			in:             makeNode(map[string]string{"foo": "bar", "master": "true"}, false),
			out:            true,
			readinessLabel: map[string]string{readyLabel: readyValue},
		},
		{
			in:             makeNode(map[string]string{"foo": "bar", "master": "true"}, false),
			out:            true,
			readinessLabel: map[string]string{readyLabel: readyValue},
		},
		{
			in:             makeNode(map[string]string{"foo": "bar"}, true),
			out:            true,
			readinessLabel: map[string]string{},
		},
		{
			in:             makeNode(map[string]string{"foo": "bar"}, false),
			out:            false,
			readinessLabel: map[string]string{},
		},
		{
			in:             makeNode(map[string]string{readyLabel: readyValue}, false),
			out:            false,
			readinessLabel: map[string]string{},
		},
		{
			in:             makeNode(map[string]string{"foo": "bar", "master": "true"}, false),
			out:            true,
			readinessLabel: map[string]string{},
		},
	}
	for _, tt := range testTable {
		nodeTestController.opConfig.NodeReadinessLabel = tt.readinessLabel
		if isReady := nodeTestController.nodeIsReady(tt.in); isReady != tt.out {
			t.Errorf("%s: expected response %t does not match the actual %t for the node %#v",
				testName, tt.out, isReady, tt.in)
		}
	}
}

func TestNodeIsReadyWithUnreadyTaints(t *testing.T) {
	disruptedTaint := v1.Taint{Key: "karpenter.sh/disrupted", Effect: v1.TaintEffectNoSchedule}
	otherTaint := v1.Taint{Key: "dedicated", Value: "postgres", Effect: v1.TaintEffectNoSchedule}
	unreadyTaints := []string{"karpenter.sh/disrupted"}

	tests := []struct {
		name           string
		in             *v1.Node
		unreadyTaints  []string
		readinessLabel map[string]string
		out            bool
	}{
		{
			name:          "schedulable node with unready taint is not ready",
			in:            makeNode(map[string]string{"foo": "bar"}, true, disruptedTaint),
			unreadyTaints: unreadyTaints,
			out:           false,
		},
		{
			name:          "schedulable node with other taint is ready",
			in:            makeNode(map[string]string{"foo": "bar"}, true, otherTaint),
			unreadyTaints: unreadyTaints,
			out:           true,
		},
		{
			name:          "unready taint is ignored when not configured",
			in:            makeNode(map[string]string{"foo": "bar"}, true, disruptedTaint),
			unreadyTaints: nil,
			out:           true,
		},
		{
			name:           "unready taint wins over readiness label",
			in:             makeNode(map[string]string{readyLabel: readyValue}, true, otherTaint, disruptedTaint),
			unreadyTaints:  unreadyTaints,
			readinessLabel: map[string]string{readyLabel: readyValue},
			out:            false,
		},
		{
			name:          "master node with unready taint is ready",
			in:            makeNode(map[string]string{"master": "true"}, true, disruptedTaint),
			unreadyTaints: unreadyTaints,
			out:           true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newNodeTestController()
			c.opConfig.NodeUnreadyTaints = tt.unreadyTaints
			c.opConfig.NodeReadinessLabel = tt.readinessLabel
			if isReady := c.nodeIsReady(tt.in); isReady != tt.out {
				t.Errorf("expected %t, got %t for node %#v", tt.out, isReady, tt.in)
			}
		})
	}
}

// TestMoveMasterPodsOffNodeRetriesOnError ensures a failed attempt to move
// master pods off a node is retried rather than aborting the whole retry
// loop on the first error.
func TestMoveMasterPodsOffNodeRetriesOnError(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	clientSet.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("could not list pods")
	})

	controller := newNodeTestController()
	controller.KubeClient.PodsGetter = clientSet.CoreV1()
	// timeout == the retry interval hardcoded in moveMasterPodsOffNode, so
	// the single retry attempt resolves synchronously without a real sleep.
	controller.opConfig.MasterPodMoveTimeout = &metav1.Duration{Duration: 1 * time.Minute}

	var logOutput bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&logOutput)
	controller.logger = logger.WithField("pkg", "controller")

	controller.moveMasterPodsOffNode(makeNode(map[string]string{}, false))

	if logOutput.Len() == 0 {
		t.Fatal("expected moveMasterPodsOffNode to log a warning")
	}
	if !strings.Contains(logOutput.String(), "still failing after") {
		t.Errorf("expected the retry loop to run out of attempts instead of aborting on the first error, got log output: %q", logOutput.String())
	}
}
