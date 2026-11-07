package k8sutil

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

func TestNodeHasAnyTaint(t *testing.T) {
	node := &v1.Node{Spec: v1.NodeSpec{Taints: []v1.Taint{
		{Key: "dedicated", Value: "postgres", Effect: v1.TaintEffectNoSchedule},
		{Key: "karpenter.sh/disrupted", Effect: v1.TaintEffectNoSchedule},
	}}}
	for _, tt := range []struct {
		name     string
		node     *v1.Node
		keys     []string
		expected bool
	}{
		{name: "no keys configured", node: node, keys: nil, expected: false},
		{name: "matching key", node: node, keys: []string{"karpenter.sh/disrupted"}, expected: true},
		{name: "one of several keys matches", node: node, keys: []string{"foo", "dedicated"}, expected: true},
		{name: "no matching key", node: node, keys: []string{"foo"}, expected: false},
		{name: "node without taints", node: &v1.Node{}, keys: []string{"karpenter.sh/disrupted"}, expected: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := NodeHasAnyTaint(tt.node, tt.keys); got != tt.expected {
				t.Errorf("expected %t, got %t", tt.expected, got)
			}
		})
	}
}
