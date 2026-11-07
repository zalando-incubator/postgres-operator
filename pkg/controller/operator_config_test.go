package controller

import (
	"reflect"
	"testing"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
)

func TestImportNodeUnreadyTaintsFromCRD(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    []string
		expected []string
	}{
		{name: "unset defaults to the Karpenter disruption taint", input: nil, expected: []string{"karpenter.sh/disrupted"}},
		{name: "empty list disables the check", input: []string{}, expected: []string{}},
		{name: "custom taint keys", input: []string{"foo"}, expected: []string{"foo"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fromCRD := &acidv1.OperatorConfigurationData{}
			fromCRD.Kubernetes.NodeUnreadyTaints = tt.input
			got := newNodeTestController().importConfigurationFromCRD(fromCRD).NodeUnreadyTaints
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}
