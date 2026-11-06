package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestDeleteServicePropagationPolicy(t *testing.T) {
	clientSet := fake.NewSimpleClientset()
	client := k8sutil.KubernetesClient{
		ServicesGetter: clientSet.CoreV1(),
	}
	cluster := New(Config{}, client, acidv1.Postgresql{}, logger, eventRecorder)

	for _, role := range []PostgresRole{Master, Replica} {
		svc, err := clientSet.CoreV1().Services("default").Create(context.TODO(), &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "acid-test-" + string(role), Namespace: "default"},
		}, metav1.CreateOptions{})
		assert.NoError(t, err)
		cluster.Services[role] = svc

		clientSet.ClearActions()
		assert.NoError(t, cluster.deleteService(role))
		assert.Nil(t, cluster.Services[role])

		var deletes []k8stesting.DeleteActionImpl
		for _, action := range clientSet.Actions() {
			if deleteAction, ok := action.(k8stesting.DeleteActionImpl); ok && action.GetResource().Resource == "services" {
				deletes = append(deletes, deleteAction)
			}
		}
		if assert.Len(t, deletes, 1, "role %s", role) {
			policy := deletes[0].DeleteOptions.PropagationPolicy
			if assert.NotNil(t, policy, "role %s", role) {
				assert.Equal(t, metav1.DeletePropagationBackground, *policy, "role %s", role)
			}
		}
	}
}
