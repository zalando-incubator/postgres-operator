package k8sutil

import (
	"context"
	"fmt"

	b64 "encoding/base64"
	"encoding/json"

	apiacidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	zalandoclient "github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned/typed/acid.zalan.do/v1"
	zalandov1 "github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned/typed/zalando.org/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	apiappsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextv1client "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	appsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	batchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	policyv1 "k8s.io/client-go/kubernetes/typed/policy/v1"
	rbacv1 "k8s.io/client-go/kubernetes/typed/rbac/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func Int32ToPointer(value int32) *int32 {
	return &value
}

func UInt32ToPointer(value uint32) *uint32 {
	return &value
}

func StringToPointer(str string) *string {
	return &str
}

// KubernetesClient describes getters for Kubernetes objects
type KubernetesClient struct {
	corev1.SecretsGetter
	corev1.ServicesGetter
	corev1.EndpointsGetter
	corev1.PodsGetter
	corev1.PersistentVolumesGetter
	corev1.PersistentVolumeClaimsGetter
	corev1.ConfigMapsGetter
	corev1.NodesGetter
	corev1.NamespacesGetter
	corev1.ServiceAccountsGetter
	corev1.EventsGetter
	appsv1.StatefulSetsGetter
	appsv1.DeploymentsGetter
	rbacv1.RoleBindingsGetter
	batchv1.CronJobsGetter
	policyv1.PodDisruptionBudgetsGetter
	apiextv1client.CustomResourceDefinitionsGetter
	acidv1.OperatorConfigurationsGetter
	acidv1.PostgresTeamsGetter
	acidv1.PostgresqlsGetter
	zalandov1.FabricEventStreamsGetter

	RESTClient         rest.Interface
	Clientset          *kubernetes.Clientset
	AcidV1ClientSet    *zalandoclient.Clientset
	Zalandov1ClientSet *zalandoclient.Clientset
}

type mockCustomResourceDefinition struct {
	apiextv1client.CustomResourceDefinitionInterface
}

type MockCustomResourceDefinitionsGetter struct {
}

type mockSecret struct {
	corev1.SecretInterface
}

type MockSecretGetter struct {
}

type mockDeployment struct {
	appsv1.DeploymentInterface
}

type mockDeploymentNotExist struct {
	appsv1.DeploymentInterface
}

type MockDeploymentGetter struct {
}

type MockDeploymentNotExistGetter struct {
}

type mockService struct {
	corev1.ServiceInterface
}

type mockServiceNotExist struct {
	corev1.ServiceInterface
}

type MockServiceGetter struct {
}

type MockServiceNotExistGetter struct {
}

type mockConfigMap struct {
	corev1.ConfigMapInterface
}

type MockConfigMapsGetter struct {
}

// RestConfig creates REST config
func RestConfig(kubeConfig string, outOfCluster bool) (*rest.Config, error) {
	if outOfCluster {
		return clientcmd.BuildConfigFromFlags("", kubeConfig)
	}

	return rest.InClusterConfig()
}

// ResourceAlreadyExists checks if error corresponds to Already exists error
func ResourceAlreadyExists(err error) bool {
	return apierrors.IsAlreadyExists(err)
}

// ResourceNotFound checks if error corresponds to Not found error
func ResourceNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

// NewFromConfig create Kubernetes Interface using REST config
func NewFromConfig(cfg *rest.Config) (KubernetesClient, error) {
	kubeClient := KubernetesClient{}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return kubeClient, fmt.Errorf("could not get clientset: %v", err)
	}

	kubeClient.Clientset = client
	kubeClient.PodsGetter = client.CoreV1()
	kubeClient.ServicesGetter = client.CoreV1()
	kubeClient.EndpointsGetter = client.CoreV1()
	kubeClient.SecretsGetter = client.CoreV1()
	kubeClient.ServiceAccountsGetter = client.CoreV1()
	kubeClient.ConfigMapsGetter = client.CoreV1()
	kubeClient.PersistentVolumeClaimsGetter = client.CoreV1()
	kubeClient.PersistentVolumesGetter = client.CoreV1()
	kubeClient.NodesGetter = client.CoreV1()
	kubeClient.NamespacesGetter = client.CoreV1()
	kubeClient.StatefulSetsGetter = client.AppsV1()
	kubeClient.DeploymentsGetter = client.AppsV1()
	kubeClient.PodDisruptionBudgetsGetter = client.PolicyV1()
	kubeClient.RESTClient = client.CoreV1().RESTClient()
	kubeClient.RoleBindingsGetter = client.RbacV1()
	kubeClient.CronJobsGetter = client.BatchV1()
	kubeClient.EventsGetter = client.CoreV1()

	apiextClient, err := apiextclient.NewForConfig(cfg)
	if err != nil {
		return kubeClient, fmt.Errorf("could not create api client:%v", err)
	}

	kubeClient.CustomResourceDefinitionsGetter = apiextClient.ApiextensionsV1()

	kubeClient.AcidV1ClientSet = zalandoclient.NewForConfigOrDie(cfg)
	if err != nil {
		return kubeClient, fmt.Errorf("could not create acid.zalan.do clientset: %v", err)
	}
	kubeClient.Zalandov1ClientSet = zalandoclient.NewForConfigOrDie(cfg)
	if err != nil {
		return kubeClient, fmt.Errorf("could not create zalando.org clientset: %v", err)
	}

	kubeClient.OperatorConfigurationsGetter = kubeClient.AcidV1ClientSet.AcidV1()
	kubeClient.PostgresTeamsGetter = kubeClient.AcidV1ClientSet.AcidV1()
	kubeClient.PostgresqlsGetter = kubeClient.AcidV1ClientSet.AcidV1()
	kubeClient.FabricEventStreamsGetter = kubeClient.Zalandov1ClientSet.ZalandoV1()

	return kubeClient, nil
}

// SetPostgresCRDStatus of Postgres cluster
func (client *KubernetesClient) SetPostgresCRDStatus(clusterName spec.NamespacedName, pg *apiacidv1.Postgresql) (*apiacidv1.Postgresql, error) {
	pg, err := client.PostgresqlsGetter.Postgresqls(clusterName.Namespace).UpdateStatus(context.TODO(), pg, metav1.UpdateOptions{})
	if err != nil {
		return pg, fmt.Errorf("could not update status: %v", err)
	}

	return pg, nil
}

// SetFinalizer of Postgres cluster
func (client *KubernetesClient) SetFinalizer(clusterName spec.NamespacedName, pg *apiacidv1.Postgresql, finalizers []string) (*apiacidv1.Postgresql, error) {
	var (
		updatedPg *apiacidv1.Postgresql
		patch     []byte
		err       error
	)
	pg.ObjectMeta.Finalizers = finalizers

	if len(finalizers) > 0 {
		patch, err = json.Marshal(struct {
			PgMetadata interface{} `json:"metadata"`
		}{&pg.ObjectMeta})
		if err != nil {
			return pg, fmt.Errorf("could not marshal ObjectMeta: %v", err)
		}

		updatedPg, err = client.PostgresqlsGetter.Postgresqls(clusterName.Namespace).Patch(
			context.TODO(), clusterName.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	} else {
		// in case finalizers are empty and update is needed to remove
		updatedPg, err = client.PostgresqlsGetter.Postgresqls(clusterName.Namespace).Update(
			context.TODO(), pg, metav1.UpdateOptions{})
	}
	if err != nil {
		return updatedPg, fmt.Errorf("could not set finalizer: %v", err)
	}

	return updatedPg, nil
}

func (c *mockCustomResourceDefinition) Get(ctx context.Context, name string, options metav1.GetOptions) (*apiextv1.CustomResourceDefinition, error) {
	return &apiextv1.CustomResourceDefinition{}, nil
}

func (c *mockCustomResourceDefinition) Create(ctx context.Context, crd *apiextv1.CustomResourceDefinition, options metav1.CreateOptions) (*apiextv1.CustomResourceDefinition, error) {
	return &apiextv1.CustomResourceDefinition{}, nil
}

func (mock *MockCustomResourceDefinitionsGetter) CustomResourceDefinitions() apiextv1client.CustomResourceDefinitionInterface {
	return &mockCustomResourceDefinition{}
}

func (c *mockSecret) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.Secret, error) {
	oldFormatSecret := &v1.Secret{}
	oldFormatSecret.Name = "testcluster"
	oldFormatSecret.Data = map[string][]byte{
		"user1":     []byte("testrole"),
		"password1": []byte("testpassword"),
		"inrole1":   []byte("testinrole"),
		"foobar":    []byte(b64.StdEncoding.EncodeToString([]byte("password"))),
	}

	newFormatSecret := &v1.Secret{}
	newFormatSecret.Name = "test-secret-new-format"
	newFormatSecret.Data = map[string][]byte{
		"user":       []byte("new-test-role"),
		"password":   []byte("new-test-password"),
		"inrole":     []byte("new-test-inrole"),
		"new-foobar": []byte(b64.StdEncoding.EncodeToString([]byte("password"))),
	}

	secrets := map[string]*v1.Secret{
		"infrastructureroles-old-test": oldFormatSecret,
		"infrastructureroles-new-test": newFormatSecret,
	}

	for idx := 1; idx <= 2; idx++ {
		newFormatStandaloneSecret := &v1.Secret{}
		newFormatStandaloneSecret.Name = fmt.Sprintf("test-secret-new-format%d", idx)
		newFormatStandaloneSecret.Data = map[string][]byte{
			"user":     []byte(fmt.Sprintf("new-test-role%d", idx)),
			"password": []byte(fmt.Sprintf("new-test-password%d", idx)),
			"inrole":   []byte(fmt.Sprintf("new-test-inrole%d", idx)),
		}

		secrets[fmt.Sprintf("infrastructureroles-new-test%d", idx)] =
			newFormatStandaloneSecret
	}

	if secret, exists := secrets[name]; exists {
		return secret, nil
	}

	return nil, fmt.Errorf("NotFound")

}

func (c *mockConfigMap) Get(ctx context.Context, name string, options metav1.GetOptions) (*v1.ConfigMap, error) {
	oldFormatConfigmap := &v1.ConfigMap{}
	oldFormatConfigmap.Name = "testcluster"
	oldFormatConfigmap.Data = map[string]string{
		"foobar": "{}",
	}

	newFormatConfigmap := &v1.ConfigMap{}
	newFormatConfigmap.Name = "testcluster"
	newFormatConfigmap.Data = map[string]string{
		"new-foobar": "{\"user_flags\": [\"createdb\"]}",
	}

	configmaps := map[string]*v1.ConfigMap{
		"infrastructureroles-old-test": oldFormatConfigmap,
		"infrastructureroles-new-test": newFormatConfigmap,
	}

	if configmap, exists := configmaps[name]; exists {
		return configmap, nil
	}

	return nil, fmt.Errorf("NotFound")
}

// Secrets to be mocked
func (mock *MockSecretGetter) Secrets(namespace string) corev1.SecretInterface {
	return &mockSecret{}
}

// ConfigMaps to be mocked
func (mock *MockConfigMapsGetter) ConfigMaps(namespace string) corev1.ConfigMapInterface {
	return &mockConfigMap{}
}

func (mock *MockDeploymentGetter) Deployments(namespace string) appsv1.DeploymentInterface {
	return &mockDeployment{}
}

func (mock *MockDeploymentNotExistGetter) Deployments(namespace string) appsv1.DeploymentInterface {
	return &mockDeploymentNotExist{}
}

func (mock *mockDeployment) Create(context.Context, *apiappsv1.Deployment, metav1.CreateOptions) (*apiappsv1.Deployment, error) {
	return &apiappsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-deployment",
		},
		Spec: apiappsv1.DeploymentSpec{
			Replicas: Int32ToPointer(1),
		},
	}, nil
}

func (mock *mockDeployment) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return nil
}

func (mock *mockDeployment) Get(ctx context.Context, name string, opts metav1.GetOptions) (*apiappsv1.Deployment, error) {
	return &apiappsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-deployment",
		},
		Spec: apiappsv1.DeploymentSpec{
			Replicas: Int32ToPointer(1),
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Image: "pooler:1.0",
						},
					},
				},
			},
		},
	}, nil
}

func (mock *mockDeployment) Patch(ctx context.Context, name string, t types.PatchType, data []byte, opts metav1.PatchOptions, subres ...string) (*apiappsv1.Deployment, error) {
	return &apiappsv1.Deployment{
		Spec: apiappsv1.DeploymentSpec{
			Replicas: Int32ToPointer(2),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-deployment",
		},
	}, nil
}

func (mock *mockDeploymentNotExist) Get(ctx context.Context, name string, opts metav1.GetOptions) (*apiappsv1.Deployment, error) {
	return nil, &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Reason: metav1.StatusReasonNotFound,
		},
	}
}

func (mock *mockDeploymentNotExist) Create(context.Context, *apiappsv1.Deployment, metav1.CreateOptions) (*apiappsv1.Deployment, error) {
	return &apiappsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-deployment",
		},
		Spec: apiappsv1.DeploymentSpec{
			Replicas: Int32ToPointer(1),
		},
	}, nil
}

func (mock *MockServiceGetter) Services(namespace string) corev1.ServiceInterface {
	return &mockService{}
}

func (mock *MockServiceNotExistGetter) Services(namespace string) corev1.ServiceInterface {
	return &mockServiceNotExist{}
}

func (mock *mockService) Create(context.Context, *v1.Service, metav1.CreateOptions) (*v1.Service, error) {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-service",
		},
	}, nil
}

func (mock *mockService) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return nil
}

func (mock *mockService) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.Service, error) {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-service",
		},
	}, nil
}

func (mock *mockServiceNotExist) Create(context.Context, *v1.Service, metav1.CreateOptions) (*v1.Service, error) {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-service",
		},
	}, nil
}

func (mock *mockServiceNotExist) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1.Service, error) {
	return nil, &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Reason: metav1.StatusReasonNotFound,
		},
	}
}

// NewMockKubernetesClient for other tests
func NewMockKubernetesClient() KubernetesClient {
	return KubernetesClient{
		SecretsGetter:     &MockSecretGetter{},
		ConfigMapsGetter:  &MockConfigMapsGetter{},
		DeploymentsGetter: &MockDeploymentGetter{},
		ServicesGetter:    &MockServiceGetter{},

		CustomResourceDefinitionsGetter: &MockCustomResourceDefinitionsGetter{},
	}
}

func ClientMissingObjects() KubernetesClient {
	return KubernetesClient{
		DeploymentsGetter: &MockDeploymentNotExistGetter{},
		ServicesGetter:    &MockServiceNotExistGetter{},
	}
}

// NodeHasAnyTaint checks if the node carries a taint with one of the given keys,
// regardless of the taint's value or effect.
func NodeHasAnyTaint(node *v1.Node, taintKeys []string) bool {
	for _, taint := range node.Spec.Taints {
		for _, key := range taintKeys {
			if taint.Key == key {
				return true
			}
		}
	}
	return false
}
