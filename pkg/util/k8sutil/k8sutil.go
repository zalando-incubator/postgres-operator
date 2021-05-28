package k8sutil

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	b64 "encoding/base64"
	"encoding/json"

	batchv1beta1 "k8s.io/api/batch/v1beta1"
	clientbatchv1beta1 "k8s.io/client-go/kubernetes/typed/batch/v1beta1"

	apiacidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	acidv1client "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned"
	acidv1 "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/typed/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/spec"
	apiappsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policybeta1 "k8s.io/api/policy/v1beta1"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	appsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	policyv1beta1 "k8s.io/client-go/kubernetes/typed/policy/v1beta1"
	rbacv1 "k8s.io/client-go/kubernetes/typed/rbac/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func Int32ToPointer(value int32) *int32 {
	return &value
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
	policyv1beta1.PodDisruptionBudgetsGetter
	apiextv1.CustomResourceDefinitionsGetter
	clientbatchv1beta1.CronJobsGetter
	acidv1.OperatorConfigurationsGetter
	acidv1.PostgresTeamsGetter
	acidv1.PostgresqlsGetter

	RESTClient      rest.Interface
	AcidV1ClientSet *acidv1client.Clientset
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
	kubeClient.PodDisruptionBudgetsGetter = client.PolicyV1beta1()
	kubeClient.RESTClient = client.CoreV1().RESTClient()
	kubeClient.RoleBindingsGetter = client.RbacV1()
	kubeClient.CronJobsGetter = client.BatchV1beta1()
	kubeClient.EventsGetter = client.CoreV1()

	apiextClient, err := apiextclient.NewForConfig(cfg)
	if err != nil {
		return kubeClient, fmt.Errorf("could not create api client:%v", err)
	}

	kubeClient.CustomResourceDefinitionsGetter = apiextClient.ApiextensionsV1()

	kubeClient.AcidV1ClientSet = acidv1client.NewForConfigOrDie(cfg)
	if err != nil {
		return kubeClient, fmt.Errorf("could not create acid.zalan.do clientset: %v", err)
	}

	kubeClient.OperatorConfigurationsGetter = kubeClient.AcidV1ClientSet.AcidV1()
	kubeClient.PostgresTeamsGetter = kubeClient.AcidV1ClientSet.AcidV1()
	kubeClient.PostgresqlsGetter = kubeClient.AcidV1ClientSet.AcidV1()

	return kubeClient, nil
}

// SetPostgresCRDStatus of Postgres cluster
func (client *KubernetesClient) SetPostgresCRDStatus(clusterName spec.NamespacedName, status string) (*apiacidv1.Postgresql, error) {
	var pg *apiacidv1.Postgresql
	var pgStatus apiacidv1.PostgresStatus
	pgStatus.PostgresClusterStatus = status

	patch, err := json.Marshal(struct {
		PgStatus interface{} `json:"status"`
	}{&pgStatus})

	if err != nil {
		return pg, fmt.Errorf("could not marshal status: %v", err)
	}

	// we cannot do a full scale update here without fetching the previous manifest (as the resourceVersion may differ),
	// however, we could do patch without it. In the future, once /status subresource is there (starting Kubernetes 1.11)
	// we should take advantage of it.
	pg, err = client.PostgresqlsGetter.Postgresqls(clusterName.Namespace).Patch(
		context.TODO(), clusterName.Name, types.MergePatchType, patch, metav1.PatchOptions{}, "status")
	if err != nil {
		return pg, fmt.Errorf("could not update status: %v", err)
	}

	// update the spec, maintaining the new resourceVersion.
	return pg, nil
}

// SameService compares the Services
func SameService(cur, new *v1.Service) (match bool, reason string) {
	//TODO: improve comparison
	if cur.Spec.Type != new.Spec.Type {
		return false, fmt.Sprintf("new service's type %q does not match the current one %q",
			new.Spec.Type, cur.Spec.Type)
	}

	oldSourceRanges := cur.Spec.LoadBalancerSourceRanges
	newSourceRanges := new.Spec.LoadBalancerSourceRanges

	/* work around Kubernetes 1.6 serializing [] as nil. See https://github.com/kubernetes/kubernetes/issues/43203 */
	if (len(oldSourceRanges) != 0) || (len(newSourceRanges) != 0) {
		if !reflect.DeepEqual(oldSourceRanges, newSourceRanges) {
			return false, "new service's LoadBalancerSourceRange does not match the current one"
		}
	}

	match = true

	ignoredServiceAnnotations := new.Annotations["ignore_service_annotations"]
	reasonPrefix := "new service's annotations does not match the current one:"
	for ann := range cur.Annotations {
		if strings.Contains(ignoredServiceAnnotations, ann) {
			continue
		}

		if _, ok := new.Annotations[ann]; !ok {
			match = false
			if len(reason) == 0 {
				reason = reasonPrefix
			}
			reason += fmt.Sprintf(" Removed '%s'.", ann)
		}
	}

	for ann := range new.Annotations {
		v, ok := cur.Annotations[ann]
		if !ok {
			if len(reason) == 0 {
				reason = reasonPrefix
			}
			reason += fmt.Sprintf(" Added '%s' with value '%s'.", ann, new.Annotations[ann])
			match = false
		} else if v != new.Annotations[ann] {
			if len(reason) == 0 {
				reason = reasonPrefix
			}
			reason += fmt.Sprintf(" '%s' changed from '%s' to '%s'.", ann, v, new.Annotations[ann])
			match = false
		}
	}

	return match, reason
}

// SamePDB compares the PodDisruptionBudgets
func SamePDB(cur, new *policybeta1.PodDisruptionBudget) (match bool, reason string) {
	//TODO: improve comparison
	match = reflect.DeepEqual(new.Spec, cur.Spec)
	if !match {
		reason = "new PDB spec does not match the current one"
	}

	return
}

func getJobImage(cronJob *batchv1beta1.CronJob) string {
	return cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image
}

// SameLogicalBackupJob compares Specs of logical backup cron jobs
func SameLogicalBackupJob(cur, new *batchv1beta1.CronJob) (match bool, reason string) {

	if cur.Spec.Schedule != new.Spec.Schedule {
		return false, fmt.Sprintf("new job's schedule %q does not match the current one %q",
			new.Spec.Schedule, cur.Spec.Schedule)
	}

	newImage := getJobImage(new)
	curImage := getJobImage(cur)
	if newImage != curImage {
		return false, fmt.Sprintf("new job's image %q does not match the current one %q",
			newImage, curImage)
	}

	return true, ""
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
						v1.Container{
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
	}
}

func ClientMissingObjects() KubernetesClient {
	return KubernetesClient{
		DeploymentsGetter: &MockDeploymentNotExistGetter{},
		ServicesGetter:    &MockServiceNotExistGetter{},
	}
}
