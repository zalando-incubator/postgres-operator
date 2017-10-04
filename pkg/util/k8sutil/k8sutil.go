package k8sutil

import (
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
	v1beta1 "k8s.io/client-go/kubernetes/typed/apps/v1beta1"
	policyv1beta1 "k8s.io/client-go/kubernetes/typed/policy/v1beta1"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	extensions "k8s.io/client-go/kubernetes/typed/extensions/v1beta1"

	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/zalando-incubator/postgres-operator/pkg/util/constants"
	"github.com/zalando-incubator/postgres-operator/pkg/util/retryutil"
)

// KubernetesClient describes getters for Kubernetes objects
type KubernetesClient struct {
	v1core.SecretsGetter
	v1core.ServicesGetter
	v1core.EndpointsGetter
	v1core.PodsGetter
	v1core.PersistentVolumesGetter
	v1core.PersistentVolumeClaimsGetter
	v1core.ConfigMapsGetter
	v1core.NodesGetter
	v1beta1.StatefulSetsGetter
	policyv1beta1.PodDisruptionBudgetsGetter
	extensions.ThirdPartyResourcesGetter
	RESTClient rest.Interface
}

// NewFromKubernetesInterface creates KubernetesClient from kubernetes Interface
func NewFromKubernetesInterface(src kubernetes.Interface) (c KubernetesClient) {
	c = KubernetesClient{}
	c.PodsGetter = src.CoreV1()
	c.ServicesGetter = src.CoreV1()
	c.EndpointsGetter = src.CoreV1()
	c.SecretsGetter = src.CoreV1()
	c.ConfigMapsGetter = src.CoreV1()
	c.PersistentVolumeClaimsGetter = src.CoreV1()
	c.PersistentVolumesGetter = src.CoreV1()
	c.NodesGetter = src.CoreV1()
	c.StatefulSetsGetter = src.AppsV1beta1()
	c.ThirdPartyResourcesGetter = src.ExtensionsV1beta1()
	c.PodDisruptionBudgetsGetter = src.PolicyV1beta1()
	c.RESTClient = src.CoreV1().RESTClient()
	return
}

// RestConfig creates REST config
func RestConfig(kubeConfig string, outOfCluster bool) (*rest.Config, error) {
	if outOfCluster {
		return clientcmd.BuildConfigFromFlags("", kubeConfig)
	}

	return rest.InClusterConfig()
}

// ClientSet creates clientset using REST config
func ClientSet(config *rest.Config) (client *kubernetes.Clientset, err error) {
	return kubernetes.NewForConfig(config)
}

// ResourceAlreadyExists checks if error corresponds to Already exists error
func ResourceAlreadyExists(err error) bool {
	return apierrors.IsAlreadyExists(err)
}

// ResourceNotFound checks if error corresponds to Not found error
func ResourceNotFound(err error) bool {
	return apierrors.IsNotFound(err)
}

// KubernetesRestClient create kubernets Interface using REST config
func KubernetesRestClient(cfg rest.Config) (rest.Interface, error) {
	cfg.GroupVersion = &schema.GroupVersion{
		Group:   constants.TPRGroup,
		Version: constants.TPRApiVersion,
	}
	cfg.APIPath = constants.K8sAPIPath
	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: api.Codecs}

	return rest.RESTClientFor(&cfg)
}

// WaitTPRReady waits until ThirdPartyResource is ready
func WaitTPRReady(restclient rest.Interface, interval, timeout time.Duration, ns string) error {
	return retryutil.Retry(interval, timeout, func() (bool, error) {
		_, err := restclient.
			Get().
			Namespace(ns).
			Resource(constants.ResourceName).
			DoRaw()
		if err != nil {
			if ResourceNotFound(err) { // not set up yet. wait more.
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}
