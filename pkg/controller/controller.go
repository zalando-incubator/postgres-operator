package controller

import (
	"fmt"
	"os"
	"sync"

	"github.com/Sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/pkg/api/v1"
	rbac "k8s.io/client-go/pkg/apis/rbac/v1beta1"
	"k8s.io/client-go/tools/cache"

	"github.com/zalando-incubator/postgres-operator/pkg/apiserver"
	"github.com/zalando-incubator/postgres-operator/pkg/cluster"
	"github.com/zalando-incubator/postgres-operator/pkg/spec"
	"github.com/zalando-incubator/postgres-operator/pkg/util"
	"github.com/zalando-incubator/postgres-operator/pkg/util/config"
	"github.com/zalando-incubator/postgres-operator/pkg/util/constants"
	"github.com/zalando-incubator/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando-incubator/postgres-operator/pkg/util/ringlog"
)

// Controller represents operator controller
type Controller struct {
	config   spec.ControllerConfig
	opConfig *config.Config

	logger     *logrus.Entry
	KubeClient k8sutil.KubernetesClient
	apiserver  *apiserver.Server

	stopCh chan struct{}

	curWorkerID      uint32 //initialized with 0
	curWorkerCluster sync.Map
	clusterWorkers   map[spec.NamespacedName]uint32
	clustersMu       sync.RWMutex
	clusters         map[spec.NamespacedName]*cluster.Cluster
	clusterLogs      map[spec.NamespacedName]ringlog.RingLogger
	clusterHistory   map[spec.NamespacedName]ringlog.RingLogger // history of the cluster changes
	teamClusters     map[string][]spec.NamespacedName

	postgresqlInformer cache.SharedIndexInformer
	podInformer        cache.SharedIndexInformer
	nodesInformer      cache.SharedIndexInformer
	podCh              chan spec.PodEvent

	clusterEventQueues  []*cache.FIFO // [workerID]Queue
	lastClusterSyncTime int64

	workerLogs map[uint32]ringlog.RingLogger

	PodServiceAccount            *v1.ServiceAccount
	PodServiceAccountRoleBinding *rbac.RoleBinding
}

// NewController creates a new controller
func NewController(controllerConfig *spec.ControllerConfig) *Controller {
	logger := logrus.New()

	c := &Controller{
		config:           *controllerConfig,
		opConfig:         &config.Config{},
		logger:           logger.WithField("pkg", "controller"),
		curWorkerCluster: sync.Map{},
		clusterWorkers:   make(map[spec.NamespacedName]uint32),
		clusters:         make(map[spec.NamespacedName]*cluster.Cluster),
		clusterLogs:      make(map[spec.NamespacedName]ringlog.RingLogger),
		clusterHistory:   make(map[spec.NamespacedName]ringlog.RingLogger),
		teamClusters:     make(map[string][]spec.NamespacedName),
		stopCh:           make(chan struct{}),
		podCh:            make(chan spec.PodEvent),
	}
	logger.Hooks.Add(c)

	return c
}

func (c *Controller) initClients() {
	var err error

	c.KubeClient, err = k8sutil.NewFromConfig(c.config.RestConfig)
	if err != nil {
		c.logger.Fatalf("could not create kubernetes clients: %v", err)
	}
}

func (c *Controller) initOperatorConfig() {
	configMapData := make(map[string]string)

	if c.config.ConfigMapName != (spec.NamespacedName{}) {
		configMap, err := c.KubeClient.ConfigMaps(c.config.ConfigMapName.Namespace).
			Get(c.config.ConfigMapName.Name, metav1.GetOptions{})
		if err != nil {
			panic(err)
		}

		configMapData = configMap.Data
	} else {
		c.logger.Infoln("no ConfigMap specified. Loading default values")
	}

	configMapData["watched_namespace"] = c.getEffectiveNamespace(os.Getenv("WATCHED_NAMESPACE"), configMapData["watched_namespace"])

	if c.config.NoDatabaseAccess {
		configMapData["enable_database_access"] = "false"
	}
	if c.config.NoTeamsAPI {
		configMapData["enable_teams_api"] = "false"
	}

	c.opConfig = config.NewFromMap(configMapData)
	c.warnOnDeprecatedOperatorParameters()

	scalyrAPIKey := os.Getenv("SCALYR_API_KEY")
	if scalyrAPIKey != "" {
		c.opConfig.ScalyrAPIKey = scalyrAPIKey
	}

}

// warningOnDeprecatedParameters emits warnings upon finding deprecated parmaters
func (c *Controller) warnOnDeprecatedOperatorParameters() {
	if c.opConfig.EnableLoadBalancer != nil {
		c.logger.Warningf("Operator configuration parameter 'enable_load_balancer' is deprecated and takes no effect. " +
			"Consider using the 'enable_master_load_balancer' or 'enable_replica_load_balancer' instead.")
	}
}

func (c *Controller) initPodServiceAccount() {

	if c.opConfig.PodServiceAccountDefinition == "" {
		c.opConfig.PodServiceAccountDefinition = `
		{ "apiVersion": "v1", 
		  "kind": "ServiceAccount", 
		  "metadata": { 
				 "name": "operator" 
		   }
		}`
	}

	// re-uses k8s internal parsing. See k8s client-go issue #193 for explanation
	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, groupVersionKind, err := decode([]byte(c.opConfig.PodServiceAccountDefinition), nil, nil)

	switch {
	case err != nil:
		panic(fmt.Errorf("Unable to parse pod service account definiton from the operator config map: %v", err))
	case groupVersionKind.Kind != "ServiceAccount":
		panic(fmt.Errorf("pod service account definiton in the operator config map defines another type of resource: %v", groupVersionKind.Kind))
	default:
		c.PodServiceAccount = obj.(*v1.ServiceAccount)
		if c.PodServiceAccount.Name != c.opConfig.PodServiceAccountName {
			c.logger.Warnf("in the operator config map, the pod service account name %v does not match the name %v given in the account definition; using the former for consistency", c.opConfig.PodServiceAccountName, c.PodServiceAccount.Name)
			c.PodServiceAccount.Name = c.opConfig.PodServiceAccountName
		}
		c.PodServiceAccount.Namespace = ""
	}

	// service account on its own lacks any rights starting with k8s v1.8
	// operator binds it to the cluster role with sufficient priviliges
	// we assume the role is created by the k8s administrator
	if c.opConfig.PodServiceAccountRoleBindingDefinition == "" {
		c.opConfig.PodServiceAccountRoleBindingDefinition = `
		{ 
			"apiVersion": "rbac.authorization.k8s.io/v1beta1",
			"kind": "RoleBinding", 
			"metadata": { 
				   "name": "zalando-postgres-operator"
			},
			"roleRef": {
				"apiGroup": "rbac.authorization.k8s.io",
				"kind": "ClusterRole",
				"name": "zalando-postgres-operator"
			},
			"subjects": [
				{
					"kind": "ServiceAccount",
					"name": "operator"
				}
			]	
		}`
	}
	c.logger.Info("Parse role bindings")
	// re-uses k8s internal parsing. See k8s client-go issue #193 for explanation
	obj, groupVersionKind, err = decode([]byte(c.opConfig.PodServiceAccountRoleBindingDefinition), nil, nil)

	switch {
	case err != nil:
		panic(fmt.Errorf("Unable to parse the definiton of the role binding  for the pod service account definiton from the operator config map: %v", err))
	case groupVersionKind.Kind != "RoleBinding":
		panic(fmt.Errorf("role binding definiton in the operator config map defines another type of resource: %v", groupVersionKind.Kind))
	default:
		c.PodServiceAccountRoleBinding = obj.(*rbac.RoleBinding)
		c.PodServiceAccountRoleBinding.Namespace = ""
		c.PodServiceAccountRoleBinding.Subjects[0].Name = c.PodServiceAccount.Name
		c.logger.Info("successfully parsed")

	}

	// actual service accounts and roles bindings are deployed at the time of Postgres/Spilo cluster creation

}

func (c *Controller) initController() {
	c.initClients()
	c.initOperatorConfig()
	c.initPodServiceAccount()

	c.initSharedInformers()

	c.logger.Infof("config: %s", c.opConfig.MustMarshal())

	if c.opConfig.DebugLogging {
		c.logger.Logger.Level = logrus.DebugLevel
	}

	if err := c.createCRD(); err != nil {
		c.logger.Fatalf("could not register CustomResourceDefinition: %v", err)
	}

	if infraRoles, err := c.getInfrastructureRoles(&c.opConfig.InfrastructureRolesSecretName); err != nil {
		c.logger.Warningf("could not get infrastructure roles: %v", err)
	} else {
		c.config.InfrastructureRoles = infraRoles
	}

	c.clusterEventQueues = make([]*cache.FIFO, c.opConfig.Workers)
	c.workerLogs = make(map[uint32]ringlog.RingLogger, c.opConfig.Workers)
	for i := range c.clusterEventQueues {
		c.clusterEventQueues[i] = cache.NewFIFO(func(obj interface{}) (string, error) {
			e, ok := obj.(spec.ClusterEvent)
			if !ok {
				return "", fmt.Errorf("could not cast to ClusterEvent")
			}

			return queueClusterKey(e.EventType, e.UID), nil
		})
	}

	c.apiserver = apiserver.New(c, c.opConfig.APIPort, c.logger.Logger)
}

func (c *Controller) initSharedInformers() {
	// Postgresqls
	c.postgresqlInformer = cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc:  c.clusterListFunc,
			WatchFunc: c.clusterWatchFunc,
		},
		&spec.Postgresql{},
		constants.QueueResyncPeriodTPR,
		cache.Indexers{})

	c.postgresqlInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.postgresqlAdd,
		UpdateFunc: c.postgresqlUpdate,
		DeleteFunc: c.postgresqlDelete,
	})

	// Pods
	podLw := &cache.ListWatch{
		ListFunc:  c.podListFunc,
		WatchFunc: c.podWatchFunc,
	}

	c.podInformer = cache.NewSharedIndexInformer(
		podLw,
		&v1.Pod{},
		constants.QueueResyncPeriodPod,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.podAdd,
		UpdateFunc: c.podUpdate,
		DeleteFunc: c.podDelete,
	})

	// Kubernetes Nodes
	nodeLw := &cache.ListWatch{
		ListFunc:  c.nodeListFunc,
		WatchFunc: c.nodeWatchFunc,
	}

	c.nodesInformer = cache.NewSharedIndexInformer(
		nodeLw,
		&v1.Node{},
		constants.QueueResyncPeriodNode,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	c.nodesInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.nodeAdd,
		UpdateFunc: c.nodeUpdate,
		DeleteFunc: c.nodeDelete,
	})
}

// Run starts background controller processes
func (c *Controller) Run(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	c.initController()

	wg.Add(5)
	go c.runPodInformer(stopCh, wg)
	go c.runPostgresqlInformer(stopCh, wg)
	go c.clusterResync(stopCh, wg)
	go c.apiserver.Run(stopCh, wg)
	go c.kubeNodesInformer(stopCh, wg)

	for i := range c.clusterEventQueues {
		wg.Add(1)
		c.workerLogs[uint32(i)] = ringlog.New(c.opConfig.RingLogLines)
		go c.processClusterEventsQueue(i, stopCh, wg)
	}

	c.logger.Info("started working in background")
}

func (c *Controller) runPodInformer(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	c.podInformer.Run(stopCh)
}

func (c *Controller) runPostgresqlInformer(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	c.postgresqlInformer.Run(stopCh)
}

func queueClusterKey(eventType spec.EventType, uid types.UID) string {
	return fmt.Sprintf("%s-%s", eventType, uid)
}

func (c *Controller) kubeNodesInformer(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	c.nodesInformer.Run(stopCh)
}

func (c *Controller) getEffectiveNamespace(namespaceFromEnvironment, namespaceFromConfigMap string) string {

	namespace := util.Coalesce(namespaceFromEnvironment, util.Coalesce(namespaceFromConfigMap, spec.GetOperatorNamespace()))

	if namespace == "*" {

		namespace = v1.NamespaceAll
		c.logger.Infof("Listening to all namespaces")

	} else {

		if _, err := c.KubeClient.Namespaces().Get(namespace, metav1.GetOptions{}); err != nil {
			c.logger.Fatalf("Could not find the watched namespace %q", namespace)
		} else {
			c.logger.Infof("Listenting to the specific namespace %q", namespace)
		}

	}

	return namespace
}
