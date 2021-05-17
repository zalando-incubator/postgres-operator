package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/apiserver"
	"github.com/zalando/postgres-operator/pkg/cluster"
	acidv1informer "github.com/zalando/postgres-operator/pkg/generated/informers/externalversions/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/teams"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/pkg/util/ringlog"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
)

// Controller represents operator controller
type Controller struct {
	config    spec.ControllerConfig
	opConfig  *config.Config
	pgTeamMap teams.PostgresTeamMap

	logger     *logrus.Entry
	KubeClient k8sutil.KubernetesClient
	apiserver  *apiserver.Server

	eventRecorder    record.EventRecorder
	eventBroadcaster record.EventBroadcaster

	stopCh chan struct{}

	controllerID     string
	curWorkerID      uint32 //initialized with 0
	curWorkerCluster sync.Map
	clusterWorkers   map[spec.NamespacedName]uint32
	clustersMu       sync.RWMutex
	clusters         map[spec.NamespacedName]*cluster.Cluster
	clusterLogs      map[spec.NamespacedName]ringlog.RingLogger
	clusterHistory   map[spec.NamespacedName]ringlog.RingLogger // history of the cluster changes
	teamClusters     map[string][]spec.NamespacedName

	postgresqlInformer   cache.SharedIndexInformer
	postgresTeamInformer cache.SharedIndexInformer
	podInformer          cache.SharedIndexInformer
	nodesInformer        cache.SharedIndexInformer
	podCh                chan cluster.PodEvent

	clusterEventQueues    []*cache.FIFO // [workerID]Queue
	lastClusterSyncTime   int64
	lastClusterRepairTime int64

	workerLogs map[uint32]ringlog.RingLogger

	PodServiceAccount            *v1.ServiceAccount
	PodServiceAccountRoleBinding *rbacv1.RoleBinding
}

// NewController creates a new controller
func NewController(controllerConfig *spec.ControllerConfig, controllerId string) *Controller {
	logger := logrus.New()
	if controllerConfig.EnableJsonLogging {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		if os.Getenv("LOG_NOQUOTE") != "" {
			logger.SetFormatter(&logrus.TextFormatter{PadLevelText: true, DisableQuote: true})
		}
	}

	var myComponentName = "postgres-operator"
	if controllerId != "" {
		myComponentName += "/" + controllerId
	}

	eventBroadcaster := record.NewBroadcaster()

	// disabling the sending of events also to the logoutput
	// the operator currently duplicates a lot of log entries with this setup
	// eventBroadcaster.StartLogging(logger.Infof)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: myComponentName})

	c := &Controller{
		config:           *controllerConfig,
		opConfig:         &config.Config{},
		logger:           logger.WithField("pkg", "controller"),
		eventRecorder:    recorder,
		eventBroadcaster: eventBroadcaster,
		controllerID:     controllerId,
		curWorkerCluster: sync.Map{},
		clusterWorkers:   make(map[spec.NamespacedName]uint32),
		clusters:         make(map[spec.NamespacedName]*cluster.Cluster),
		clusterLogs:      make(map[spec.NamespacedName]ringlog.RingLogger),
		clusterHistory:   make(map[spec.NamespacedName]ringlog.RingLogger),
		teamClusters:     make(map[string][]spec.NamespacedName),
		stopCh:           make(chan struct{}),
		podCh:            make(chan cluster.PodEvent),
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
	c.eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: c.KubeClient.EventsGetter.Events("")})
	if err != nil {
		c.logger.Fatalf("could not setup kubernetes event sink: %v", err)
	}

}

func (c *Controller) initOperatorConfig() {
	configMapData := make(map[string]string)

	if c.config.ConfigMapName != (spec.NamespacedName{}) {
		configMap, err := c.KubeClient.ConfigMaps(c.config.ConfigMapName.Namespace).
			Get(context.TODO(), c.config.ConfigMapName.Name, metav1.GetOptions{})
		if err != nil {
			panic(err)
		}

		configMapData = configMap.Data
	} else {
		c.logger.Infoln("no ConfigMap specified. Loading default values")
	}

	c.opConfig = config.NewFromMap(configMapData)
	c.warnOnDeprecatedOperatorParameters()

	if c.opConfig.SetMemoryRequestToLimit {

		isSmaller, err := util.IsSmallerQuantity(c.opConfig.DefaultMemoryRequest, c.opConfig.DefaultMemoryLimit)
		if err != nil {
			panic(err)
		}
		if isSmaller {
			c.logger.Warningf("The default memory request of %v for Postgres containers is increased to match the default memory limit of %v.", c.opConfig.DefaultMemoryRequest, c.opConfig.DefaultMemoryLimit)
			c.opConfig.DefaultMemoryRequest = c.opConfig.DefaultMemoryLimit
		}

		isSmaller, err = util.IsSmallerQuantity(c.opConfig.ScalyrMemoryRequest, c.opConfig.ScalyrMemoryLimit)
		if err != nil {
			panic(err)
		}
		if isSmaller {
			c.logger.Warningf("The memory request of %v for the Scalyr sidecar container is increased to match the memory limit of %v.", c.opConfig.ScalyrMemoryRequest, c.opConfig.ScalyrMemoryLimit)
			c.opConfig.ScalyrMemoryRequest = c.opConfig.ScalyrMemoryLimit
		}

		// generateStatefulSet adjusts values for individual Postgres clusters
	}

}

func (c *Controller) modifyConfigFromEnvironment() {
	c.opConfig.WatchedNamespace = c.getEffectiveNamespace(os.Getenv("WATCHED_NAMESPACE"), c.opConfig.WatchedNamespace)

	if c.config.NoDatabaseAccess {
		c.opConfig.EnableDBAccess = false
	}
	if c.config.NoTeamsAPI {
		c.opConfig.EnableTeamsAPI = false
	}

	c.opConfig.IgnoredAnnotations = append(c.opConfig.IgnoredAnnotations, c.config.IgnoredAnnotations...)

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

	if len(c.opConfig.SidecarImages) > 0 {
		c.logger.Warningf("Operator configuration parameter 'sidecar_docker_images' is deprecated. " +
			"Consider using 'sidecars' instead.")
	}
}

func compactValue(v string) string {
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(v)); err != nil {
		panic("Hard coded json strings broken!")
	}
	return compact.String()
}

func (c *Controller) initPodServiceAccount() {

	if c.opConfig.PodServiceAccountDefinition == "" {
		stringValue := `
		{
			"apiVersion": "v1",
			"kind": "ServiceAccount",
			"metadata": {
				"name": "postgres-pod"
			}
		}`

		c.opConfig.PodServiceAccountDefinition = compactValue(stringValue)

	}

	// re-uses k8s internal parsing. See k8s client-go issue #193 for explanation
	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, groupVersionKind, err := decode([]byte(c.opConfig.PodServiceAccountDefinition), nil, nil)

	switch {
	case err != nil:
		panic(fmt.Errorf("Unable to parse pod service account definition from the operator configuration: %v", err))
	case groupVersionKind.Kind != "ServiceAccount":
		panic(fmt.Errorf("pod service account definition in the operator configuration defines another type of resource: %v", groupVersionKind.Kind))
	default:
		c.PodServiceAccount = obj.(*v1.ServiceAccount)
		if c.PodServiceAccount.Name != c.opConfig.PodServiceAccountName {
			c.logger.Warnf("in the operator configuration, the pod service account name %v does not match the name %v given in the account definition; using the former for consistency", c.opConfig.PodServiceAccountName, c.PodServiceAccount.Name)
			c.PodServiceAccount.Name = c.opConfig.PodServiceAccountName
		}
		c.PodServiceAccount.Namespace = ""
	}

	// actual service accounts are deployed at the time of Postgres/Spilo cluster creation
}

func (c *Controller) initRoleBinding() {

	// service account on its own lacks any rights starting with k8s v1.8
	// operator binds it to the cluster role with sufficient privileges
	// we assume the role is created by the k8s administrator
	if c.opConfig.PodServiceAccountRoleBindingDefinition == "" {
		stringValue := fmt.Sprintf(`
		{
			"apiVersion": "rbac.authorization.k8s.io/v1",
			"kind": "RoleBinding",
			"metadata": {
				   "name": "%s"
			},
			"roleRef": {
				"apiGroup": "rbac.authorization.k8s.io",
				"kind": "ClusterRole",
				"name": "%s"
			},
			"subjects": [
				{
					"kind": "ServiceAccount",
					"name": "%s"
				}
			]
		}`, c.PodServiceAccount.Name, c.PodServiceAccount.Name, c.PodServiceAccount.Name)
		c.opConfig.PodServiceAccountRoleBindingDefinition = compactValue(stringValue)
	}
	c.logger.Info("Parse role bindings")
	// re-uses k8s internal parsing. See k8s client-go issue #193 for explanation
	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, groupVersionKind, err := decode([]byte(c.opConfig.PodServiceAccountRoleBindingDefinition), nil, nil)

	switch {
	case err != nil:
		panic(fmt.Errorf("unable to parse the role binding definition from the operator configuration: %v", err))
	case groupVersionKind.Kind != "RoleBinding":
		panic(fmt.Errorf("role binding definition in the operator configuration defines another type of resource: %v", groupVersionKind.Kind))
	default:
		c.PodServiceAccountRoleBinding = obj.(*rbacv1.RoleBinding)
		c.PodServiceAccountRoleBinding.Namespace = ""
		c.logger.Info("successfully parsed")

	}

	// actual roles bindings ar*logrus.Entrye deployed at the time of Postgres/Spilo cluster creation
}

func logMultiLineConfig(log *logrus.Entry, config string) {
	lines := strings.Split(config, "\n")
	for _, l := range lines {
		log.Infof("%s", l)
	}
}

func (c *Controller) initController() {
	c.initClients()
	c.controllerID = os.Getenv("CONTROLLER_ID")

	if configObjectName := os.Getenv("POSTGRES_OPERATOR_CONFIGURATION_OBJECT"); configObjectName != "" {
		if err := c.createConfigurationCRD(c.opConfig.EnableCRDValidation); err != nil {
			c.logger.Fatalf("could not register Operator Configuration CustomResourceDefinition: %v", err)
		}
		if cfg, err := c.readOperatorConfigurationFromCRD(spec.GetOperatorNamespace(), configObjectName); err != nil {
			c.logger.Fatalf("unable to read operator configuration: %v", err)
		} else {
			c.opConfig = c.importConfigurationFromCRD(&cfg.Configuration)
		}
	} else {
		c.initOperatorConfig()
	}
	c.initPodServiceAccount()
	c.initRoleBinding()

	c.modifyConfigFromEnvironment()

	if err := c.createPostgresCRD(c.opConfig.EnableCRDValidation); err != nil {
		c.logger.Fatalf("could not register Postgres CustomResourceDefinition: %v", err)
	}

	c.initSharedInformers()

	c.pgTeamMap = teams.PostgresTeamMap{}
	if c.opConfig.EnablePostgresTeamCRD {
		c.loadPostgresTeams()
	}

	if c.opConfig.DebugLogging {
		c.logger.Logger.Level = logrus.DebugLevel
	}

	logMultiLineConfig(c.logger, c.opConfig.MustMarshal())

	roleDefs := c.getInfrastructureRoleDefinitions()
	if infraRoles, err := c.getInfrastructureRoles(roleDefs); err != nil {
		c.logger.Warningf("could not get infrastructure roles: %v", err)
	} else {
		c.config.InfrastructureRoles = infraRoles
	}

	c.clusterEventQueues = make([]*cache.FIFO, c.opConfig.Workers)
	c.workerLogs = make(map[uint32]ringlog.RingLogger, c.opConfig.Workers)
	for i := range c.clusterEventQueues {
		c.clusterEventQueues[i] = cache.NewFIFO(func(obj interface{}) (string, error) {
			e, ok := obj.(ClusterEvent)
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
	c.postgresqlInformer = acidv1informer.NewPostgresqlInformer(
		c.KubeClient.AcidV1ClientSet,
		c.opConfig.WatchedNamespace,
		constants.QueueResyncPeriodTPR,
		cache.Indexers{})

	c.postgresqlInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.postgresqlAdd,
		UpdateFunc: c.postgresqlUpdate,
		DeleteFunc: c.postgresqlDelete,
	})

	// PostgresTeams
	if c.opConfig.EnablePostgresTeamCRD {
		c.postgresTeamInformer = acidv1informer.NewPostgresTeamInformer(
			c.KubeClient.AcidV1ClientSet,
			c.opConfig.WatchedNamespace,
			constants.QueueResyncPeriodTPR*6, // 30 min
			cache.Indexers{})

		c.postgresTeamInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    c.postgresTeamAdd,
			UpdateFunc: c.postgresTeamUpdate,
		})
	}

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

	// start workers reading from the events queue to prevent the initial sync from blocking on it.
	for i := range c.clusterEventQueues {
		wg.Add(1)
		c.workerLogs[uint32(i)] = ringlog.New(c.opConfig.RingLogLines)
		go c.processClusterEventsQueue(i, stopCh, wg)
	}

	// populate clusters before starting nodeInformer that relies on it and run the initial sync
	if err := c.acquireInitialListOfClusters(); err != nil {
		panic("could not acquire initial list of clusters")
	}

	wg.Add(5)
	go c.runPodInformer(stopCh, wg)
	go c.runPostgresqlInformer(stopCh, wg)
	go c.clusterResync(stopCh, wg)
	go c.apiserver.Run(stopCh, wg)
	go c.kubeNodesInformer(stopCh, wg)

	if c.opConfig.EnablePostgresTeamCRD {
		go c.runPostgresTeamInformer(stopCh, wg)
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

func (c *Controller) runPostgresTeamInformer(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	c.postgresTeamInformer.Run(stopCh)
}

func queueClusterKey(eventType EventType, uid types.UID) string {
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

		if _, err := c.KubeClient.Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{}); err != nil {
			c.logger.Fatalf("Could not find the watched namespace %q", namespace)
		} else {
			c.logger.Infof("Listenting to the specific namespace %q", namespace)
		}

	}

	return namespace
}

// GetReference of Postgres CR object
// i.e. required to emit events to this resource
func (c *Controller) GetReference(postgresql *acidv1.Postgresql) *v1.ObjectReference {
	ref, err := reference.GetReference(scheme.Scheme, postgresql)
	if err != nil {
		c.logger.Errorf("could not get reference for Postgresql CR %v/%v: %v", postgresql.Namespace, postgresql.Name, err)
	}
	return ref
}

func (c *Controller) meetsClusterDeleteAnnotations(postgresql *acidv1.Postgresql) error {

	deleteAnnotationDateKey := c.opConfig.DeleteAnnotationDateKey
	currentTime := time.Now()
	currentDate := currentTime.Format("2006-01-02") // go's reference date

	if deleteAnnotationDateKey != "" {
		if deleteDate, ok := postgresql.Annotations[deleteAnnotationDateKey]; ok {
			if deleteDate != currentDate {
				return fmt.Errorf("annotation %s not matching the current date: got %s, expected %s", deleteAnnotationDateKey, deleteDate, currentDate)
			}
		} else {
			return fmt.Errorf("annotation %s not set in manifest to allow cluster deletion", deleteAnnotationDateKey)
		}
	}

	deleteAnnotationNameKey := c.opConfig.DeleteAnnotationNameKey

	if deleteAnnotationNameKey != "" {
		if clusterName, ok := postgresql.Annotations[deleteAnnotationNameKey]; ok {
			if clusterName != postgresql.Name {
				return fmt.Errorf("annotation %s not matching the cluster name: got %s, expected %s", deleteAnnotationNameKey, clusterName, postgresql.Name)
			}
		} else {
			return fmt.Errorf("annotation %s not set in manifest to allow cluster deletion", deleteAnnotationNameKey)
		}
	}

	return nil
}

// hasOwnership returns true if the controller is the "owner" of the postgresql.
// Whether it's owner is determined by the value of 'acid.zalan.do/controller'
// annotation. If the value matches the controllerID then it owns it, or if the
// controllerID is "" and there's no annotation set.
func (c *Controller) hasOwnership(postgresql *acidv1.Postgresql) bool {
	if postgresql.Annotations != nil {
		if owner, ok := postgresql.Annotations[constants.PostgresqlControllerAnnotationKey]; ok {
			return owner == c.controllerID
		}
	}
	return c.controllerID == ""
}
