package cluster

// Postgres CustomResourceDefinition object i.e. Spilo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	zalandov1 "github.com/zalando/postgres-operator/v2/pkg/apis/zalando.org/v1"

	"github.com/zalando/postgres-operator/v2/pkg/generated/clientset/versioned/scheme"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	pgteams "github.com/zalando/postgres-operator/v2/pkg/teams"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/patroni"
	"github.com/zalando/postgres-operator/v2/pkg/util/teams"
	"github.com/zalando/postgres-operator/v2/pkg/util/users"
	"github.com/zalando/postgres-operator/v2/pkg/util/volumes"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
)

var (
	alphaNumericRegexp    = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9]*$")
	databaseNameRegexp    = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]*$")
	userRegexp            = regexp.MustCompile(`^[a-z0-9]([-_a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-_a-z0-9]*[a-z0-9])?)*$`)
	patroniObjectSuffixes = []string{"leader", "config", "sync", "failover"}
	finalizerName         = "postgres-operator.acid.zalan.do"
)

// Config contains operator-wide clients and configuration used from a cluster. TODO: remove struct duplication.
type Config struct {
	OpConfig                     config.Config
	RestConfig                   *rest.Config
	PgTeamMap                    *pgteams.PostgresTeamMap
	InfrastructureRoles          map[string]spec.PgUser // inherited from the controller
	PodServiceAccount            *v1.ServiceAccount
	PodServiceAccountRoleBinding *rbacv1.RoleBinding
}

type kubeResources struct {
	Services                      map[PostgresRole]*v1.Service
	Endpoints                     map[PostgresRole]*v1.Endpoints
	PatroniEndpoints              map[string]*v1.Endpoints
	PatroniConfigMaps             map[string]*v1.ConfigMap
	Secrets                       map[types.UID]*v1.Secret
	Statefulset                   *appsv1.StatefulSet
	VolumeClaims                  map[types.UID]*v1.PersistentVolumeClaim
	PrimaryPodDisruptionBudget    *policyv1.PodDisruptionBudget
	CriticalOpPodDisruptionBudget *policyv1.PodDisruptionBudget
	LogicalBackupJob              *batchv1.CronJob
	Streams                       map[string]*zalandov1.FabricEventStream
	// Pods are treated separately
}

// Cluster describes postgresql cluster
type Cluster struct {
	kubeResources
	acidv1.Postgresql
	Config
	logger           *logrus.Entry
	eventRecorder    record.EventRecorder
	patroni          patroni.Interface
	pgUsers          map[string]spec.PgUser
	pgUsersCache     map[string]spec.PgUser
	systemUsers      map[string]spec.PgUser
	podSubscribers   map[spec.NamespacedName]chan PodEvent
	podSubscribersMu sync.RWMutex
	pgDb             *sql.DB
	mu               sync.Mutex
	userSyncStrategy spec.UserSyncer
	deleteOptions    metav1.DeleteOptions
	podEventsStore   cache.Store
	podEventsQueue   *cache.FIFO
	replicationSlots map[string]interface{}

	teamsAPIClient      teams.Interface
	oauthTokenGetter    OAuthTokenGetter
	KubeClient          k8sutil.KubernetesClient // TODO: move clients to the better place?
	currentProcess      Process
	processMu           sync.RWMutex // protects the current operation for reporting, no need to hold the master mutex
	specMu              sync.RWMutex // protects the spec for reporting, no need to hold the master mutex
	ConnectionPooler    map[PostgresRole]*ConnectionPoolerObjects
	EBSVolumes          map[string]volumes.VolumeProperties
	VolumeResizer       volumes.VolumeResizer
	currentMajorVersion int
}

type compareStatefulsetResult struct {
	match                 bool
	replace               bool
	rollingUpdate         bool
	reasons               []string
	deletedPodAnnotations []string
}

type compareLogicalBackupJobResult struct {
	match                 bool
	reasons               []string
	deletedPodAnnotations []string
}

// New creates a new cluster. This function should be called from a controller.
func New(cfg Config, kubeClient k8sutil.KubernetesClient, pgSpec acidv1.Postgresql, logger *logrus.Entry, eventRecorder record.EventRecorder) *Cluster {
	deletePropagationPolicy := metav1.DeletePropagationOrphan

	keyFn := func(obj interface{}) (string, error) {
		e, ok := obj.(PodEvent)
		if !ok {
			return "", fmt.Errorf("could not cast to pod event")
		}

		return fmt.Sprintf("%s-%s", e.PodName, e.ResourceVersion), nil
	}
	podEventsStore := cache.NewStore(keyFn)
	podEventsQueue := cache.NewFIFO(keyFn)

	passwordEncryption := passwordEncryptionFromSpec(&pgSpec.Spec)

	cluster := &Cluster{
		Config:         cfg,
		Postgresql:     pgSpec,
		pgUsers:        make(map[string]spec.PgUser),
		systemUsers:    make(map[string]spec.PgUser),
		podSubscribers: make(map[spec.NamespacedName]chan PodEvent),
		kubeResources: kubeResources{
			Secrets:           make(map[types.UID]*v1.Secret),
			Services:          make(map[PostgresRole]*v1.Service),
			Endpoints:         make(map[PostgresRole]*v1.Endpoints),
			PatroniEndpoints:  make(map[string]*v1.Endpoints),
			PatroniConfigMaps: make(map[string]*v1.ConfigMap),
			VolumeClaims:      make(map[types.UID]*v1.PersistentVolumeClaim),
			Streams:           make(map[string]*zalandov1.FabricEventStream),
		},
		userSyncStrategy: users.DefaultUserSyncStrategy{
			PasswordEncryption:   string(passwordEncryption),
			RoleDeletionSuffix:   cfg.OpConfig.RoleDeletionSuffix,
			AdditionalOwnerRoles: cfg.OpConfig.AdditionalOwnerRoles,
		},
		deleteOptions:       metav1.DeleteOptions{PropagationPolicy: &deletePropagationPolicy},
		podEventsStore:      podEventsStore,
		podEventsQueue:      podEventsQueue,
		KubeClient:          kubeClient,
		currentMajorVersion: 0,
		replicationSlots:    make(map[string]interface{}),
	}
	cluster.logger = logger.WithField("pkg", "cluster").WithField("cluster-name", cluster.clusterName())
	cluster.teamsAPIClient = teams.NewTeamsAPI(cfg.OpConfig.TeamsAPIUrl, logger)
	cluster.oauthTokenGetter = newSecretOauthTokenGetter(&kubeClient, cfg.OpConfig.OAuthTokenSecretName)
	cluster.patroni = patroni.New(cluster.logger, nil)
	cluster.eventRecorder = eventRecorder

	cluster.EBSVolumes = make(map[string]volumes.VolumeProperties)
	if cfg.OpConfig.StorageResizeMode != "pvc" {
		cluster.VolumeResizer = &volumes.EBSVolumeResizer{AWSRegion: cfg.OpConfig.AWSRegion}
	}

	return cluster
}

func (c *Cluster) clusterName() spec.NamespacedName {
	return util.NameFromMeta(c.ObjectMeta)
}

func (c *Cluster) clusterNamespace() string {
	return c.ObjectMeta.Namespace
}

func (c *Cluster) teamName() string {
	// TODO: check Teams API for the actual name (in case the user passes an integer Id).
	return c.Spec.TeamID
}

func (c *Cluster) setProcessName(procName string, args ...interface{}) {
	c.processMu.Lock()
	defer c.processMu.Unlock()
	c.currentProcess = Process{
		Name:      fmt.Sprintf(procName, args...),
		StartTime: time.Now(),
	}
}

// GetReference of Postgres CR object
// i.e. required to emit events to this resource
func (c *Cluster) GetReference() *v1.ObjectReference {
	ref, err := reference.GetReference(scheme.Scheme, &c.Postgresql)
	if err != nil {
		c.logger.Errorf("could not get reference for Postgresql CR %v/%v: %v", c.Postgresql.Namespace, c.Postgresql.Name, err)
	}
	return ref
}

func (c *Cluster) isNewCluster() bool {
	return c.Status.Creating()
}

// initUsers populates c.systemUsers and c.pgUsers maps.
func (c *Cluster) initUsers() error {
	c.setProcessName("initializing users")

	// if team member deprecation is enabled save current state of pgUsers
	// to check for deleted roles
	c.pgUsersCache = map[string]spec.PgUser{}
	if c.OpConfig.EnableTeamMemberDeprecation {
		for k, v := range c.pgUsers {
			if v.Origin == spec.RoleOriginTeamsAPI {
				c.pgUsersCache[k] = v
			}
		}
	}

	// clear our the previous state of the cluster users (in case we are
	// running a sync).
	c.systemUsers = map[string]spec.PgUser{}
	c.pgUsers = map[string]spec.PgUser{}

	c.initSystemUsers()

	if err := c.initInfrastructureRoles(); err != nil {
		return fmt.Errorf("could not init infrastructure roles: %v", err)
	}

	if err := c.initPreparedDatabaseRoles(); err != nil {
		return fmt.Errorf("could not init default users: %v", err)
	}

	if err := c.initRobotUsers(); err != nil {
		return fmt.Errorf("could not init robot users: %v", err)
	}

	if err := c.initHumanUsers(); err != nil {
		// remember all cached users in c.pgUsers
		for cachedUserName, cachedUser := range c.pgUsersCache {
			c.pgUsers[cachedUserName] = cachedUser
		}
		return fmt.Errorf("could not init human users: %v", err)
	}

	c.initAdditionalOwnerRoles()

	return nil
}

// Create creates the new kubernetes objects associated with the cluster.
func (c *Cluster) Create() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		pgCreateStatus *acidv1.Postgresql
		service        *v1.Service
		ep             *v1.Endpoints
		ss             *appsv1.StatefulSet
	)

	defer func() {
		currentStatus := c.Status.DeepCopy()
		pg := c.Postgresql.DeepCopy()
		pg.Status.PostgresClusterStatus = acidv1.ClusterStatusRunning

		if err != nil {
			c.logger.Warningf("cluster created failed: %v", err)
			pg.Status.PostgresClusterStatus = acidv1.ClusterStatusAddFailed
		}

		if !equality.Semantic.DeepEqual(currentStatus, pg.Status) {
			pgUpdatedStatus, err := c.KubeClient.SetPostgresCRDStatus(c.clusterName(), pg)
			if err != nil {
				c.logger.Warningf("could not set cluster status: %v", err)
				return
			}
			c.setSpec(pgUpdatedStatus)
		}
	}()

	pg := c.Postgresql.DeepCopy()
	pg.Status.PostgresClusterStatus = acidv1.ClusterStatusCreating

	pgCreateStatus, err = c.KubeClient.SetPostgresCRDStatus(c.clusterName(), pg)
	if err != nil {
		return fmt.Errorf("could not set cluster status: %v", err)
	}
	c.setSpec(pgCreateStatus)

	if c.OpConfig.EnableFinalizers != nil && *c.OpConfig.EnableFinalizers {
		if err = c.addFinalizer(); err != nil {
			return fmt.Errorf("could not add finalizer: %v", err)
		}
	}
	c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Create", "Started creation of new cluster resources")

	for _, role := range []PostgresRole{Master, Replica} {

		// if kubernetes_use_configmaps is set Patroni will create configmaps
		// otherwise it will use endpoints
		if !c.patroniKubernetesUseConfigMaps() {
			if c.Endpoints[role] != nil {
				return fmt.Errorf("%s endpoint already exists in the cluster", role)
			}
			if role == Master {
				// replica endpoint will be created by the replica service. Master endpoint needs to be created by us,
				// since the corresponding master service does not define any selectors.
				ep, err = c.createEndpoint(role)
				if err != nil {
					return fmt.Errorf("could not create %s endpoint: %v", role, err)
				}
				c.logger.Infof("endpoint %q has been successfully created", util.NameFromMeta(ep.ObjectMeta))
				c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Endpoints", "Endpoint %q has been successfully created", util.NameFromMeta(ep.ObjectMeta))
			}
		}

		if c.Services[role] != nil {
			return fmt.Errorf("service already exists in the cluster")
		}
		service, err = c.createService(role)
		if err != nil {
			return fmt.Errorf("could not create %s service: %v", role, err)
		}
		c.logger.Infof("%s service %q has been successfully created", role, util.NameFromMeta(service.ObjectMeta))
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Services", "The service %q for role %s has been successfully created", util.NameFromMeta(service.ObjectMeta), role)
	}

	if err = c.initUsers(); err != nil {
		return err
	}
	c.logger.Infof("users have been initialized")

	if err = c.syncSecrets(); err != nil {
		return fmt.Errorf("could not create secrets: %v", err)
	}
	c.logger.Infof("secrets have been successfully created")
	c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Secrets", "The secrets have been successfully created")

	if err = c.createPodDisruptionBudgets(); err != nil {
		return fmt.Errorf("could not create pod disruption budgets: %v", err)
	}
	c.logger.Info("pod disruption budgets have been successfully created")

	if c.Statefulset != nil {
		return fmt.Errorf("statefulset already exists in the cluster")
	}
	ss, err = c.createStatefulSet()
	if err != nil {
		return fmt.Errorf("could not create statefulset: %v", err)
	}
	c.logger.Infof("statefulset %q has been successfully created", util.NameFromMeta(ss.ObjectMeta))
	c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "StatefulSet", "Statefulset %q has been successfully created", util.NameFromMeta(ss.ObjectMeta))

	c.logger.Info("waiting for the cluster being ready")

	if err = c.waitStatefulsetPodsReady(); err != nil {
		c.logger.Errorf("failed to create cluster: %v", err)
		return err
	}
	c.logger.Infof("pods are ready")
	c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "StatefulSet", "Pods are ready")

	// sync volume may already transition volumes to gp3, if iops/throughput or type is specified
	if err = c.syncVolumes(); err != nil {
		return err
	}

	// sync resources created by Patroni
	if err = c.syncPatroniResources(); err != nil {
		c.logger.Warnf("Patroni resources not yet synced: %v", err)
	}

	// create database objects unless we are running without pods or disabled
	// that feature explicitly
	if !(c.databaseAccessDisabled() || c.getNumberOfInstances(&c.Spec) <= 0 || isStandbyCluster(&c.Spec)) {
		c.logger.Infof("Create roles")
		if err = c.createRoles(); err != nil {
			return fmt.Errorf("could not create users: %v", err)
		}
		c.logger.Infof("users have been successfully created")

		if err = c.syncDatabases(); err != nil {
			return fmt.Errorf("could not sync databases: %v", err)
		}
		if err = c.syncPreparedDatabases(); err != nil {
			return fmt.Errorf("could not sync prepared databases: %v", err)
		}
		c.logger.Infof("databases have been successfully created")
	}

	if c.Postgresql.Spec.EnableLogicalBackup {
		if err := c.createLogicalBackupJob(); err != nil {
			return fmt.Errorf("could not create a k8s cron job for logical backups: %v", err)
		}
		c.logger.Info("a k8s cron job for logical backup has been successfully created")
	}

	// Create connection pooler deployment and services if necessary. Since we
	// need to perform some operations with the database itself (e.g. install
	// lookup function), do it as the last step, when everything is available.
	//
	// Do not consider connection pooler as a strict requirement, and if
	// something fails, report warning
	c.createConnectionPooler(c.installLookupFunction)

	// remember slots to detect deletion from manifest
	for slotName, desiredSlot := range c.Spec.Patroni.Slots {
		c.replicationSlots[slotName] = desiredSlot
	}

	if len(c.Spec.Streams) > 0 {
		// creating streams requires syncing the statefulset first
		err = c.syncStatefulSet()
		if err != nil {
			return fmt.Errorf("could not sync statefulset: %v", err)
		}
		if err = c.syncStreams(); err != nil {
			c.logger.Errorf("could not create streams: %v", err)
		}
	}

	if err := c.listResources(); err != nil {
		c.logger.Errorf("could not list resources: %v", err)
	}

	return nil
}

func (c *Cluster) compareStatefulSetWith(statefulSet *appsv1.StatefulSet) *compareStatefulsetResult {
	deletedPodAnnotations := []string{}
	reasons := make([]string, 0)
	var match, needsRollUpdate, needsReplace bool

	match = true
	// TODO: improve me
	if *c.Statefulset.Spec.Replicas != *statefulSet.Spec.Replicas {
		match = false
		reasons = append(reasons, "new statefulset's number of replicas does not match the current one")
	}
	if !reflect.DeepEqual(c.Statefulset.OwnerReferences, statefulSet.OwnerReferences) {
		match = false
		needsReplace = true
		reasons = append(reasons, "new statefulset's ownerReferences do not match")
	}
	if changed, reason := c.compareAnnotations(c.Statefulset.Annotations, statefulSet.Annotations, nil); changed {
		match = false
		needsReplace = true
		reasons = append(reasons, "new statefulset's annotations do not match: "+reason)
	}
	if c.Statefulset.Spec.PodManagementPolicy != statefulSet.Spec.PodManagementPolicy {
		match = false
		needsReplace = true
		reasons = append(reasons, "new statefulset's pod management policy do not match")
	}

	if c.Statefulset.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		c.Statefulset.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
			WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
		}
	}
	if !reflect.DeepEqual(c.Statefulset.Spec.PersistentVolumeClaimRetentionPolicy, statefulSet.Spec.PersistentVolumeClaimRetentionPolicy) {
		match = false
		needsReplace = true
		reasons = append(reasons, "new statefulset's persistent volume claim retention policy do not match")
	}

	needsRollUpdate, reasons = c.compareContainers("statefulset initContainers", c.Statefulset.Spec.Template.Spec.InitContainers, statefulSet.Spec.Template.Spec.InitContainers, needsRollUpdate, reasons)
	needsRollUpdate, reasons = c.compareContainers("statefulset containers", c.Statefulset.Spec.Template.Spec.Containers, statefulSet.Spec.Template.Spec.Containers, needsRollUpdate, reasons)

	if len(c.Statefulset.Spec.Template.Spec.Containers) == 0 {
		c.logger.Warningf("statefulset %q has no container", util.NameFromMeta(c.Statefulset.ObjectMeta))
		return &compareStatefulsetResult{}
	}
	// In the comparisons below, the needsReplace and needsRollUpdate flags are never reset, since checks fall through
	// and the combined effect of all the changes should be applied.
	// TODO: make sure this is in sync with generatePodTemplate, ideally by using the same list of fields to generate
	// the template and the diff
	if c.Statefulset.Spec.Template.Spec.ServiceAccountName != statefulSet.Spec.Template.Spec.ServiceAccountName {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's serviceAccountName service account name does not match the current one")
	}
	if *c.Statefulset.Spec.Template.Spec.TerminationGracePeriodSeconds != *statefulSet.Spec.Template.Spec.TerminationGracePeriodSeconds {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's terminationGracePeriodSeconds does not match the current one")
	}
	if !reflect.DeepEqual(c.Statefulset.Spec.Template.Spec.Affinity, statefulSet.Spec.Template.Spec.Affinity) {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod affinity does not match the current one")
	}
	if !reflect.DeepEqual(c.Statefulset.Spec.Template.Spec.TopologySpreadConstraints, statefulSet.Spec.Template.Spec.TopologySpreadConstraints) {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod topologySpreadConstraints does not match the current one")
	}
	if len(c.Statefulset.Spec.Template.Spec.Tolerations) != len(statefulSet.Spec.Template.Spec.Tolerations) {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod tolerations does not match the current one")
	}

	// Some generated fields like creationTimestamp make it not possible to use DeepCompare on Spec.Template.ObjectMeta
	if !reflect.DeepEqual(c.Statefulset.Spec.Template.Labels, statefulSet.Spec.Template.Labels) {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's metadata labels does not match the current one")
	}
	if (c.Statefulset.Spec.Selector != nil) && (statefulSet.Spec.Selector != nil) {
		if !reflect.DeepEqual(c.Statefulset.Spec.Selector.MatchLabels, statefulSet.Spec.Selector.MatchLabels) {
			// forbid introducing new labels in the selector on the new statefulset, as it would cripple replacements
			// due to the fact that the new statefulset won't be able to pick up old pods with non-matching labels.
			if !util.MapContains(c.Statefulset.Spec.Selector.MatchLabels, statefulSet.Spec.Selector.MatchLabels) {
				c.logger.Warningf("new statefulset introduces extra labels in the label selector, cannot continue")
				return &compareStatefulsetResult{}
			}
			needsReplace = true
			reasons = append(reasons, "new statefulset's selector does not match the current one")
		}
	}

	if changed, reason := c.compareAnnotations(c.Statefulset.Spec.Template.Annotations, statefulSet.Spec.Template.Annotations, &deletedPodAnnotations); changed {
		match = false
		needsReplace = true
		reasons = append(reasons, "new statefulset's pod template metadata annotations does not match "+reason)
	}
	if !reflect.DeepEqual(c.Statefulset.Spec.Template.Spec.SecurityContext, statefulSet.Spec.Template.Spec.SecurityContext) {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod template security context in spec does not match the current one")
	}
	if len(c.Statefulset.Spec.VolumeClaimTemplates) != len(statefulSet.Spec.VolumeClaimTemplates) {
		needsReplace = true
		reasons = append(reasons, "new statefulset's volumeClaimTemplates contains different number of volumes to the old one")
	} else {
		for i := 0; i < len(c.Statefulset.Spec.VolumeClaimTemplates); i++ {
			name := c.Statefulset.Spec.VolumeClaimTemplates[i].Name
			// Some generated fields like creationTimestamp make it not possible to use DeepCompare on ObjectMeta
			if name != statefulSet.Spec.VolumeClaimTemplates[i].Name {
				needsReplace = true
				reasons = append(reasons, fmt.Sprintf("new statefulset's name for volume %d does not match the current one", i))
				continue
			}
			if changed, reason := c.compareAnnotations(c.Statefulset.Spec.VolumeClaimTemplates[i].Annotations, statefulSet.Spec.VolumeClaimTemplates[i].Annotations, nil); changed {
				needsReplace = true
				reasons = append(reasons, fmt.Sprintf("new statefulset's annotations for volume %q do not match the current ones: %s", name, reason))
			}
			if !reflect.DeepEqual(c.Statefulset.Spec.VolumeClaimTemplates[i].Spec, statefulSet.Spec.VolumeClaimTemplates[i].Spec) {
				name := c.Statefulset.Spec.VolumeClaimTemplates[i].Name
				needsReplace = true
				reasons = append(reasons, fmt.Sprintf("new statefulset's volumeClaimTemplates specification for volume %q does not match the current one", name))
			}
		}
	}

	if len(c.Statefulset.Spec.Template.Spec.Volumes) != len(statefulSet.Spec.Template.Spec.Volumes) {
		needsReplace = true
		reasons = append(reasons, "new statefulset's volumes contains different number of volumes to the old one")
	}

	// we assume any change in priority happens by rolling out a new priority class
	// changing the priority value in an existing class is not supproted
	if c.Statefulset.Spec.Template.Spec.PriorityClassName != statefulSet.Spec.Template.Spec.PriorityClassName {
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod priority class in spec does not match the current one")
	}

	if needsRollUpdate || needsReplace {
		match = false
	}

	return &compareStatefulsetResult{match: match, reasons: reasons, rollingUpdate: needsRollUpdate, replace: needsReplace, deletedPodAnnotations: deletedPodAnnotations}
}

type containerCondition func(a, b v1.Container) bool

type containerCheck struct {
	condition containerCondition
	reason    string
}

func newCheck(msg string, cond containerCondition) containerCheck {
	return containerCheck{reason: msg, condition: cond}
}

// compareContainers: compare two list of Containers
// and return:
// * whether or not a rolling update is needed
// * a list of reasons in a human readable format

func (c *Cluster) compareContainers(description string, setA, setB []v1.Container, needsRollUpdate bool, reasons []string) (bool, []string) {
	if len(setA) != len(setB) {
		return true, append(reasons, fmt.Sprintf("new %s's length does not match the current ones", description))
	}

	checks := []containerCheck{
		newCheck("new %s's %s (index %d) name does not match the current one",
			func(a, b v1.Container) bool { return a.Name != b.Name }),
		newCheck("new %s's %s (index %d) image does not match the current one",
			func(a, b v1.Container) bool { return a.Image != b.Image }),
		newCheck("new %s's %s (index %d) readiness probe does not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.ReadinessProbe, b.ReadinessProbe) }),
		newCheck("new %s's %s (index %d) liveness probe does not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.LivenessProbe, b.LivenessProbe) }),
		newCheck("new %s's %s (index %d) ports do not match the current one",
			func(a, b v1.Container) bool { return !comparePorts(a.Ports, b.Ports) }),
		newCheck("new %s's %s (index %d) resources do not match the current ones",
			func(a, b v1.Container) bool { return !compareResources(&a.Resources, &b.Resources) }),
		newCheck("new %s's %s (index %d) environment does not match the current one",
			func(a, b v1.Container) bool { return !compareEnv(a.Env, b.Env) }),
		newCheck("new %s's %s (index %d) environment sources do not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.EnvFrom, b.EnvFrom) }),
		newCheck("new %s's %s (index %d) security context does not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.SecurityContext, b.SecurityContext) }),
		newCheck("new %s's %s (index %d) volume mounts do not match the current one",
			func(a, b v1.Container) bool { return !compareVolumeMounts(a.VolumeMounts, b.VolumeMounts) }),
	}

	for index, containerA := range setA {
		containerB := setB[index]
		for _, check := range checks {
			if check.condition(containerA, containerB) {
				needsRollUpdate = true
				reasons = append(reasons, fmt.Sprintf(check.reason, description, containerA.Name, index))
			}
		}
	}

	return needsRollUpdate, reasons
}

func compareResources(a *v1.ResourceRequirements, b *v1.ResourceRequirements) bool {
	equal := true
	if a != nil {
		equal = compareResourcesAssumeFirstNotNil(a, b)
	}
	if equal && (b != nil) {
		equal = compareResourcesAssumeFirstNotNil(b, a)
	}

	return equal
}

func compareResourcesAssumeFirstNotNil(a *v1.ResourceRequirements, b *v1.ResourceRequirements) bool {
	if b == nil || (len(b.Requests) == 0) {
		return len(a.Requests) == 0
	}
	for k, v := range a.Requests {
		if (&v).Cmp(b.Requests[k]) != 0 {
			return false
		}
	}
	for k, v := range a.Limits {
		if (&v).Cmp(b.Limits[k]) != 0 {
			return false
		}
	}
	return true
}

func compareEnv(a, b []v1.EnvVar) bool {
	if len(a) != len(b) {
		return false
	}
	var equal bool
	for _, enva := range a {
		hasmatch := false
		for _, envb := range b {
			if enva.Name == envb.Name {
				hasmatch = true
				if enva.Name == "SPILO_CONFIGURATION" {
					equal = compareSpiloConfiguration(enva.Value, envb.Value)
				} else {
					if enva.Value == "" && envb.Value == "" {
						equal = reflect.DeepEqual(enva.ValueFrom, envb.ValueFrom)
					} else {
						equal = (enva.Value == envb.Value)
					}
				}
				if !equal {
					return false
				}
			}
		}
		if !hasmatch {
			return false
		}
	}
	return true
}

func compareSpiloConfiguration(configa, configb string) bool {
	var oa, ob spiloConfiguration

	var err error
	err = json.Unmarshal([]byte(configa), &oa)
	if err != nil {
		return false
	}
	oa.Bootstrap.DCS = patroniDCS{}
	err = json.Unmarshal([]byte(configb), &ob)
	if err != nil {
		return false
	}
	ob.Bootstrap.DCS = patroniDCS{}
	return reflect.DeepEqual(oa, ob)
}

func areProtocolsEqual(a, b v1.Protocol) bool {
	return a == b ||
		(a == "" && b == v1.ProtocolTCP) ||
		(a == v1.ProtocolTCP && b == "")
}

func comparePorts(a, b []v1.ContainerPort) bool {
	if len(a) != len(b) {
		return false
	}

	areContainerPortsEqual := func(a, b v1.ContainerPort) bool {
		return a.Name == b.Name &&
			a.HostPort == b.HostPort &&
			areProtocolsEqual(a.Protocol, b.Protocol) &&
			a.HostIP == b.HostIP
	}

	findByPortValue := func(portSpecs []v1.ContainerPort, port int32) (v1.ContainerPort, bool) {
		for _, portSpec := range portSpecs {
			if portSpec.ContainerPort == port {
				return portSpec, true
			}
		}
		return v1.ContainerPort{}, false
	}

	for _, portA := range a {
		portB, found := findByPortValue(b, portA.ContainerPort)
		if !found {
			return false
		}
		if !areContainerPortsEqual(portA, portB) {
			return false
		}
	}

	return true
}

func compareVolumeMounts(old, new []v1.VolumeMount) bool {
	if len(old) != len(new) {
		return false
	}
	for _, mount := range old {
		if !volumeMountExists(mount, new) {
			return false
		}
	}
	return true
}

func volumeMountExists(mount v1.VolumeMount, mounts []v1.VolumeMount) bool {
	for _, m := range mounts {
		if reflect.DeepEqual(mount, m) {
			return true
		}
	}
	return false
}

func (c *Cluster) compareAnnotations(old, new map[string]string, removedList *[]string) (bool, string) {
	reason := ""
	ignoredAnnotations := make(map[string]bool)
	for _, ignore := range c.OpConfig.IgnoredAnnotations {
		ignoredAnnotations[ignore] = true
	}

	for key := range old {
		if _, ok := ignoredAnnotations[key]; ok {
			continue
		}
		if _, ok := new[key]; !ok {
			reason += fmt.Sprintf(" Removed %q.", key)
			if removedList != nil {
				*removedList = append(*removedList, key)
			}
		}
	}

	for key := range new {
		if _, ok := ignoredAnnotations[key]; ok {
			continue
		}
		v, ok := old[key]
		if !ok {
			reason += fmt.Sprintf(" Added %q with value %q.", key, new[key])
		} else if v != new[key] {
			reason += fmt.Sprintf(" %q changed from %q to %q.", key, v, new[key])
		}
	}

	return reason != "", reason
}

func (c *Cluster) compareServices(old, new *v1.Service) (bool, string) {
	if old.Spec.Type != new.Spec.Type {
		return false, fmt.Sprintf("new service's type %q does not match the current one %q",
			new.Spec.Type, old.Spec.Type)
	}

	oldSourceRanges := old.Spec.LoadBalancerSourceRanges
	newSourceRanges := new.Spec.LoadBalancerSourceRanges

	/* work around Kubernetes 1.6 serializing [] as nil. See https://github.com/kubernetes/kubernetes/issues/43203 */
	if (len(oldSourceRanges) != 0) || (len(newSourceRanges) != 0) {
		if !util.IsEqualIgnoreOrder(oldSourceRanges, newSourceRanges) {
			return false, "new service's LoadBalancerSourceRange does not match the current one"
		}
	}

	if !reflect.DeepEqual(old.ObjectMeta.OwnerReferences, new.ObjectMeta.OwnerReferences) {
		return false, "new service's owner references do not match the current ones"
	}

	if !reflect.DeepEqual(old.Spec.Selector, new.Spec.Selector) {
		return false, "new service's selector does not match the current one"
	}

	if old.Spec.ExternalTrafficPolicy != new.Spec.ExternalTrafficPolicy {
		return false, "new service's ExternalTrafficPolicy does not match the current one"
	}

	if len(old.Spec.Ports) > 0 && len(new.Spec.Ports) > 0 {
		// we need to check whether the new port is not zero (=user-defined)
		// and only overwrite if it is
		if new.Spec.Ports[0].NodePort != 0 && old.Spec.Ports[0].NodePort != new.Spec.Ports[0].NodePort {
			return false, "new service's NodePort does not match the current one"
		}
	}

	return true, ""
}

func (c *Cluster) compareLogicalBackupJob(cur, new *batchv1.CronJob) *compareLogicalBackupJobResult {
	deletedPodAnnotations := []string{}
	reasons := make([]string, 0)
	match := true

	if cur.Spec.Schedule != new.Spec.Schedule {
		match = false
		reasons = append(reasons, fmt.Sprintf("new job's schedule %q does not match the current one %q", new.Spec.Schedule, cur.Spec.Schedule))
	}

	newImage := new.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image
	curImage := cur.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image
	if newImage != curImage {
		match = false
		reasons = append(reasons, fmt.Sprintf("new job's image %q does not match the current one %q", newImage, curImage))
	}

	newPodAnnotation := new.Spec.JobTemplate.Spec.Template.Annotations
	curPodAnnotation := cur.Spec.JobTemplate.Spec.Template.Annotations
	if changed, reason := c.compareAnnotations(curPodAnnotation, newPodAnnotation, &deletedPodAnnotations); changed {
		match = false
		reasons = append(reasons, fmt.Sprint("new job's pod template metadata annotations do not match "+reason))
	}

	newPgVersion := getPgVersion(new)
	curPgVersion := getPgVersion(cur)
	if newPgVersion != curPgVersion {
		match = false
		reasons = append(reasons, fmt.Sprintf("new job's env PG_VERSION %q does not match the current one %q", newPgVersion, curPgVersion))
	}

	if !reflect.DeepEqual(cur.Labels, new.Labels) {
		match = false
		reasons = append(reasons, "new job's labels do not match the current ones")
	}

	if !reflect.DeepEqual(cur.Spec.JobTemplate.Labels, new.Spec.JobTemplate.Labels) {
		match = false
		reasons = append(reasons, "new job's template labels do not match the current ones")
	}

	needsReplace := false
	contReasons := make([]string, 0)
	needsReplace, contReasons = c.compareContainers("cronjob container", cur.Spec.JobTemplate.Spec.Template.Spec.Containers, new.Spec.JobTemplate.Spec.Template.Spec.Containers, needsReplace, contReasons)
	if needsReplace {
		match = false
		reasons = append(reasons, fmt.Sprintf("logical backup container specs do not match: %v", strings.Join(contReasons, `', '`)))
	}

	if !reflect.DeepEqual(cur.Spec.SuccessfulJobsHistoryLimit, new.Spec.SuccessfulJobsHistoryLimit) {
		match = false
		reasons = append(reasons, fmt.Sprintf("new job's successfulJobsHistoryLimit %v does not match the current one %v", new.Spec.SuccessfulJobsHistoryLimit, cur.Spec.SuccessfulJobsHistoryLimit))
	}

	if !reflect.DeepEqual(cur.Spec.FailedJobsHistoryLimit, new.Spec.FailedJobsHistoryLimit) {
		match = false
		reasons = append(reasons, fmt.Sprintf("new job's failedJobsHistoryLimit %v does not match the current one %v", new.Spec.FailedJobsHistoryLimit, cur.Spec.FailedJobsHistoryLimit))
	}

	if !reflect.DeepEqual(cur.Spec.JobTemplate.Spec.TTLSecondsAfterFinished, new.Spec.JobTemplate.Spec.TTLSecondsAfterFinished) {
		match = false
		reasons = append(reasons, fmt.Sprintf("new job's TTLSecondsAfterFinished %v does not match the current one %v", new.Spec.JobTemplate.Spec.TTLSecondsAfterFinished, cur.Spec.JobTemplate.Spec.TTLSecondsAfterFinished))
	}

	return &compareLogicalBackupJobResult{match: match, reasons: reasons, deletedPodAnnotations: deletedPodAnnotations}
}

func (c *Cluster) comparePodDisruptionBudget(cur, new *policyv1.PodDisruptionBudget) (bool, string) {
	// TODO: improve comparison
	if !reflect.DeepEqual(new.Spec, cur.Spec) {
		return false, "new PDB's spec does not match the current one"
	}
	if !reflect.DeepEqual(new.ObjectMeta.OwnerReferences, cur.ObjectMeta.OwnerReferences) {
		return false, "new PDB's owner references do not match the current ones"
	}
	if changed, reason := c.compareAnnotations(cur.Annotations, new.Annotations, nil); changed {
		return false, "new PDB's annotations do not match the current ones:" + reason
	}
	return true, ""
}

func getPgVersion(cronJob *batchv1.CronJob) string {
	envs := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env
	for _, env := range envs {
		if env.Name == "PG_VERSION" {
			return env.Value
		}
	}
	return ""
}

// addFinalizer patches the postgresql CR to add finalizer
func (c *Cluster) addFinalizer() error {
	if c.hasFinalizer() {
		return nil
	}

	c.logger.Infof("adding finalizer %s", finalizerName)
	finalizers := append(c.ObjectMeta.Finalizers, finalizerName)
	newSpec, err := c.KubeClient.SetFinalizer(c.clusterName(), c.DeepCopy(), finalizers)
	if err != nil {
		return fmt.Errorf("error adding finalizer: %v", err)
	}

	// update the spec, maintaining the new resourceVersion
	c.setSpec(newSpec)

	return nil
}

// removeFinalizer patches postgresql CR to remove finalizer
func (c *Cluster) removeFinalizer() error {
	if !c.hasFinalizer() {
		return nil
	}

	c.logger.Infof("removing finalizer %s", finalizerName)

	// Fetch the latest version of the object to avoid resourceVersion conflicts
	clusterName := c.clusterName()
	latestPg, err := c.KubeClient.PostgresqlsGetter.Postgresqls(clusterName.Namespace).Get(
		context.TODO(), clusterName.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error fetching latest postgresql for finalizer removal: %v", err)
	}

	finalizers := util.RemoveString(latestPg.ObjectMeta.Finalizers, finalizerName)
	newSpec, err := c.KubeClient.SetFinalizer(clusterName, latestPg, finalizers)
	if err != nil {
		return fmt.Errorf("error removing finalizer: %v", err)
	}

	// update the spec, maintaining the new resourceVersion.
	c.setSpec(newSpec)

	return nil
}

// hasFinalizer checks if finalizer is currently set or not
func (c *Cluster) hasFinalizer() bool {
	for _, finalizer := range c.ObjectMeta.Finalizers {
		if finalizer == finalizerName {
			return true
		}
	}
	return false
}

// Update changes Kubernetes objects according to the new specification. Unlike the sync case, the missing object
// (i.e. service) is treated as an error
// logical backup cron jobs are an exception: a user-initiated Update can enable a logical backup job
// for a cluster that had no such job before. In this case a missing job is not an error.
func (c *Cluster) Update(oldSpec, newSpec *acidv1.Postgresql) error {
	updateFailed := false
	userInitFailed := false

	c.mu.Lock()
	defer c.mu.Unlock()

	newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusUpdating

	newSpec, err := c.KubeClient.SetPostgresCRDStatus(c.clusterName(), newSpec)
	if err != nil {
		return fmt.Errorf("could not set cluster status to updating: %w", err)
	}

	if !c.isInMaintenanceWindow(newSpec.Spec.MaintenanceWindows) {
		// do not apply any major version related changes yet
		newSpec.Spec.PostgresqlParam.PgVersion = oldSpec.Spec.PostgresqlParam.PgVersion
	}
	c.setSpec(newSpec)

	defer func() {
		currentStatus := newSpec.Status.DeepCopy()
		newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusRunning

		if updateFailed {
			newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusUpdateFailed
		}

		if !equality.Semantic.DeepEqual(currentStatus, newSpec.Status) {
			pgUpdatedStatus, err := c.KubeClient.SetPostgresCRDStatus(c.clusterName(), newSpec)
			if err != nil {
				c.logger.Warningf("could not set cluster status: %v", err)
				return
			}
			c.setSpec(pgUpdatedStatus)
		}
	}()

	logNiceDiff(c.logger, oldSpec, newSpec)

	if IsBiggerPostgresVersion(oldSpec.Spec.PostgresqlParam.PgVersion, c.GetDesiredMajorVersion()) {
		c.logger.Infof("postgresql version increased (%s -> %s), depending on config manual upgrade needed",
			oldSpec.Spec.PostgresqlParam.PgVersion, newSpec.Spec.PostgresqlParam.PgVersion)
	} else {
		c.logger.Infof("postgresql major version unchanged or smaller, no changes needed")
		// sticking with old version, this will also advance GetDesiredVersion next time.
		newSpec.Spec.PostgresqlParam.PgVersion = oldSpec.Spec.PostgresqlParam.PgVersion
	}

	// Service
	if err := c.syncServices(); err != nil {
		c.logger.Errorf("could not sync services: %v", err)
		updateFailed = true
	}

	// Patroni service and endpoints / config maps
	if err := c.syncPatroniResources(); err != nil {
		c.logger.Errorf("could not sync Patroni resources: %v", err)
		updateFailed = true
	}

	// Users
	func() {
		// check if users need to be synced during update
		sameUsers := reflect.DeepEqual(oldSpec.Spec.Users, newSpec.Spec.Users) &&
			reflect.DeepEqual(oldSpec.Spec.PreparedDatabases, newSpec.Spec.PreparedDatabases)
		sameRotatedUsers := reflect.DeepEqual(oldSpec.Spec.UsersWithSecretRotation, newSpec.Spec.UsersWithSecretRotation) &&
			reflect.DeepEqual(oldSpec.Spec.UsersWithInPlaceSecretRotation, newSpec.Spec.UsersWithInPlaceSecretRotation)

		// connection pooler needs one system user created who is initialized in initUsers
		// only when disabled in oldSpec and enabled in newSpec
		needPoolerUser := c.needConnectionPoolerUser(&oldSpec.Spec, &newSpec.Spec)

		// streams new replication user created who is initialized in initUsers
		// only when streams were not specified in oldSpec but in newSpec
		needStreamUser := len(oldSpec.Spec.Streams) == 0 && len(newSpec.Spec.Streams) > 0

		initUsers := !sameUsers || !sameRotatedUsers || needPoolerUser || needStreamUser

		// if inherited annotations differ secrets have to be synced on update
		newAnnotations := c.annotationsSet(nil)
		oldAnnotations := make(map[string]string)
		for _, secret := range c.Secrets {
			oldAnnotations = secret.ObjectMeta.Annotations
			break
		}
		annotationsChanged, _ := c.compareAnnotations(oldAnnotations, newAnnotations, nil)

		if initUsers || annotationsChanged {
			c.logger.Debug("initialize users")
			if err := c.initUsers(); err != nil {
				c.logger.Errorf("could not init users - skipping sync of secrets and databases: %v", err)
				userInitFailed = true
				updateFailed = true
				return
			}

			c.logger.Debug("syncing secrets")
			// TODO: mind the secrets of the deleted/new users
			if err := c.syncSecrets(); err != nil {
				c.logger.Errorf("could not sync secrets: %v", err)
				updateFailed = true
			}
		}
	}()

	// Volume
	if c.OpConfig.StorageResizeMode != "off" {
		c.syncVolumes()
	} else {
		c.logger.Infof("Storage resize is disabled (storage_resize_mode is off). Skipping volume size sync.")
	}

	// Pod service account (IRSA annotation sync)
	if err := c.syncPodServiceAccount(); err != nil {
		c.logger.Errorf("could not sync pod service account: %v", err)
		updateFailed = true
	}

	// Statefulset
	func() {
		if err := c.syncStatefulSet(); err != nil {
			c.logger.Errorf("could not sync statefulsets: %v", err)
			updateFailed = true
		}
	}()

	// add or remove standby_cluster section from Patroni config depending on changes in standby section
	if !reflect.DeepEqual(oldSpec.Spec.StandbyCluster, newSpec.Spec.StandbyCluster) {
		if err := c.syncStandbyClusterConfiguration(); err != nil {
			return fmt.Errorf("could not set StandbyCluster configuration options: %v", err)
		}
	}

	// pod disruption budgets
	if err := c.syncPodDisruptionBudgets(true); err != nil {
		c.logger.Errorf("could not sync pod disruption budgets: %v", err)
		updateFailed = true
	}

	// logical backup job
	func() {
		// create if it did not exist
		if !oldSpec.Spec.EnableLogicalBackup && newSpec.Spec.EnableLogicalBackup {
			c.logger.Debug("creating backup cron job")
			if err := c.createLogicalBackupJob(); err != nil {
				c.logger.Errorf("could not create a k8s cron job for logical backups: %v", err)
				updateFailed = true
				return
			}
		}

		// delete if no longer needed
		if oldSpec.Spec.EnableLogicalBackup && !newSpec.Spec.EnableLogicalBackup {
			c.logger.Debug("deleting backup cron job")
			if err := c.deleteLogicalBackupJob(); err != nil {
				c.logger.Errorf("could not delete a k8s cron job for logical backups: %v", err)
				updateFailed = true
				return
			}

		}

		if oldSpec.Spec.EnableLogicalBackup && newSpec.Spec.EnableLogicalBackup {
			if err := c.syncLogicalBackupJob(); err != nil {
				c.logger.Errorf("could not sync logical backup jobs: %v", err)
				updateFailed = true
			}
		}
	}()

	// Roles and Databases
	if !userInitFailed && !(c.databaseAccessDisabled() || c.getNumberOfInstances(&c.Spec) <= 0 || c.Spec.StandbyCluster != nil) {
		c.logger.Debug("syncing roles")
		if err := c.syncRoles(); err != nil {
			c.logger.Errorf("could not sync roles: %v", err)
			updateFailed = true
		}
		if !reflect.DeepEqual(oldSpec.Spec.Databases, newSpec.Spec.Databases) ||
			!reflect.DeepEqual(oldSpec.Spec.PreparedDatabases, newSpec.Spec.PreparedDatabases) {
			c.logger.Infof("syncing databases")
			if err := c.syncDatabases(); err != nil {
				c.logger.Errorf("could not sync databases: %v", err)
				updateFailed = true
			}
		}
		if !reflect.DeepEqual(oldSpec.Spec.PreparedDatabases, newSpec.Spec.PreparedDatabases) {
			c.logger.Infof("syncing prepared databases")
			if err := c.syncPreparedDatabases(); err != nil {
				c.logger.Errorf("could not sync prepared databases: %v", err)
				updateFailed = true
			}
		}
	}

	// Sync connection pooler. Before actually doing sync reset lookup
	// installation flag, since manifest updates could add another db which we
	// need to process. In the future we may want to do this more careful and
	// check which databases we need to process, but even repeating the whole
	// installation process should be good enough.
	if _, err := c.syncConnectionPooler(oldSpec, newSpec, c.installLookupFunction); err != nil {
		c.logger.Errorf("could not sync connection pooler: %v", err)
		updateFailed = true
	}

	// streams
	if len(newSpec.Spec.Streams) > 0 || len(oldSpec.Spec.Streams) != len(newSpec.Spec.Streams) {
		c.logger.Debug("syncing streams")
		if err := c.syncStreams(); err != nil {
			c.logger.Errorf("could not sync streams: %v", err)
			updateFailed = true
		}
	}

	if !updateFailed {
		// Major version upgrade must only fire after success of earlier operations and should stay last
		if err := c.majorVersionUpgrade(); err != nil {
			c.logger.Errorf("major version upgrade failed: %v", err)
			updateFailed = true
		}
	}

	return nil
}

func syncResources(a, b *v1.ResourceRequirements) bool {
	for _, res := range []v1.ResourceName{
		v1.ResourceCPU,
		v1.ResourceMemory,
	} {
		if !a.Limits[res].Equal(b.Limits[res]) ||
			!a.Requests[res].Equal(b.Requests[res]) {
			return true
		}
	}

	return false
}

// Delete deletes the cluster and cleans up all objects associated with it (including statefulsets).
// The deletion order here is somewhat significant, because Patroni, when running with the Kubernetes
// DCS, reuses the master's endpoint to store the leader related metadata. If we remove the endpoint
// before the pods, it will be re-created by the current master pod and will remain, obstructing the
// creation of the new cluster with the same name. Therefore, the endpoints should be deleted last.
func (c *Cluster) Delete() error {
	anyErrors := false
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Delete", "Started deletion of cluster resources")

	if err := c.deleteStreams(); err != nil {
		anyErrors = true
		c.logger.Warningf("could not delete event streams: %v", err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete event streams: %v", err)
	}

	// delete the backup job before the stateful set of the cluster to prevent connections to non-existing pods
	// deleting the cron job also removes pods and batch jobs it created
	if err := c.deleteLogicalBackupJob(); err != nil {
		anyErrors = true
		c.logger.Warningf("could not remove the logical backup k8s cron job; %v", err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not remove the logical backup k8s cron job; %v", err)
	}

	if err := c.deleteStatefulSet(); err != nil {
		anyErrors = true
		c.logger.Warningf("could not delete statefulset: %v", err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete statefulset: %v", err)
	}

	if c.OpConfig.EnableSecretsDeletion != nil && *c.OpConfig.EnableSecretsDeletion {
		if err := c.deleteSecrets(); err != nil {
			anyErrors = true
			c.logger.Warningf("could not delete secrets: %v", err)
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete secrets: %v", err)
		}
	} else {
		c.logger.Info("not deleting secrets because disabled in configuration")
	}

	if err := c.deletePodDisruptionBudgets(); err != nil {
		anyErrors = true
		c.logger.Warningf("could not delete pod disruption budgets: %v", err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete pod disruption budgets: %v", err)
	}

	for _, role := range []PostgresRole{Master, Replica} {
		if !c.patroniKubernetesUseConfigMaps() {
			if err := c.deleteEndpoint(role); err != nil {
				anyErrors = true
				c.logger.Warningf("could not delete %s endpoint: %v", role, err)
				c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete %s endpoint: %v", role, err)
			}
		}

		if err := c.deleteService(role); err != nil {
			anyErrors = true
			c.logger.Warningf("could not delete %s service: %v", role, err)
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete %s service: %v", role, err)
		}
	}

	if err := c.deletePatroniResources(); err != nil {
		anyErrors = true
		c.logger.Warningf("could not delete all Patroni resources: %v", err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not delete all Patroni resources: %v", err)
	}

	// Delete connection pooler objects anyway, even if it's not mentioned in the
	// manifest, just to not keep orphaned components in case if something went
	// wrong
	for _, role := range [2]PostgresRole{Master, Replica} {
		if err := c.deleteConnectionPooler(role); err != nil {
			anyErrors = true
			c.logger.Warningf("could not remove connection pooler: %v", err)
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "Delete", "could not remove connection pooler: %v", err)
		}
	}

	// If we are done deleting our various resources we remove the finalizer to let K8S finally delete the Postgres CR
	if anyErrors {
		c.eventRecorder.Event(c.GetReference(), v1.EventTypeWarning, "Delete", "some resources could be successfully deleted yet")
		return fmt.Errorf("some error(s) occurred when deleting resources, NOT removing finalizer yet")
	}
	if err := c.removeFinalizer(); err != nil {
		return fmt.Errorf("done cleaning up, but error when removing finalizer: %v", err)
	}

	return nil
}

// NeedsRepair returns true if the cluster should be included in the repair scan (based on its in-memory status).
func (c *Cluster) NeedsRepair() (bool, acidv1.PostgresStatus) {
	c.specMu.RLock()
	defer c.specMu.RUnlock()
	return !c.Status.Success(), c.Status
}

// ReceivePodEvent is called back by the controller in order to add the cluster's pod event to the queue.
func (c *Cluster) ReceivePodEvent(event PodEvent) {
	if err := c.podEventsStore.Add(event); err != nil {
		c.logger.Errorf("error when receiving pod event for lookup: %v", err)
	}
	if err := c.podEventsQueue.Add(event); err != nil {
		c.logger.Errorf("error when receiving pod events: %v", err)
	}
}

func (c *Cluster) processPodEvent(obj interface{}, isInInitialList bool) error {
	event, ok := obj.(PodEvent)
	if !ok {
		return fmt.Errorf("could not cast to PodEvent")
	}

	// can only take lock when (un)registerPodSubscriber is finshed
	c.podSubscribersMu.RLock()
	subscriber, ok := c.podSubscribers[spec.NamespacedName(event.PodName)]
	if ok {
		select {
		case subscriber <- event:
		default:
			// ending up here when there is no receiver on the channel (i.e. waitForPodLabel finished)
			// avoids blocking channel: https://gobyexample.com/non-blocking-channel-operations
		}
	}
	// hold lock for the time of processing the event to avoid race condition
	// with unregisterPodSubscriber closing the channel (see #1876)
	c.podSubscribersMu.RUnlock()

	return nil
}

// Run starts the pod event dispatching for the given cluster.
func (c *Cluster) Run(stopCh <-chan struct{}) {
	go c.processPodEventQueue(stopCh)
}

func (c *Cluster) processPodEventQueue(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			_, err := c.podEventsQueue.Pop(cache.PopProcessFunc(func(obj interface{}, isInInitialList bool) error {
				event, ok := obj.(PodEvent)
				if !ok {
					c.logger.Errorf("could not cast to pod event")
					return nil // skip event to keep processing
				}
				c.processPodEvent(event, isInInitialList)
				if err := c.podEventsStore.Delete(obj); err != nil {
					c.logger.Errorf("failed to delete key from lookup store: %v", err)
				}
				return nil
			}))
			if err != nil {
				c.logger.Errorf("error when processing pod event queue %v", err)
			}
		}
	}
}

func (c *Cluster) initSystemUsers() {
	// We don't actually use that to create users, delegating this
	// task to Patroni. Those definitions are only used to create
	// secrets, therefore, setting flags like SUPERUSER or REPLICATION
	// is not necessary here
	c.systemUsers[constants.SuperuserKeyName] = spec.PgUser{
		Origin:    spec.RoleOriginSystem,
		Name:      c.OpConfig.SuperUsername,
		Namespace: c.Namespace,
		Password:  util.RandomPassword(constants.PasswordLength),
	}
	c.systemUsers[constants.ReplicationUserKeyName] = spec.PgUser{
		Origin:    spec.RoleOriginSystem,
		Name:      c.OpConfig.ReplicationUsername,
		Namespace: c.Namespace,
		Flags:     []string{constants.RoleFlagLogin},
		Password:  util.RandomPassword(constants.PasswordLength),
	}

	// Connection pooler user is an exception
	// if requested it's going to be created by operator
	if needConnectionPooler(&c.Spec) {
		username := c.poolerUser(&c.Spec)

		// connection pooler application should be able to login with this role
		connectionPoolerUser := spec.PgUser{
			Origin:    spec.RoleOriginConnectionPooler,
			Name:      username,
			Namespace: c.Namespace,
			Flags:     []string{constants.RoleFlagLogin},
			Password:  util.RandomPassword(constants.PasswordLength),
		}

		if _, exists := c.systemUsers[constants.ConnectionPoolerUserKeyName]; !exists {
			c.systemUsers[constants.ConnectionPoolerUserKeyName] = connectionPoolerUser
		}
	}

	// replication users for event streams are another exception
	// the operator will create one replication user for all streams
	if len(c.Spec.Streams) > 0 {
		username := fmt.Sprintf("%s%s", constants.EventStreamSourceSlotPrefix, constants.UserRoleNameSuffix)
		streamUser := spec.PgUser{
			Origin:    spec.RoleOriginStream,
			Name:      username,
			Namespace: c.Namespace,
			Flags:     []string{constants.RoleFlagLogin, constants.RoleFlagReplication},
			Password:  util.RandomPassword(constants.PasswordLength),
		}

		if _, exists := c.systemUsers[constants.EventStreamUserKeyName]; !exists {
			c.systemUsers[constants.EventStreamUserKeyName] = streamUser
		}
	}
}

func (c *Cluster) initPreparedDatabaseRoles() error {
	if c.Spec.PreparedDatabases != nil && len(c.Spec.PreparedDatabases) == 0 { // TODO: add option to disable creating such a default DB
		c.Spec.PreparedDatabases = map[string]acidv1.PreparedDatabase{strings.Replace(c.Name, "-", "_", -1): {}}
	}

	// create maps with default roles/users as keys and their membership as values
	defaultRoles := map[string]string{
		constants.OwnerRoleNameSuffix:  "",
		constants.ReaderRoleNameSuffix: "",
		constants.WriterRoleNameSuffix: constants.ReaderRoleNameSuffix,
	}
	defaultUsers := map[string]string{
		fmt.Sprintf("%s%s", constants.OwnerRoleNameSuffix, constants.UserRoleNameSuffix):  constants.OwnerRoleNameSuffix,
		fmt.Sprintf("%s%s", constants.ReaderRoleNameSuffix, constants.UserRoleNameSuffix): constants.ReaderRoleNameSuffix,
		fmt.Sprintf("%s%s", constants.WriterRoleNameSuffix, constants.UserRoleNameSuffix): constants.WriterRoleNameSuffix,
	}

	for preparedDbName, preparedDB := range c.Spec.PreparedDatabases {
		// get list of prepared schemas to set in search_path
		preparedSchemas := preparedDB.PreparedSchemas
		if len(preparedDB.PreparedSchemas) == 0 {
			preparedSchemas = map[string]acidv1.PreparedSchema{"data": {DefaultRoles: util.True()}}
		}

		searchPathArr := []string{constants.DefaultSearchPath}
		for preparedSchemaName := range preparedSchemas {
			searchPathArr = append(searchPathArr, fmt.Sprintf("%q", preparedSchemaName))
		}
		searchPath := strings.Join(searchPathArr, ", ")

		// default roles per database
		if err := c.initDefaultRoles(defaultRoles, "admin", preparedDbName, searchPath, preparedDB.SecretNamespace); err != nil {
			return fmt.Errorf("could not initialize default roles for database %s: %v", preparedDbName, err)
		}
		if preparedDB.DefaultUsers {
			if err := c.initDefaultRoles(defaultUsers, "admin", preparedDbName, searchPath, preparedDB.SecretNamespace); err != nil {
				return fmt.Errorf("could not initialize default roles for database %s: %v", preparedDbName, err)
			}
		}

		// default roles per database schema
		for preparedSchemaName, preparedSchema := range preparedSchemas {
			if preparedSchema.DefaultRoles == nil || *preparedSchema.DefaultRoles {
				if err := c.initDefaultRoles(defaultRoles,
					preparedDbName+constants.OwnerRoleNameSuffix,
					preparedDbName+"_"+preparedSchemaName,
					fmt.Sprintf("%s, %q", constants.DefaultSearchPath, preparedSchemaName),
					preparedDB.SecretNamespace); err != nil {
					return fmt.Errorf("could not initialize default roles for database schema %s: %v", preparedSchemaName, err)
				}
				if preparedSchema.DefaultUsers {
					if err := c.initDefaultRoles(defaultUsers,
						preparedDbName+constants.OwnerRoleNameSuffix,
						preparedDbName+"_"+preparedSchemaName,
						fmt.Sprintf("%s, %q", constants.DefaultSearchPath, preparedSchemaName),
						preparedDB.SecretNamespace); err != nil {
						return fmt.Errorf("could not initialize default users for database schema %s: %v", preparedSchemaName, err)
					}
				}
			}
		}
	}
	return nil
}

func (c *Cluster) initDefaultRoles(defaultRoles map[string]string, admin, prefix, searchPath, secretNamespace string) error {
	for defaultRole, inherits := range defaultRoles {
		namespace := c.Namespace
		// if namespaced secrets are allowed
		if secretNamespace != "" {
			if c.Config.OpConfig.EnableCrossNamespaceSecret {
				namespace = secretNamespace
			} else {
				c.logger.Warn("secretNamespace ignored because enable_cross_namespace_secret set to false. Creating secrets in cluster namespace.")
			}
		}
		roleName := fmt.Sprintf("%s%s", prefix, defaultRole)

		flags := []string{constants.RoleFlagNoLogin}
		if defaultRole[len(defaultRole)-5:] == constants.UserRoleNameSuffix {
			flags = []string{constants.RoleFlagLogin}
		}

		memberOf := make([]string, 0)
		if inherits != "" {
			memberOf = append(memberOf, prefix+inherits)
		}

		adminRole := ""
		isOwner := false
		if strings.Contains(defaultRole, constants.OwnerRoleNameSuffix) {
			adminRole = admin
			isOwner = true
		} else {
			adminRole = fmt.Sprintf("%s%s", prefix, constants.OwnerRoleNameSuffix)
		}

		newRole := spec.PgUser{
			Origin:     spec.RoleOriginBootstrap,
			Name:       roleName,
			Namespace:  namespace,
			Password:   util.RandomPassword(constants.PasswordLength),
			Flags:      flags,
			MemberOf:   memberOf,
			Parameters: map[string]string{"search_path": searchPath},
			AdminRole:  adminRole,
			IsDbOwner:  isOwner,
		}
		if currentRole, present := c.pgUsers[roleName]; present {
			c.pgUsers[roleName] = c.resolveNameConflict(&currentRole, &newRole)
		} else {
			c.pgUsers[roleName] = newRole
		}
	}
	return nil
}

func (c *Cluster) initRobotUsers() error {
	for username, userFlags := range c.Spec.Users {
		if !isValidUsername(username) {
			return fmt.Errorf("invalid username: %q", username)
		}

		if c.shouldAvoidProtectedOrSystemRole(username, "manifest robot role") {
			continue
		}
		namespace := c.Namespace

		// check if role is specified as database owner
		isOwner := false
		for _, owner := range c.Spec.Databases {
			if username == owner {
				isOwner = true
			}
		}

		// if namespaced secrets are allowed
		if c.Config.OpConfig.EnableCrossNamespaceSecret {
			if strings.Contains(username, ".") {
				splits := strings.Split(username, ".")
				namespace = splits[0]
				c.logger.Warningf("enable_cross_namespace_secret is set. Database role name contains the respective namespace i.e. %s is the created user", username)
			}
		}

		flags, err := normalizeUserFlags(userFlags)
		if err != nil {
			return fmt.Errorf("invalid flags for user %q: %v", username, err)
		}
		adminRole := ""
		if c.OpConfig.EnableAdminRoleForUsers {
			adminRole = c.OpConfig.TeamAdminRole
		}
		newRole := spec.PgUser{
			Origin:    spec.RoleOriginManifest,
			Name:      username,
			Namespace: namespace,
			Password:  util.RandomPassword(constants.PasswordLength),
			Flags:     flags,
			AdminRole: adminRole,
			IsDbOwner: isOwner,
		}
		if currentRole, present := c.pgUsers[username]; present {
			c.pgUsers[username] = c.resolveNameConflict(&currentRole, &newRole)
		} else {
			c.pgUsers[username] = newRole
		}
	}
	return nil
}

func (c *Cluster) initAdditionalOwnerRoles() {
	if len(c.OpConfig.AdditionalOwnerRoles) == 0 {
		return
	}

	// fetch database owners and assign additional owner roles
	for username, pgUser := range c.pgUsers {
		if pgUser.IsDbOwner {
			pgUser.MemberOf = append(pgUser.MemberOf, c.OpConfig.AdditionalOwnerRoles...)
			c.pgUsers[username] = pgUser
		}
	}
}

func (c *Cluster) initTeamMembers(teamID string, isPostgresSuperuserTeam bool) error {
	teamMembers, err := c.getTeamMembers(teamID)
	if err != nil {
		return fmt.Errorf("could not get list of team members for team %q: %v", teamID, err)
	}

	for _, username := range teamMembers {
		flags := []string{constants.RoleFlagLogin}
		memberOf := []string{c.OpConfig.PamRoleName}

		if c.shouldAvoidProtectedOrSystemRole(username, "API role") {
			continue
		}
		if (c.OpConfig.EnableTeamSuperuser && teamID == c.Spec.TeamID) || isPostgresSuperuserTeam {
			flags = append(flags, constants.RoleFlagSuperuser)
		} else {
			if c.OpConfig.TeamAdminRole != "" {
				memberOf = append(memberOf, c.OpConfig.TeamAdminRole)
			}
		}

		newRole := spec.PgUser{
			Origin:     spec.RoleOriginTeamsAPI,
			Name:       username,
			Flags:      flags,
			MemberOf:   memberOf,
			Parameters: c.OpConfig.TeamAPIRoleConfiguration,
		}

		if currentRole, present := c.pgUsers[username]; present {
			c.pgUsers[username] = c.resolveNameConflict(&currentRole, &newRole)
		} else {
			c.pgUsers[username] = newRole
		}
	}

	return nil
}

func (c *Cluster) initHumanUsers() error {
	var clusterIsOwnedBySuperuserTeam bool
	superuserTeams := []string{}

	if c.OpConfig.EnablePostgresTeamCRD && c.OpConfig.EnablePostgresTeamCRDSuperusers && c.Config.PgTeamMap != nil {
		superuserTeams = c.Config.PgTeamMap.GetAdditionalSuperuserTeams(c.Spec.TeamID, true)
	}

	for _, postgresSuperuserTeam := range c.OpConfig.PostgresSuperuserTeams {
		if !(util.SliceContains(superuserTeams, postgresSuperuserTeam)) {
			superuserTeams = append(superuserTeams, postgresSuperuserTeam)
		}
	}

	for _, superuserTeam := range superuserTeams {
		err := c.initTeamMembers(superuserTeam, true)
		if err != nil {
			return fmt.Errorf("cannot initialize members for team %q of Postgres superusers: %v", superuserTeam, err)
		}
		if superuserTeam == c.Spec.TeamID {
			clusterIsOwnedBySuperuserTeam = true
		}
	}

	if c.OpConfig.EnablePostgresTeamCRD && c.Config.PgTeamMap != nil {
		additionalTeams := c.Config.PgTeamMap.GetAdditionalTeams(c.Spec.TeamID, true)
		for _, additionalTeam := range additionalTeams {
			if !(util.SliceContains(superuserTeams, additionalTeam)) {
				err := c.initTeamMembers(additionalTeam, false)
				if err != nil {
					return fmt.Errorf("cannot initialize members for additional team %q for cluster owned by %q: %v", additionalTeam, c.Spec.TeamID, err)
				}
			}
		}
	}

	if clusterIsOwnedBySuperuserTeam {
		c.logger.Infof("Team %q owning the cluster is also a team of superusers. Created superuser roles for its members instead of admin roles.", c.Spec.TeamID)
		return nil
	}

	err := c.initTeamMembers(c.Spec.TeamID, false)
	if err != nil {
		return fmt.Errorf("cannot initialize members for team %q who owns the Postgres cluster: %v", c.Spec.TeamID, err)
	}

	return nil
}

func (c *Cluster) initInfrastructureRoles() error {
	// add infrastructure roles from the operator's definition
	for username, newRole := range c.InfrastructureRoles {
		if !isValidUsername(username) {
			return fmt.Errorf("invalid username: '%v'", username)
		}
		if c.shouldAvoidProtectedOrSystemRole(username, "infrastructure role") {
			continue
		}
		flags, err := normalizeUserFlags(newRole.Flags)
		if err != nil {
			return fmt.Errorf("invalid flags for user '%v': %v", username, err)
		}
		newRole.Flags = flags
		newRole.Namespace = c.Namespace

		if currentRole, present := c.pgUsers[username]; present {
			c.pgUsers[username] = c.resolveNameConflict(&currentRole, &newRole)
		} else {
			c.pgUsers[username] = newRole
		}
	}
	return nil
}

// resolves naming conflicts between existing and new roles by choosing either of them.
func (c *Cluster) resolveNameConflict(currentRole, newRole *spec.PgUser) spec.PgUser {
	var result spec.PgUser
	if newRole.Origin >= currentRole.Origin {
		result = *newRole
	} else {
		result = *currentRole
	}
	c.logger.Debugf("resolved a conflict of role %q between %s and %s to %s",
		newRole.Name, newRole.Origin, currentRole.Origin, result.Origin)
	return result
}

func (c *Cluster) shouldAvoidProtectedOrSystemRole(username, purpose string) bool {
	if c.isProtectedUsername(username) {
		c.logger.Warnf("cannot initialize a new %s with the name of the protected user %q", purpose, username)
		return true
	}
	if c.isSystemUsername(username) {
		c.logger.Warnf("cannot initialize a new %s with the name of the system user %q", purpose, username)
		return true
	}
	return false
}

// GetCurrentProcess provides name of the last process of the cluster
func (c *Cluster) GetCurrentProcess() Process {
	c.processMu.RLock()
	defer c.processMu.RUnlock()

	return c.currentProcess
}

// GetStatus provides status of the cluster
func (c *Cluster) GetStatus() *ClusterStatus {
	status := &ClusterStatus{
		Cluster:                       c.Name,
		Namespace:                     c.Namespace,
		Team:                          c.Spec.TeamID,
		Status:                        c.Status,
		Spec:                          c.Spec,
		MasterService:                 c.GetServiceMaster(),
		ReplicaService:                c.GetServiceReplica(),
		StatefulSet:                   c.GetStatefulSet(),
		PrimaryPodDisruptionBudget:    c.GetPrimaryPodDisruptionBudget(),
		CriticalOpPodDisruptionBudget: c.GetCriticalOpPodDisruptionBudget(),
		CurrentProcess:                c.GetCurrentProcess(),

		Error: fmt.Errorf("error: %s", c.Error),
	}

	if !c.patroniKubernetesUseConfigMaps() {
		status.MasterEndpoint = c.GetEndpointMaster()
		status.ReplicaEndpoint = c.GetEndpointReplica()
	}

	return status
}

func (c *Cluster) GetSwitchoverSchedule() string {
	now := time.Now().UTC()
	return c.getSwitchoverScheduleAtTime(now)
}

func (c *Cluster) getSwitchoverScheduleAtTime(now time.Time) string {
	var possibleSwitchover, schedule time.Time

	maintenanceWindows := c.Spec.MaintenanceWindows
	if len(maintenanceWindows) == 0 {
		maintenanceWindows = make([]acidv1.MaintenanceWindow, 0, len(c.OpConfig.MaintenanceWindows))
		for _, windowStr := range c.OpConfig.MaintenanceWindows {
			var window acidv1.MaintenanceWindow
			if err := window.UnmarshalJSON([]byte(windowStr)); err != nil {
				c.logger.Errorf("could not parse default maintenance window %q: %v", windowStr, err)
				continue
			}
			maintenanceWindows = append(maintenanceWindows, window)
		}
	}

	for _, window := range maintenanceWindows {
		// in the best case it is possible today
		possibleSwitchover = time.Date(now.Year(), now.Month(), now.Day(), window.StartTime.Hour(), window.StartTime.Minute(), 0, 0, time.UTC)
		if window.Everyday {
			if now.After(possibleSwitchover) {
				// we are already past the time for today, try tomorrow
				possibleSwitchover = possibleSwitchover.AddDate(0, 0, 1)
			}
		} else {
			if now.Weekday() != window.Weekday {
				// get closest possible time for this window
				possibleSwitchover = possibleSwitchover.AddDate(0, 0, int((7+window.Weekday-now.Weekday())%7))
			} else if now.After(possibleSwitchover) {
				// we are already past the time for today, try next week
				possibleSwitchover = possibleSwitchover.AddDate(0, 0, 7)
			}
		}

		if (schedule.Equal(time.Time{})) || possibleSwitchover.Before(schedule) {
			schedule = possibleSwitchover
		}
	}

	if schedule.IsZero() {
		return ""
	}

	return schedule.Format("2006-01-02T15:04+00")
}

// Switchover does a switchover (via Patroni) to a candidate pod
func (c *Cluster) Switchover(curMaster *v1.Pod, candidate spec.NamespacedName, scheduled bool) error {
	var err error

	stopCh := make(chan struct{})
	ch := c.registerPodSubscriber(candidate)
	defer c.unregisterPodSubscriber(candidate)
	defer close(stopCh)

	var scheduled_at string
	if scheduled {
		scheduled_at = c.GetSwitchoverSchedule()
	} else {
		c.logger.Debugf("switching over from %q to %q", curMaster.Name, candidate)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Switchover", "Switching over from %q to %q", curMaster.Name, candidate)
		scheduled_at = ""
	}

	if err = c.patroni.Switchover(curMaster, candidate.Name, scheduled_at); err == nil {
		if scheduled {
			c.logger.Infof("switchover from %q to %q is scheduled at %s", curMaster.Name, candidate, scheduled_at)
			return nil
		}
		c.logger.Debugf("successfully switched over from %q to %q", curMaster.Name, candidate)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Switchover", "Successfully switched over from %q to %q", curMaster.Name, candidate)
		_, err = c.waitForPodLabel(ch, stopCh, nil)
		if err != nil {
			err = fmt.Errorf("could not get master pod label: %v", err)
		}
	} else {
		if scheduled {
			return fmt.Errorf("could not schedule switchover: %v", err)
		}
		err = fmt.Errorf("could not switch over from %q to %q: %v", curMaster.Name, candidate, err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Switchover", "Switchover from %q to %q FAILED: %v", curMaster.Name, candidate, err)
	}

	return err
}

// Lock locks the cluster
func (c *Cluster) Lock() {
	c.mu.Lock()
}

// Unlock unlocks the cluster
func (c *Cluster) Unlock() {
	c.mu.Unlock()
}
