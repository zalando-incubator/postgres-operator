package cluster

// Postgres CustomResourceDefinition object i.e. Spilo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"

	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/scheme"
	"github.com/zalando/postgres-operator/pkg/spec"
	pgteams "github.com/zalando/postgres-operator/pkg/teams"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/pkg/util/patroni"
	"github.com/zalando/postgres-operator/pkg/util/teams"
	"github.com/zalando/postgres-operator/pkg/util/users"
	"github.com/zalando/postgres-operator/pkg/util/volumes"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	policybeta1 "k8s.io/api/policy/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
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
	patroniObjectSuffixes = []string{"config", "failover", "sync"}
	newDBCount 	          = promauto.NewCounter(prometheus.CounterOpts{Name: "pg_new_db", Help: "The total number newly DB created"})
// Count DB added
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
	Services            map[PostgresRole]*v1.Service
	Endpoints           map[PostgresRole]*v1.Endpoints
	Secrets             map[types.UID]*v1.Secret
	Statefulset         *appsv1.StatefulSet
	PodDisruptionBudget *policybeta1.PodDisruptionBudget
	//Pods are treated separately
	//PVCs are treated separately
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
	podEventsQueue   *cache.FIFO

	teamsAPIClient      teams.Interface
	oauthTokenGetter    OAuthTokenGetter
	KubeClient          k8sutil.KubernetesClient //TODO: move clients to the better place?
	currentProcess      Process
	processMu           sync.RWMutex // protects the current operation for reporting, no need to hold the master mutex
	specMu              sync.RWMutex // protects the spec for reporting, no need to hold the master mutex
	ConnectionPooler    map[PostgresRole]*ConnectionPoolerObjects
	EBSVolumes          map[string]volumes.VolumeProperties
	VolumeResizer       volumes.VolumeResizer
	currentMajorVersion int
}

type compareStatefulsetResult struct {
	match         bool
	replace       bool
	rollingUpdate bool
	reasons       []string
}

// New creates a new cluster. This function should be called from a controller.
func New(cfg Config, kubeClient k8sutil.KubernetesClient, pgSpec acidv1.Postgresql, logger *logrus.Entry, eventRecorder record.EventRecorder) *Cluster {
	deletePropagationPolicy := metav1.DeletePropagationOrphan

	podEventsQueue := cache.NewFIFO(func(obj interface{}) (string, error) {
		e, ok := obj.(PodEvent)
		if !ok {
			return "", fmt.Errorf("could not cast to PodEvent")
		}

		return fmt.Sprintf("%s-%s", e.PodName, e.ResourceVersion), nil
	})
	passwordEncryption, ok := pgSpec.Spec.PostgresqlParam.Parameters["password_encryption"]
	if !ok {
		passwordEncryption = "md5"
	}

	cluster := &Cluster{
		Config:         cfg,
		Postgresql:     pgSpec,
		pgUsers:        make(map[string]spec.PgUser),
		systemUsers:    make(map[string]spec.PgUser),
		podSubscribers: make(map[spec.NamespacedName]chan PodEvent),
		kubeResources: kubeResources{
			Secrets:   make(map[types.UID]*v1.Secret),
			Services:  make(map[PostgresRole]*v1.Service),
			Endpoints: make(map[PostgresRole]*v1.Endpoints)},
		userSyncStrategy: users.DefaultUserSyncStrategy{
			PasswordEncryption: passwordEncryption,
			RoleDeletionSuffix: cfg.OpConfig.RoleDeletionSuffix},
		deleteOptions:       metav1.DeleteOptions{PropagationPolicy: &deletePropagationPolicy},
		podEventsQueue:      podEventsQueue,
		KubeClient:          kubeClient,
		currentMajorVersion: 0,
	}
	cluster.logger = logger.WithField("pkg", "cluster").WithField("cluster-name", cluster.clusterName())
	cluster.teamsAPIClient = teams.NewTeamsAPI(cfg.OpConfig.TeamsAPIUrl, logger)
	cluster.oauthTokenGetter = newSecretOauthTokenGetter(&kubeClient, cfg.OpConfig.OAuthTokenSecretName)
	cluster.patroni = patroni.New(cluster.logger, nil)
	cluster.eventRecorder = eventRecorder

	cluster.EBSVolumes = make(map[string]volumes.VolumeProperties)
	if cfg.OpConfig.StorageResizeMode != "pvc" || cfg.OpConfig.EnableEBSGp3Migration {
		cluster.VolumeResizer = &volumes.EBSVolumeResizer{AWSRegion: cfg.OpConfig.AWSRegion}

	}
	newDBCount.Inc()
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
		return fmt.Errorf("could not init human users: %v", err)
	}

	return nil
}

// Create creates the new kubernetes objects associated with the cluster.
func (c *Cluster) Create() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var (
		err error

		service *v1.Service
		ep      *v1.Endpoints
		ss      *appsv1.StatefulSet
	)

	defer func() {
		if err == nil {
			c.KubeClient.SetPostgresCRDStatus(c.clusterName(), acidv1.ClusterStatusRunning) //TODO: are you sure it's running?
		} else {
			c.KubeClient.SetPostgresCRDStatus(c.clusterName(), acidv1.ClusterStatusAddFailed)
		}
	}()

	c.KubeClient.SetPostgresCRDStatus(c.clusterName(), acidv1.ClusterStatusCreating)
	c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Create", "Started creation of new cluster resources")

	if err = c.enforceMinResourceLimits(&c.Spec); err != nil {
		return fmt.Errorf("could not enforce minimum resource limits: %v", err)
	}

	for _, role := range []PostgresRole{Master, Replica} {

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

	if c.PodDisruptionBudget != nil {
		return fmt.Errorf("pod disruption budget already exists in the cluster")
	}
	pdb, err := c.createPodDisruptionBudget()
	if err != nil {
		return fmt.Errorf("could not create pod disruption budget: %v", err)
	}
	c.logger.Infof("pod disruption budget %q has been successfully created", util.NameFromMeta(pdb.ObjectMeta))

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

	// create database objects unless we are running without pods or disabled
	// that feature explicitly
	if !(c.databaseAccessDisabled() || c.getNumberOfInstances(&c.Spec) <= 0 || c.Spec.StandbyCluster != nil) {
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

	if err := c.listResources(); err != nil {
		c.logger.Errorf("could not list resources: %v", err)
	}

	// Create connection pooler deployment and services if necessary. Since we
	// need to perform some operations with the database itself (e.g. install
	// lookup function), do it as the last step, when everything is available.
	//
	// Do not consider connection pooler as a strict requirement, and if
	// something fails, report warning
	c.createConnectionPooler(c.installLookupFunction)

	return nil
}

func (c *Cluster) compareStatefulSetWith(statefulSet *appsv1.StatefulSet) *compareStatefulsetResult {
	reasons := make([]string, 0)
	var match, needsRollUpdate, needsReplace bool

	match = true
	//TODO: improve me
	if *c.Statefulset.Spec.Replicas != *statefulSet.Spec.Replicas {
		match = false
		reasons = append(reasons, "new statefulset's number of replicas does not match the current one")
	}
	if !reflect.DeepEqual(c.Statefulset.Annotations, statefulSet.Annotations) {
		match = false
		needsReplace = true
		reasons = append(reasons, "new statefulset's annotations do not match the current one")
	}

	needsRollUpdate, reasons = c.compareContainers("initContainers", c.Statefulset.Spec.Template.Spec.InitContainers, statefulSet.Spec.Template.Spec.InitContainers, needsRollUpdate, reasons)
	needsRollUpdate, reasons = c.compareContainers("containers", c.Statefulset.Spec.Template.Spec.Containers, statefulSet.Spec.Template.Spec.Containers, needsRollUpdate, reasons)

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

	if !reflect.DeepEqual(c.Statefulset.Spec.Template.Annotations, statefulSet.Spec.Template.Annotations) {
		match = false
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod template metadata annotations does not match the current one")
	}
	if !reflect.DeepEqual(c.Statefulset.Spec.Template.Spec.SecurityContext, statefulSet.Spec.Template.Spec.SecurityContext) {
		match = false
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod template security context in spec does not match the current one")
	}
	if len(c.Statefulset.Spec.VolumeClaimTemplates) != len(statefulSet.Spec.VolumeClaimTemplates) {
		needsReplace = true
		reasons = append(reasons, "new statefulset's volumeClaimTemplates contains different number of volumes to the old one")
	}
	for i := 0; i < len(c.Statefulset.Spec.VolumeClaimTemplates); i++ {
		name := c.Statefulset.Spec.VolumeClaimTemplates[i].Name
		// Some generated fields like creationTimestamp make it not possible to use DeepCompare on ObjectMeta
		if name != statefulSet.Spec.VolumeClaimTemplates[i].Name {
			needsReplace = true
			reasons = append(reasons, fmt.Sprintf("new statefulset's name for volume %d does not match the current one", i))
			continue
		}
		if !reflect.DeepEqual(c.Statefulset.Spec.VolumeClaimTemplates[i].Annotations, statefulSet.Spec.VolumeClaimTemplates[i].Annotations) {
			needsReplace = true
			reasons = append(reasons, fmt.Sprintf("new statefulset's annotations for volume %q does not match the current one", name))
		}
		if !reflect.DeepEqual(c.Statefulset.Spec.VolumeClaimTemplates[i].Spec, statefulSet.Spec.VolumeClaimTemplates[i].Spec) {
			name := c.Statefulset.Spec.VolumeClaimTemplates[i].Name
			needsReplace = true
			reasons = append(reasons, fmt.Sprintf("new statefulset's volumeClaimTemplates specification for volume %q does not match the current one", name))
		}
	}

	if len(c.Statefulset.Spec.Template.Spec.Volumes) != len(statefulSet.Spec.Template.Spec.Volumes) {
		needsReplace = true
		reasons = append(reasons, "new statefulset's volumes contains different number of volumes to the old one")
	}

	// we assume any change in priority happens by rolling out a new priority class
	// changing the priority value in an existing class is not supproted
	if c.Statefulset.Spec.Template.Spec.PriorityClassName != statefulSet.Spec.Template.Spec.PriorityClassName {
		match = false
		needsReplace = true
		needsRollUpdate = true
		reasons = append(reasons, "new statefulset's pod priority class in spec does not match the current one")
	}

	// lazy Spilo update: modify the image in the statefulset itself but let its pods run with the old image
	// until they are re-created for other reasons, for example node rotation
	effectivePodImage := getPostgresContainer(&c.Statefulset.Spec.Template.Spec).Image
	desiredImage := getPostgresContainer(&statefulSet.Spec.Template.Spec).Image
	if c.OpConfig.EnableLazySpiloUpgrade && !reflect.DeepEqual(effectivePodImage, desiredImage) {
		needsReplace = true
		reasons = append(reasons, "lazy Spilo update: new statefulset's pod image does not match the current one")
	}

	if needsRollUpdate || needsReplace {
		match = false
	}

	return &compareStatefulsetResult{match: match, reasons: reasons, rollingUpdate: needsRollUpdate, replace: needsReplace}
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
		return true, append(reasons, fmt.Sprintf("new statefulset %s's length does not match the current ones", description))
	}

	checks := []containerCheck{
		newCheck("new statefulset %s's %s (index %d) name does not match the current one",
			func(a, b v1.Container) bool { return a.Name != b.Name }),
		newCheck("new statefulset %s's %s (index %d) ports do not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.Ports, b.Ports) }),
		newCheck("new statefulset %s's %s (index %d) resources do not match the current ones",
			func(a, b v1.Container) bool { return !compareResources(&a.Resources, &b.Resources) }),
		newCheck("new statefulset %s's %s (index %d) environment does not match the current one",
			func(a, b v1.Container) bool { return !compareEnv(a.Env, b.Env) }),
		newCheck("new statefulset %s's %s (index %d) environment sources do not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.EnvFrom, b.EnvFrom) }),
		newCheck("new statefulset %s's %s (index %d) security context does not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.SecurityContext, b.SecurityContext) }),
		newCheck("new statefulset %s's %s (index %d) volume mounts do not match the current one",
			func(a, b v1.Container) bool { return !reflect.DeepEqual(a.VolumeMounts, b.VolumeMounts) }),
	}

	if !c.OpConfig.EnableLazySpiloUpgrade {
		checks = append(checks, newCheck("new statefulset %s's %s (index %d) image does not match the current one",
			func(a, b v1.Container) bool { return a.Image != b.Image }))
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
	equal := true
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
	var (
		oa, ob spiloConfiguration
	)

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

func (c *Cluster) enforceMinResourceLimits(spec *acidv1.PostgresSpec) error {

	var (
		isSmaller bool
		err       error
	)

	// setting limits too low can cause unnecessary evictions / OOM kills
	minCPULimit := c.OpConfig.MinCPULimit
	minMemoryLimit := c.OpConfig.MinMemoryLimit

	cpuLimit := spec.Resources.ResourceLimits.CPU
	if cpuLimit != "" {
		isSmaller, err = util.IsSmallerQuantity(cpuLimit, minCPULimit)
		if err != nil {
			return fmt.Errorf("could not compare defined CPU limit %s with configured minimum value %s: %v", cpuLimit, minCPULimit, err)
		}
		if isSmaller {
			c.logger.Warningf("defined CPU limit %s is below required minimum %s and will be increased", cpuLimit, minCPULimit)
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "ResourceLimits", "defined CPU limit %s is below required minimum %s and will be set to it", cpuLimit, minCPULimit)
			spec.Resources.ResourceLimits.CPU = minCPULimit
		}
	}

	memoryLimit := spec.Resources.ResourceLimits.Memory
	if memoryLimit != "" {
		isSmaller, err = util.IsSmallerQuantity(memoryLimit, minMemoryLimit)
		if err != nil {
			return fmt.Errorf("could not compare defined memory limit %s with configured minimum value %s: %v", memoryLimit, minMemoryLimit, err)
		}
		if isSmaller {
			c.logger.Warningf("defined memory limit %s is below required minimum %s and will be increased", memoryLimit, minMemoryLimit)
			c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeWarning, "ResourceLimits", "defined memory limit %s is below required minimum %s and will be set to it", memoryLimit, minMemoryLimit)
			spec.Resources.ResourceLimits.Memory = minMemoryLimit
		}
	}

	return nil
}

// Update changes Kubernetes objects according to the new specification. Unlike the sync case, the missing object
// (i.e. service) is treated as an error
// logical backup cron jobs are an exception: a user-initiated Update can enable a logical backup job
// for a cluster that had no such job before. In this case a missing job is not an error.
func (c *Cluster) Update(oldSpec, newSpec *acidv1.Postgresql) error {
	updateFailed := false
	syncStatefulSet := false

	c.mu.Lock()
	defer c.mu.Unlock()

	c.KubeClient.SetPostgresCRDStatus(c.clusterName(), acidv1.ClusterStatusUpdating)
	c.setSpec(newSpec)

	defer func() {
		if updateFailed {
			c.KubeClient.SetPostgresCRDStatus(c.clusterName(), acidv1.ClusterStatusUpdateFailed)
		} else {
			c.KubeClient.SetPostgresCRDStatus(c.clusterName(), acidv1.ClusterStatusRunning)
		}
	}()

	logNiceDiff(c.logger, oldSpec, newSpec)

	if IsBiggerPostgresVersion(oldSpec.Spec.PostgresqlParam.PgVersion, c.GetDesiredMajorVersion()) {
		c.logger.Infof("postgresql version increased (%s -> %s), depending on config manual upgrade needed",
			oldSpec.Spec.PostgresqlParam.PgVersion, newSpec.Spec.PostgresqlParam.PgVersion)
		syncStatefulSet = true
	} else {
		c.logger.Infof("postgresql major version unchanged or smaller, no changes needed")
		// sticking with old version, this will also advance GetDesiredVersion next time.
		newSpec.Spec.PostgresqlParam.PgVersion = oldSpec.Spec.PostgresqlParam.PgVersion
	}

	// Service
	if !reflect.DeepEqual(c.generateService(Master, &oldSpec.Spec), c.generateService(Master, &newSpec.Spec)) ||
		!reflect.DeepEqual(c.generateService(Replica, &oldSpec.Spec), c.generateService(Replica, &newSpec.Spec)) {
		if err := c.syncServices(); err != nil {
			c.logger.Errorf("could not sync services: %v", err)
			updateFailed = true
		}
	}

	// connection pooler needs one system user created, which is done in
	// initUsers. Check if it needs to be called.
	sameUsers := reflect.DeepEqual(oldSpec.Spec.Users, newSpec.Spec.Users) &&
		reflect.DeepEqual(oldSpec.Spec.PreparedDatabases, newSpec.Spec.PreparedDatabases)
	needConnectionPooler := needMasterConnectionPoolerWorker(&newSpec.Spec) ||
		needReplicaConnectionPoolerWorker(&newSpec.Spec)
	if !sameUsers || needConnectionPooler {
		c.logger.Debugf("initialize users")
		if err := c.initUsers(); err != nil {
			c.logger.Errorf("could not init users: %v", err)
			updateFailed = true
		}

		c.logger.Debugf("syncing secrets")

		//TODO: mind the secrets of the deleted/new users
		if err := c.syncSecrets(); err != nil {
			c.logger.Errorf("could not sync secrets: %v", err)
			updateFailed = true
		}
	}

	// Volume
	if c.OpConfig.StorageResizeMode != "off" {
		c.syncVolumes()
	} else {
		c.logger.Infof("Storage resize is disabled (storage_resize_mode is off). Skipping volume sync.")
	}

	// Statefulset
	func() {
		if err := c.enforceMinResourceLimits(&c.Spec); err != nil {
			c.logger.Errorf("could not sync resources: %v", err)
			updateFailed = true
			return
		}

		oldSs, err := c.generateStatefulSet(&oldSpec.Spec)
		if err != nil {
			c.logger.Errorf("could not generate old statefulset spec: %v", err)
			updateFailed = true
			return
		}

		// update newSpec to for latter comparison with oldSpec
		c.enforceMinResourceLimits(&newSpec.Spec)

		newSs, err := c.generateStatefulSet(&newSpec.Spec)
		if err != nil {
			c.logger.Errorf("could not generate new statefulset spec: %v", err)
			updateFailed = true
			return
		}
		if syncStatefulSet || !reflect.DeepEqual(oldSs, newSs) || !reflect.DeepEqual(oldSpec.Annotations, newSpec.Annotations) {
			c.logger.Debugf("syncing statefulsets")
			syncStatefulSet = false
			// TODO: avoid generating the StatefulSet object twice by passing it to syncStatefulSet
			if err := c.syncStatefulSet(); err != nil {
				c.logger.Errorf("could not sync statefulsets: %v", err)
				updateFailed = true
			}
		}
	}()

	// pod disruption budget
	if oldSpec.Spec.NumberOfInstances != newSpec.Spec.NumberOfInstances {
		c.logger.Debug("syncing pod disruption budgets")
		if err := c.syncPodDisruptionBudget(true); err != nil {
			c.logger.Errorf("could not sync pod disruption budget: %v", err)
			updateFailed = true
		}
	}

	// logical backup job
	func() {

		// create if it did not exist
		if !oldSpec.Spec.EnableLogicalBackup && newSpec.Spec.EnableLogicalBackup {
			c.logger.Debugf("creating backup cron job")
			if err := c.createLogicalBackupJob(); err != nil {
				c.logger.Errorf("could not create a k8s cron job for logical backups: %v", err)
				updateFailed = true
				return
			}
		}

		// delete if no longer needed
		if oldSpec.Spec.EnableLogicalBackup && !newSpec.Spec.EnableLogicalBackup {
			c.logger.Debugf("deleting backup cron job")
			if err := c.deleteLogicalBackupJob(); err != nil {
				c.logger.Errorf("could not delete a k8s cron job for logical backups: %v", err)
				updateFailed = true
				return
			}

		}

		// apply schedule changes
		// this is the only parameter of logical backups a user can overwrite in the cluster manifest
		if (oldSpec.Spec.EnableLogicalBackup && newSpec.Spec.EnableLogicalBackup) &&
			(newSpec.Spec.LogicalBackupSchedule != oldSpec.Spec.LogicalBackupSchedule) {
			c.logger.Debugf("updating schedule of the backup cron job")
			if err := c.syncLogicalBackupJob(); err != nil {
				c.logger.Errorf("could not sync logical backup jobs: %v", err)
				updateFailed = true
			}
		}

	}()

	// Roles and Databases
	if !(c.databaseAccessDisabled() || c.getNumberOfInstances(&c.Spec) <= 0 || c.Spec.StandbyCluster != nil) {
		c.logger.Debugf("syncing roles")
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
func (c *Cluster) Delete() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Delete", "Started deletion of new cluster resources")

	// delete the backup job before the stateful set of the cluster to prevent connections to non-existing pods
	// deleting the cron job also removes pods and batch jobs it created
	if err := c.deleteLogicalBackupJob(); err != nil {
		c.logger.Warningf("could not remove the logical backup k8s cron job; %v", err)
	}

	if err := c.deleteStatefulSet(); err != nil {
		c.logger.Warningf("could not delete statefulset: %v", err)
	}

	if err := c.deleteSecrets(); err != nil {
		c.logger.Warningf("could not delete secrets: %v", err)
	}

	if err := c.deletePodDisruptionBudget(); err != nil {
		c.logger.Warningf("could not delete pod disruption budget: %v", err)
	}

	for _, role := range []PostgresRole{Master, Replica} {

		if !c.patroniKubernetesUseConfigMaps() {
			if err := c.deleteEndpoint(role); err != nil {
				c.logger.Warningf("could not delete %s endpoint: %v", role, err)
			}
		}

		if err := c.deleteService(role); err != nil {
			c.logger.Warningf("could not delete %s service: %v", role, err)
		}
	}

	if err := c.deletePatroniClusterObjects(); err != nil {
		c.logger.Warningf("could not remove leftover patroni objects; %v", err)
	}

	// Delete connection pooler objects anyway, even if it's not mentioned in the
	// manifest, just to not keep orphaned components in case if something went
	// wrong
	for _, role := range [2]PostgresRole{Master, Replica} {
		if err := c.deleteConnectionPooler(role); err != nil {
			c.logger.Warningf("could not remove connection pooler: %v", err)
		}
	}

}

//NeedsRepair returns true if the cluster should be included in the repair scan (based on its in-memory status).
func (c *Cluster) NeedsRepair() (bool, acidv1.PostgresStatus) {
	c.specMu.RLock()
	defer c.specMu.RUnlock()
	return !c.Status.Success(), c.Status

}

// ReceivePodEvent is called back by the controller in order to add the cluster's pod event to the queue.
func (c *Cluster) ReceivePodEvent(event PodEvent) {
	if err := c.podEventsQueue.Add(event); err != nil {
		c.logger.Errorf("error when receiving pod events: %v", err)
	}
}

func (c *Cluster) processPodEvent(obj interface{}) error {
	event, ok := obj.(PodEvent)
	if !ok {
		return fmt.Errorf("could not cast to PodEvent")
	}

	c.podSubscribersMu.RLock()
	subscriber, ok := c.podSubscribers[spec.NamespacedName(event.PodName)]
	c.podSubscribersMu.RUnlock()
	if ok {
		subscriber <- event
	}

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
			if _, err := c.podEventsQueue.Pop(cache.PopProcessFunc(c.processPodEvent)); err != nil {
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
		Password:  util.RandomPassword(constants.PasswordLength),
	}

	// Connection pooler user is an exception, if requested it's going to be
	// created by operator as a normal pgUser
	if needConnectionPooler(&c.Spec) {
		// initialize empty connection pooler if not done yet
		if c.Spec.ConnectionPooler == nil {
			c.Spec.ConnectionPooler = &acidv1.ConnectionPooler{}
		}

		// Using superuser as pooler user is not a good idea. First of all it's
		// not going to be synced correctly with the current implementation,
		// and second it's a bad practice.
		username := c.OpConfig.ConnectionPooler.User

		isSuperUser := c.Spec.ConnectionPooler.User == c.OpConfig.SuperUsername
		isProtectedUser := c.shouldAvoidProtectedOrSystemRole(
			c.Spec.ConnectionPooler.User, "connection pool role")

		if !isSuperUser && !isProtectedUser {
			username = util.Coalesce(
				c.Spec.ConnectionPooler.User,
				c.OpConfig.ConnectionPooler.User)
		}

		// connection pooler application should be able to login with this role
		connectionPoolerUser := spec.PgUser{
			Origin:    spec.RoleConnectionPooler,
			Name:      username,
			Namespace: c.Namespace,
			Flags:     []string{constants.RoleFlagLogin},
			Password:  util.RandomPassword(constants.PasswordLength),
		}

		if _, exists := c.pgUsers[username]; !exists {
			c.pgUsers[username] = connectionPoolerUser
		}

		if _, exists := c.systemUsers[constants.ConnectionPoolerUserKeyName]; !exists {
			c.systemUsers[constants.ConnectionPoolerUserKeyName] = connectionPoolerUser
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
		constants.OwnerRoleNameSuffix + constants.UserRoleNameSuffix:  constants.OwnerRoleNameSuffix,
		constants.ReaderRoleNameSuffix + constants.UserRoleNameSuffix: constants.ReaderRoleNameSuffix,
		constants.WriterRoleNameSuffix + constants.UserRoleNameSuffix: constants.WriterRoleNameSuffix,
	}

	for preparedDbName, preparedDB := range c.Spec.PreparedDatabases {
		// get list of prepared schemas to set in search_path
		preparedSchemas := preparedDB.PreparedSchemas
		if len(preparedDB.PreparedSchemas) == 0 {
			preparedSchemas = map[string]acidv1.PreparedSchema{"data": {DefaultRoles: util.True()}}
		}

		var searchPath strings.Builder
		searchPath.WriteString(constants.DefaultSearchPath)
		for preparedSchemaName := range preparedSchemas {
			searchPath.WriteString(", " + preparedSchemaName)
		}

		// default roles per database
		if err := c.initDefaultRoles(defaultRoles, "admin", preparedDbName, searchPath.String()); err != nil {
			return fmt.Errorf("could not initialize default roles for database %s: %v", preparedDbName, err)
		}
		if preparedDB.DefaultUsers {
			if err := c.initDefaultRoles(defaultUsers, "admin", preparedDbName, searchPath.String()); err != nil {
				return fmt.Errorf("could not initialize default roles for database %s: %v", preparedDbName, err)
			}
		}

		// default roles per database schema
		for preparedSchemaName, preparedSchema := range preparedSchemas {
			if preparedSchema.DefaultRoles == nil || *preparedSchema.DefaultRoles {
				if err := c.initDefaultRoles(defaultRoles,
					preparedDbName+constants.OwnerRoleNameSuffix,
					preparedDbName+"_"+preparedSchemaName,
					constants.DefaultSearchPath+", "+preparedSchemaName); err != nil {
					return fmt.Errorf("could not initialize default roles for database schema %s: %v", preparedSchemaName, err)
				}
				if preparedSchema.DefaultUsers {
					if err := c.initDefaultRoles(defaultUsers,
						preparedDbName+constants.OwnerRoleNameSuffix,
						preparedDbName+"_"+preparedSchemaName,
						constants.DefaultSearchPath+", "+preparedSchemaName); err != nil {
						return fmt.Errorf("could not initialize default users for database schema %s: %v", preparedSchemaName, err)
					}
				}
			}
		}
	}
	return nil
}

func (c *Cluster) initDefaultRoles(defaultRoles map[string]string, admin, prefix string, searchPath string) error {

	for defaultRole, inherits := range defaultRoles {

		roleName := prefix + defaultRole

		flags := []string{constants.RoleFlagNoLogin}
		if defaultRole[len(defaultRole)-5:] == constants.UserRoleNameSuffix {
			flags = []string{constants.RoleFlagLogin}
		}

		memberOf := make([]string, 0)
		if inherits != "" {
			memberOf = append(memberOf, prefix+inherits)
		}

		adminRole := ""
		if strings.Contains(defaultRole, constants.OwnerRoleNameSuffix) {
			adminRole = admin
		} else {
			adminRole = prefix + constants.OwnerRoleNameSuffix
		}

		newRole := spec.PgUser{
			Origin:     spec.RoleOriginBootstrap,
			Name:       roleName,
			Namespace:  c.Namespace,
			Password:   util.RandomPassword(constants.PasswordLength),
			Flags:      flags,
			MemberOf:   memberOf,
			Parameters: map[string]string{"search_path": searchPath},
			AdminRole:  adminRole,
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

		//if namespaced secrets are allowed
		if c.Postgresql.Spec.EnableNamespacedSecret != nil &&
			*c.Postgresql.Spec.EnableNamespacedSecret {
			if strings.Contains(username, ".") {
				splits := strings.Split(username, ".")
				namespace = splits[0]
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
		}
		if currentRole, present := c.pgUsers[username]; present {
			c.pgUsers[username] = c.resolveNameConflict(&currentRole, &newRole)
		} else {
			c.pgUsers[username] = newRole
		}
	}
	return nil
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
	return &ClusterStatus{
		Cluster: c.Spec.ClusterName,
		Team:    c.Spec.TeamID,
		Status:  c.Status,
		Spec:    c.Spec,

		MasterService:       c.GetServiceMaster(),
		ReplicaService:      c.GetServiceReplica(),
		MasterEndpoint:      c.GetEndpointMaster(),
		ReplicaEndpoint:     c.GetEndpointReplica(),
		StatefulSet:         c.GetStatefulSet(),
		PodDisruptionBudget: c.GetPodDisruptionBudget(),
		CurrentProcess:      c.GetCurrentProcess(),

		Error: fmt.Errorf("error: %s", c.Error),
	}
}

// Switchover does a switchover (via Patroni) to a candidate pod
func (c *Cluster) Switchover(curMaster *v1.Pod, candidate spec.NamespacedName) error {

	var err error
	c.logger.Debugf("switching over from %q to %q", curMaster.Name, candidate)
	c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Switchover", "Switching over from %q to %q", curMaster.Name, candidate)

	var wg sync.WaitGroup

	podLabelErr := make(chan error)
	stopCh := make(chan struct{})

	wg.Add(1)

	go func() {
		defer wg.Done()
		ch := c.registerPodSubscriber(candidate)
		defer c.unregisterPodSubscriber(candidate)

		role := Master

		select {
		case <-stopCh:
		case podLabelErr <- func() (err2 error) {
			_, err2 = c.waitForPodLabel(ch, stopCh, &role)
			return
		}():
		}
	}()

	if err = c.patroni.Switchover(curMaster, candidate.Name); err == nil {
		c.logger.Debugf("successfully switched over from %q to %q", curMaster.Name, candidate)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Switchover", "Successfully switched over from %q to %q", curMaster.Name, candidate)
		if err = <-podLabelErr; err != nil {
			err = fmt.Errorf("could not get master pod label: %v", err)
		}
	} else {
		err = fmt.Errorf("could not switch over from %q to %q: %v", curMaster.Name, candidate, err)
		c.eventRecorder.Eventf(c.GetReference(), v1.EventTypeNormal, "Switchover", "Switchover from %q to %q FAILED: %v", curMaster.Name, candidate, err)
	}

	// signal the role label waiting goroutine to close the shop and go home
	close(stopCh)
	// wait until the goroutine terminates, since unregisterPodSubscriber
	// must be called before the outer return; otherwise we risk subscribing to the same pod twice.
	wg.Wait()
	// close the label waiting channel no sooner than the waiting goroutine terminates.
	close(podLabelErr)

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

type simpleActionWithResult func() error

type clusterObjectGet func(name string) (spec.NamespacedName, error)

type clusterObjectDelete func(name string) error

func (c *Cluster) deletePatroniClusterObjects() error {
	// TODO: figure out how to remove leftover patroni objects in other cases
	var actionsList []simpleActionWithResult

	if !c.patroniUsesKubernetes() {
		c.logger.Infof("not cleaning up Etcd Patroni objects on cluster delete")
	}

	if !c.patroniKubernetesUseConfigMaps() {
		actionsList = append(actionsList, c.deletePatroniClusterEndpoints)
	}
	actionsList = append(actionsList, c.deletePatroniClusterServices, c.deletePatroniClusterConfigMaps)

	c.logger.Debugf("removing leftover Patroni objects (endpoints / services and configmaps)")
	for _, deleter := range actionsList {
		if err := deleter(); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) deleteClusterObject(
	get clusterObjectGet,
	del clusterObjectDelete,
	objType string) error {
	for _, suffix := range patroniObjectSuffixes {
		name := fmt.Sprintf("%s-%s", c.Name, suffix)

		if namespacedName, err := get(name); err == nil {
			c.logger.Debugf("deleting Patroni cluster object %q with name %q",
				objType, namespacedName)

			if err = del(name); err != nil {
				return fmt.Errorf("could not delete Patroni cluster object %q with name %q: %v",
					objType, namespacedName, err)
			}

		} else if !k8sutil.ResourceNotFound(err) {
			return fmt.Errorf("could not fetch Patroni Endpoint %q: %v",
				namespacedName, err)
		}
	}
	return nil
}

func (c *Cluster) deletePatroniClusterServices() error {
	get := func(name string) (spec.NamespacedName, error) {
		svc, err := c.KubeClient.Services(c.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
		return util.NameFromMeta(svc.ObjectMeta), err
	}

	deleteServiceFn := func(name string) error {
		return c.KubeClient.Services(c.Namespace).Delete(context.TODO(), name, c.deleteOptions)
	}

	return c.deleteClusterObject(get, deleteServiceFn, "service")
}

func (c *Cluster) deletePatroniClusterEndpoints() error {
	get := func(name string) (spec.NamespacedName, error) {
		ep, err := c.KubeClient.Endpoints(c.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
		return util.NameFromMeta(ep.ObjectMeta), err
	}

	deleteEndpointFn := func(name string) error {
		return c.KubeClient.Endpoints(c.Namespace).Delete(context.TODO(), name, c.deleteOptions)
	}

	return c.deleteClusterObject(get, deleteEndpointFn, "endpoint")
}

func (c *Cluster) deletePatroniClusterConfigMaps() error {
	get := func(name string) (spec.NamespacedName, error) {
		cm, err := c.KubeClient.ConfigMaps(c.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
		return util.NameFromMeta(cm.ObjectMeta), err
	}

	deleteConfigMapFn := func(name string) error {
		return c.KubeClient.ConfigMaps(c.Namespace).Delete(context.TODO(), name, c.deleteOptions)
	}

	return c.deleteClusterObject(get, deleteConfigMapFn, "configmap")
}
