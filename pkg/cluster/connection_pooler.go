package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/r3labs/diff"
	"github.com/sirupsen/logrus"
	acidzalando "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/retryutil"
)

var poolerRunAsUser = int64(100)
var poolerRunAsGroup = int64(101)

// ConnectionPoolerObjects K8s objects that are belong to connection pooler
type ConnectionPoolerObjects struct {
	AuthSecret  *v1.Secret
	Deployment  *appsv1.Deployment
	Service     *v1.Service
	Name        string
	ClusterName string
	Namespace   string
	Role        PostgresRole
	// It could happen that a connection pooler was enabled, but the operator
	// was not able to properly process a corresponding event or was restarted.
	// In this case we will miss missing/require situation and a lookup function
	// will not be installed. To avoid synchronizing it all the time to prevent
	// this, we can remember the result in memory at least until the next
	// restart.
	LookupFunction bool
	// Careful with referencing cluster.spec this object pointer changes
	// during runtime and lifetime of cluster
}

func (c *Cluster) connectionPoolerName(role PostgresRole) string {
	name := fmt.Sprintf("%s-%s", c.Name, constants.ConnectionPoolerResourceSuffix)
	if role == Replica {
		name = fmt.Sprintf("%s-%s", name, "repl")
	}
	return name
}

// isConnectionPoolerEnabled
func needConnectionPooler(spec *acidv1.PostgresSpec) bool {
	return needMasterConnectionPoolerWorker(spec) ||
		needReplicaConnectionPoolerWorker(spec)
}

func needMasterConnectionPooler(spec *acidv1.PostgresSpec) bool {
	return needMasterConnectionPoolerWorker(spec)
}

func needMasterConnectionPoolerWorker(spec *acidv1.PostgresSpec) bool {
	return (spec.EnableConnectionPooler != nil && *spec.EnableConnectionPooler) ||
		(spec.ConnectionPooler != nil && spec.EnableConnectionPooler == nil)
}

func needReplicaConnectionPooler(spec *acidv1.PostgresSpec) bool {
	return needReplicaConnectionPoolerWorker(spec)
}

func needReplicaConnectionPoolerWorker(spec *acidv1.PostgresSpec) bool {
	return spec.EnableReplicaConnectionPooler != nil &&
		*spec.EnableReplicaConnectionPooler
}

func (c *Cluster) needConnectionPoolerUser(oldSpec, newSpec *acidv1.PostgresSpec) bool {
	// return true if pooler is needed AND was not disabled before OR user name differs
	return (needMasterConnectionPoolerWorker(newSpec) || needReplicaConnectionPoolerWorker(newSpec)) &&
		((!needMasterConnectionPoolerWorker(oldSpec) &&
			!needReplicaConnectionPoolerWorker(oldSpec)) ||
			c.poolerUser(oldSpec) != c.poolerUser(newSpec))
}

func (c *Cluster) poolerUser(spec *acidv1.PostgresSpec) string {
	connectionPoolerSpec := spec.ConnectionPooler
	if connectionPoolerSpec == nil {
		connectionPoolerSpec = &acidv1.ConnectionPooler{}
	}
	// Using superuser as pooler user is not a good idea. First of all it's
	// not going to be synced correctly with the current implementation,
	// and second it's a bad practice.
	username := c.OpConfig.ConnectionPooler.User

	isSuperUser := connectionPoolerSpec.User == c.OpConfig.SuperUsername
	isProtectedUser := c.shouldAvoidProtectedOrSystemRole(
		connectionPoolerSpec.User, "connection pool role")

	if !isSuperUser && !isProtectedUser {
		username = util.Coalesce(
			connectionPoolerSpec.User,
			c.OpConfig.ConnectionPooler.User)
	}

	return username
}

// when listing pooler k8s objects
func (c *Cluster) poolerLabelsSet(addExtraLabels bool) labels.Set {
	poolerLabels := c.labelsSet(addExtraLabels)

	// TODO should be config values
	poolerLabels["application"] = "db-connection-pooler"

	return poolerLabels
}

// Return connection pooler labels selector, which should from one point of view
// inherit most of the labels from the cluster itself, but at the same time
// have e.g. different `application` label, so that recreatePod operation will
// not interfere with it (it lists all the pods via labels, and if there would
// be no difference, it will recreate also pooler pods).
func (c *Cluster) connectionPoolerLabels(role PostgresRole, addExtraLabels bool) *metav1.LabelSelector {
	poolerLabelsSet := c.poolerLabelsSet(addExtraLabels)

	// TODO should be config values
	poolerLabelsSet["connection-pooler"] = c.connectionPoolerName(role)

	if addExtraLabels {
		extraLabels := map[string]string{}
		extraLabels[c.OpConfig.PodRoleLabel] = string(role)

		poolerLabelsSet = labels.Merge(poolerLabelsSet, extraLabels)
	}

	return &metav1.LabelSelector{
		MatchLabels:      poolerLabelsSet,
		MatchExpressions: nil,
	}
}

// Prepare the database for connection pooler to be used, i.e. install lookup
// function (do it first, because it should be fast and if it didn't succeed,
// it doesn't makes sense to create more K8S objects. At this moment we assume
// that necessary connection pooler user exists.
//
// After that create all the objects for connection pooler, namely a deployment
// with a chosen pooler and a service to expose it.

// have connectionpooler name in the cp object to have it immutable name
// add these cp related functions to a new cp file
// opConfig, cluster, and database name
func (c *Cluster) createConnectionPooler(LookupFunction InstallFunction) (SyncReason, error) {
	var reason SyncReason
	c.setProcessName("creating connection pooler")

	//this is essentially sync with nil as oldSpec
	if reason, err := c.syncConnectionPooler(&acidv1.Postgresql{}, &c.Postgresql, LookupFunction); err != nil {
		return reason, err
	}
	return reason, nil
}

func (c *Cluster) generateUserlist() string {
	var sb strings.Builder

	poolerAdminUser := c.systemUsers[constants.ConnectionPoolerUserKeyName]
	fmt.Fprintf(&sb, "\"%s\" \"%s\"\n", poolerAdminUser.Name, poolerAdminUser.Password)

	for roleName, infraRole := range c.InfrastructureRoles {
		if infraRole.Password != "" {
			fmt.Fprintf(&sb, "\"%s\" \"%s\"\n", roleName, infraRole.Password)
		}
	}

	return sb.String()
}

func (c *Cluster) generateConnectionPoolerAuthSecret(connectionPooler *ConnectionPoolerObjects) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          c.connectionPoolerLabels(connectionPooler.Role, true).MatchLabels,
			Name:            fmt.Sprintf("%s-u", connectionPooler.Name),
			Namespace:       connectionPooler.Namespace,
			Annotations:     c.annotationsSet(nil),
			OwnerReferences: c.ownerReferences(),
		},
		Type: v1.SecretTypeOpaque,
		// Secret data must be bytes. Kubernetes handles the encoding.
		StringData: map[string]string{
			"userlist.txt": c.generateUserlist(),
		},
	}
}

// Generate pool size related environment variables.
//
// MAX_DB_CONN would specify the global maximum for connections to a target
//
//	database.
//
// MAX_CLIENT_CONN is not configurable at the moment, just set it high enough.
//
// DEFAULT_SIZE is a pool size per db/user (having in mind the use case when
//
//	most of the queries coming through a connection pooler are from the same
//	user to the same db). In case if we want to spin up more connection pooler
//	instances, take this into account and maintain the same number of
//	connections.
//
// MIN_SIZE is a pool's minimal size, to prevent situation when sudden workload
//
//	have to wait for spinning up a new connections.
//
// RESERVE_SIZE is how many additional connections to allow for a pooler.

func (c *Cluster) getConnectionPoolerEnvVars() []v1.EnvVar {
	spec := &c.Spec
	connectionPoolerSpec := spec.ConnectionPooler
	if connectionPoolerSpec == nil {
		connectionPoolerSpec = &acidv1.ConnectionPooler{}
	}
	effectiveMode := util.Coalesce(
		connectionPoolerSpec.Mode,
		c.OpConfig.ConnectionPooler.Mode)

	numberOfInstances := connectionPoolerSpec.NumberOfInstances
	if numberOfInstances == nil {
		numberOfInstances = util.CoalesceInt32(
			c.OpConfig.ConnectionPooler.NumberOfInstances,
			k8sutil.Int32ToPointer(1))
	}

	effectiveMaxDBConn := util.CoalesceInt32(
		connectionPoolerSpec.MaxDBConnections,
		c.OpConfig.ConnectionPooler.MaxDBConnections)

	if effectiveMaxDBConn == nil {
		effectiveMaxDBConn = k8sutil.Int32ToPointer(
			constants.ConnectionPoolerMaxDBConnections)
	}

	maxDBConn := *effectiveMaxDBConn / *numberOfInstances

	defaultSize := maxDBConn / 2
	minSize := defaultSize / 2
	reserveSize := minSize

	passwordEncryption := passwordEncryptionFromSpec(spec)

	return []v1.EnvVar{
		{
			Name:  "CONNECTION_POOLER_PORT",
			Value: fmt.Sprint(pgPort),
		},
		{
			Name:  "CONNECTION_POOLER_MODE",
			Value: effectiveMode,
		},
		{
			Name:  "CONNECTION_POOLER_AUTH_TYPE",
			Value: string(passwordEncryption),
		},
		{
			Name:  "CONNECTION_POOLER_DEFAULT_SIZE",
			Value: fmt.Sprint(defaultSize),
		},
		{
			Name:  "CONNECTION_POOLER_MIN_SIZE",
			Value: fmt.Sprint(minSize),
		},
		{
			Name:  "CONNECTION_POOLER_RESERVE_SIZE",
			Value: fmt.Sprint(reserveSize),
		},
		{
			Name:  "CONNECTION_POOLER_MAX_CLIENT_CONN",
			Value: fmt.Sprint(constants.ConnectionPoolerMaxClientConnections),
		},
		{
			Name:  "CONNECTION_POOLER_MAX_DB_CONN",
			Value: fmt.Sprint(maxDBConn),
		},
	}
}

func (c *Cluster) generateConnectionPoolerPodTemplate(role PostgresRole) (
	*v1.PodTemplateSpec, error) {
	spec := &c.Spec
	connectionPoolerSpec := spec.ConnectionPooler
	if connectionPoolerSpec == nil {
		connectionPoolerSpec = &acidv1.ConnectionPooler{}
	}
	gracePeriod := int64(util.CoalesceDuration(c.OpConfig.PodTerminateGracePeriod, "5m").Seconds())
	resources, err := c.generateResourceRequirements(
		connectionPoolerSpec.Resources,
		makeDefaultConnectionPoolerResources(&c.OpConfig),
		connectionPoolerContainer)

	if err != nil {
		return nil, fmt.Errorf("could not generate resource requirements: %v", err)
	}

	effectiveDockerImage := util.Coalesce(
		connectionPoolerSpec.DockerImage,
		c.OpConfig.ConnectionPooler.Image)

	effectiveSchema := util.Coalesce(
		connectionPoolerSpec.Schema,
		c.OpConfig.ConnectionPooler.Schema)

	secretSelector := func(key string) *v1.SecretKeySelector {
		effectiveUser := util.Coalesce(
			connectionPoolerSpec.User,
			c.OpConfig.ConnectionPooler.User)

		return &v1.SecretKeySelector{
			LocalObjectReference: v1.LocalObjectReference{
				Name: c.credentialSecretName(effectiveUser),
			},
			Key: key,
		}
	}

	envVars := []v1.EnvVar{
		{
			Name:  "PGHOST",
			Value: c.serviceAddress(role),
		},
		{
			Name:  "PGPORT",
			Value: fmt.Sprint(c.servicePort(role)),
		},
		{
			Name: "PGUSER",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: secretSelector("username"),
			},
		},
		// the convention is to use the same schema name as
		// connection pooler username
		{
			Name:  "PGSCHEMA",
			Value: effectiveSchema,
		},
		{
			Name: "PGPASSWORD",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: secretSelector("password"),
			},
		},
	}
	envVars = append(envVars, c.getConnectionPoolerEnvVars()...)

	infraRolesList := make([]string, 0)
	for infraRoleName := range c.InfrastructureRoles {
		infraRolesList = append(infraRolesList, infraRoleName)
	}

	if len(infraRolesList) > 0 {
		envVars = append(envVars, v1.EnvVar{
			Name:  "INFRASTRUCTURE_ROLES",
			Value: strings.Join(infraRolesList, ","),
		})
	}

	poolerContainer := v1.Container{
		Name:            connectionPoolerContainer,
		Image:           effectiveDockerImage,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *resources,
		Ports: []v1.ContainerPort{
			{
				ContainerPort: pgPort,
				Protocol:      v1.ProtocolTCP,
			},
		},
		ReadinessProbe: &v1.Probe{
			ProbeHandler: v1.ProbeHandler{
				TCPSocket: &v1.TCPSocketAction{
					Port: intstr.IntOrString{IntVal: pgPort},
				},
			},
		},
		SecurityContext: &v1.SecurityContext{
			AllowPrivilegeEscalation: util.False(),
		},
	}

	var poolerVolumes []v1.Volume
	var volumeMounts []v1.VolumeMount

	// mount secret volume with userlist.txt for pgBouncer to authenticate users
	poolerVolumes = append(poolerVolumes, v1.Volume{
		Name: fmt.Sprintf("%s-u", c.connectionPoolerName(role)),
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName: fmt.Sprintf("%s-u", c.connectionPoolerName(role)),
			},
		},
	})
	volumeMounts = append(volumeMounts, v1.VolumeMount{
		Name:      fmt.Sprintf("%s-u", c.connectionPoolerName(role)),
		MountPath: "/etc/pgbouncer/userlist.txt",
		SubPath:   "userlist.txt",
		ReadOnly:  true,
	})

	// If the cluster has custom TLS certificates configured, we do the following:
	//  1. Add environment variables to tell pgBouncer where to find the TLS certificates
	//  2. Reference the secret in a volume
	//  3. Mount the volume to the container at /tls
	if spec.TLS != nil && spec.TLS.SecretName != "" {
		getPoolerTLSEnv := func(k string) string {
			keyName := ""
			switch k {
			case "tls.crt":
				keyName = "CONNECTION_POOLER_CLIENT_TLS_CRT"
			case "tls.key":
				keyName = "CONNECTION_POOLER_CLIENT_TLS_KEY"
			case "tls.ca":
				keyName = "CONNECTION_POOLER_CLIENT_CA_FILE"
			default:
				panic(fmt.Sprintf("TLS env key for pooler unknown %s", k))
			}

			return keyName
		}
		tlsEnv, tlsVolumes := generateTlsMounts(spec, getPoolerTLSEnv)
		envVars = append(envVars, tlsEnv...)
		for _, vol := range tlsVolumes {
			poolerVolumes = append(poolerVolumes, v1.Volume{
				Name:         vol.Name,
				VolumeSource: vol.VolumeSource,
			})
			volumeMounts = append(volumeMounts, v1.VolumeMount{
				Name:      vol.Name,
				MountPath: vol.MountPath,
			})
		}
	}

	poolerContainer.Env = envVars
	poolerContainer.VolumeMounts = volumeMounts
	tolerationsSpec := tolerations(&spec.Tolerations, c.OpConfig.PodToleration)
	securityContext := v1.PodSecurityContext{}

	// determine the User, Group and FSGroup for the pooler pod
	securityContext.RunAsUser = &poolerRunAsUser
	securityContext.RunAsGroup = &poolerRunAsGroup

	effectiveFSGroup := c.OpConfig.Resources.SpiloFSGroup
	if spec.SpiloFSGroup != nil {
		effectiveFSGroup = spec.SpiloFSGroup
	}
	if effectiveFSGroup != nil {
		securityContext.FSGroup = effectiveFSGroup
	}

	podTemplate := &v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      c.connectionPoolerLabels(role, true).MatchLabels,
			Namespace:   c.Namespace,
			Annotations: c.annotationsSet(c.generatePodAnnotations(spec)),
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers:                    []v1.Container{poolerContainer},
			Tolerations:                   tolerationsSpec,
			Volumes:                       poolerVolumes,
			SecurityContext:               &securityContext,
			ServiceAccountName:            c.OpConfig.PodServiceAccountName,
		},
	}

	nodeAffinity := c.nodeAffinity(c.OpConfig.NodeReadinessLabel, spec.NodeAffinity)
	if c.OpConfig.EnablePodAntiAffinity {
		labelsSet := labels.Set(c.connectionPoolerLabels(role, false).MatchLabels)
		podTemplate.Spec.Affinity = podAffinity(
			labelsSet,
			c.OpConfig.PodAntiAffinityTopologyKey,
			nodeAffinity,
			c.OpConfig.PodAntiAffinityPreferredDuringScheduling,
			true,
		)
	} else if nodeAffinity != nil {
		podTemplate.Spec.Affinity = nodeAffinity
	}

	return podTemplate, nil
}

func (c *Cluster) generateConnectionPoolerDeployment(connectionPooler *ConnectionPoolerObjects) (
	*appsv1.Deployment, error) {
	spec := &c.Spec

	// there are two ways to enable connection pooler, either to specify a
	// connectionPooler section or enableConnectionPooler. In the second case
	// spec.connectionPooler will be nil, so to make it easier to calculate
	// default values, initialize it to an empty structure. It could be done
	// anywhere, but here is the earliest common entry point between sync and
	// create code, so init here.
	connectionPoolerSpec := spec.ConnectionPooler
	if connectionPoolerSpec == nil {
		connectionPoolerSpec = &acidv1.ConnectionPooler{}
	}
	podTemplate, err := c.generateConnectionPoolerPodTemplate(connectionPooler.Role)

	numberOfInstances := connectionPoolerSpec.NumberOfInstances
	if numberOfInstances == nil {
		numberOfInstances = util.CoalesceInt32(
			c.OpConfig.ConnectionPooler.NumberOfInstances,
			k8sutil.Int32ToPointer(1))
	}

	if *numberOfInstances < constants.ConnectionPoolerMinInstances {
		msg := "adjusted number of connection pooler instances from %d to %d"
		c.logger.Warningf(msg, *numberOfInstances, constants.ConnectionPoolerMinInstances)

		*numberOfInstances = constants.ConnectionPoolerMinInstances
	}

	if err != nil {
		return nil, err
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        connectionPooler.Name,
			Namespace:   connectionPooler.Namespace,
			Labels:      c.connectionPoolerLabels(connectionPooler.Role, true).MatchLabels,
			Annotations: c.AnnotationsToPropagate(c.annotationsSet(nil)),
			// make StatefulSet object its owner to represent the dependency.
			// By itself StatefulSet is being deleted with "Orphaned"
			// propagation policy, which means that it's deletion will not
			// clean up this deployment, but there is a hope that this object
			// will be garbage collected if something went wrong and operator
			// didn't deleted it.
			OwnerReferences: c.ownerReferences(),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: numberOfInstances,
			Selector: c.connectionPoolerLabels(connectionPooler.Role, false),
			Template: *podTemplate,
		},
	}

	return deployment, nil
}

func (c *Cluster) generateConnectionPoolerService(connectionPooler *ConnectionPoolerObjects) *v1.Service {
	spec := &c.Spec
	poolerRole := connectionPooler.Role
	serviceSpec := v1.ServiceSpec{
		Ports: []v1.ServicePort{
			{
				Name:       connectionPooler.Name,
				Port:       pgPort,
				TargetPort: intstr.IntOrString{IntVal: c.servicePort(poolerRole)},
			},
		},
		Type: v1.ServiceTypeClusterIP,
		Selector: map[string]string{
			"connection-pooler": c.connectionPoolerName(poolerRole),
		},
	}

	if ok, port := c.shouldCreateNodePortForPoolerService(poolerRole, spec); ok {
		c.configureNodePortService(&serviceSpec, port)
	} else if c.shouldCreateLoadBalancerForPoolerService(poolerRole, spec) {
		c.configureLoadBalanceService(&serviceSpec, spec.AllowedSourceRanges)
	}

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        connectionPooler.Name,
			Namespace:   connectionPooler.Namespace,
			Labels:      c.connectionPoolerLabels(connectionPooler.Role, false).MatchLabels,
			Annotations: c.annotationsSet(c.generatePoolerServiceAnnotations(poolerRole, spec)),
			// make StatefulSet object its owner to represent the dependency.
			// By itself StatefulSet is being deleted with "Orphaned"
			// propagation policy, which means that it's deletion will not
			// clean up this service, but there is a hope that this object will
			// be garbage collected if something went wrong and operator didn't
			// deleted it.
			OwnerReferences: c.ownerReferences(),
		},
		Spec: serviceSpec,
	}

	return service
}

func (c *Cluster) generatePoolerServiceAnnotations(role PostgresRole, spec *acidv1.PostgresSpec) map[string]string {
	var dnsString string
	annotations := c.getCustomServiceAnnotations(role, spec)

	nodePort, _ := c.shouldCreateNodePortForPoolerService(role, spec)

	if !nodePort && c.shouldCreateLoadBalancerForPoolerService(role, spec) {
		// -repl suffix will be added by replicaDNSName
		clusterNameWithPoolerSuffix := c.connectionPoolerName(Master)
		if role == Master {
			dnsString = c.masterDNSName(clusterNameWithPoolerSuffix)
		} else {
			dnsString = c.replicaDNSName(clusterNameWithPoolerSuffix)
		}
		annotations[constants.ZalandoDNSNameAnnotation] = dnsString
	}

	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

func (c *Cluster) shouldCreateLoadBalancerForPoolerService(role PostgresRole, spec *acidv1.PostgresSpec) bool {

	switch role {

	case Replica:
		// if the value is explicitly set in a Postgresql manifest, follow this setting
		if spec.EnableReplicaPoolerLoadBalancer != nil {
			return *spec.EnableReplicaPoolerLoadBalancer
		}
		// otherwise, follow the operator configuration
		return c.OpConfig.EnableReplicaPoolerLoadBalancer

	case Master:
		if spec.EnableMasterPoolerLoadBalancer != nil {
			return *spec.EnableMasterPoolerLoadBalancer
		}
		return c.OpConfig.EnableMasterPoolerLoadBalancer

	default:
		panic(fmt.Sprintf("Unknown role %v", role))
	}
}

func (c *Cluster) shouldCreateNodePortForPoolerService(role PostgresRole, spec *acidv1.PostgresSpec) (bool, int32) {
	switch role {
	case Replica:
		// if the value is explicitly set in a Postgresql manifest, follow this setting
		if spec.EnableReplicaPoolerNodePort != nil {
			port := int32(0)
			if spec.ReplicaPoolerNodePort != nil {
				port = *spec.ReplicaPoolerNodePort
			}

			return *spec.EnableReplicaPoolerNodePort, port
		}

		// otherwise, follow the operator configuration
		return c.OpConfig.EnableReplicaPoolerNodePort, 0
	case Master:
		if spec.EnableMasterPoolerNodePort != nil {
			port := int32(0)
			if spec.MasterPoolerNodePort != nil {
				port = *spec.MasterPoolerNodePort
			}

			return *spec.EnableMasterPoolerNodePort, port
		}

		return c.OpConfig.EnableMasterPoolerNodePort, 0
	default:
		panic(fmt.Sprintf("Unknown role %v", role))
	}
}

func (c *Cluster) listPoolerPods(listOptions metav1.ListOptions) ([]v1.Pod, error) {
	pods, err := c.KubeClient.Pods(c.Namespace).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("could not get list of pooler pods: %v", err)
	}
	return pods.Items, nil
}

// delete connection pooler
func (c *Cluster) deleteConnectionPooler(role PostgresRole) (err error) {
	c.logger.Infof("deleting connection pooler spilo-role=%s", role)

	// Lack of connection pooler objects is not a fatal error, just log it if
	// it was present before in the manifest
	if c.ConnectionPooler[role] == nil || role == "" {
		c.logger.Debug("no connection pooler to delete")
		return nil
	}

	// Try to delete every remembered object even if one of the deletions
	// fails, and forget only the objects that are really gone. Otherwise a
	// failure on e.g. the service would leave the auth secret behind and,
	// once the in-memory references are lost, nothing would retry it.
	var errs []string

	// set delete propagation policy to foreground, so that replica set will be
	// also deleted.
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{PropagationPolicy: &policy}

	if deployment := c.ConnectionPooler[role].Deployment; deployment != nil {
		err = c.KubeClient.
			Deployments(c.Namespace).
			Delete(context.TODO(), deployment.Name, options)

		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("connection pooler deployment %s for role %s has already been deleted", deployment.Name, role)
			c.ConnectionPooler[role].Deployment = nil
		} else if err != nil {
			errs = append(errs, fmt.Sprintf("could not delete connection pooler deployment: %v", err))
		} else {
			c.logger.Infof("connection pooler deployment %s has been deleted for role %s", deployment.Name, role)
			c.ConnectionPooler[role].Deployment = nil
		}
	} else {
		c.logger.Debug("no connection pooler deployment object to delete")
	}

	// Repeat the same for the service object
	if service := c.ConnectionPooler[role].Service; service != nil {
		err = c.KubeClient.
			Services(c.Namespace).
			Delete(context.TODO(), service.Name, options)

		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("connection pooler service %s for role %s has already been deleted", service.Name, role)
			c.ConnectionPooler[role].Service = nil
		} else if err != nil {
			errs = append(errs, fmt.Sprintf("could not delete connection pooler service: %v", err))
		} else {
			c.logger.Infof("connection pooler service %s has been deleted for role %s", service.Name, role)
			c.ConnectionPooler[role].Service = nil
		}
	} else {
		c.logger.Debug("no connection pooler service object to delete")
	}

	// Repeat the same for the auth secret
	if authSecret := c.ConnectionPooler[role].AuthSecret; authSecret != nil {
		err = c.KubeClient.
			Secrets(c.Namespace).
			Delete(context.TODO(), authSecret.Name, metav1.DeleteOptions{})

		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("connection pooler auth secret %s for role %s has already been deleted", authSecret.Name, role)
			c.ConnectionPooler[role].AuthSecret = nil
		} else if err != nil {
			errs = append(errs, fmt.Sprintf("could not delete connection pooler auth secret: %v", err))
		} else {
			c.logger.Infof("connection pooler auth secret %s has been deleted for role %s", authSecret.Name, role)
			c.ConnectionPooler[role].AuthSecret = nil
		}
	} else {
		c.logger.Debug("no connection pooler auth secret to delete")
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// Look up connection pooler objects of the given role by their names and
// remember the ones that exist. This is needed when the in-memory references
// are lost (e.g. after an operator restart) while the pooler is disabled, so
// that leftovers of a previously enabled pooler can still be cleaned up.
// Only objects carrying the connection-pooler label set by the operator are
// taken into account.
func (c *Cluster) discoverConnectionPoolerObjects(role PostgresRole) error {
	name := c.connectionPoolerName(role)
	authSecretName := fmt.Sprintf("%s-u", name)
	isPoolerObject := func(objectLabels map[string]string) bool {
		return objectLabels["connection-pooler"] == name
	}

	deployment, err := c.KubeClient.Deployments(c.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err == nil {
		if isPoolerObject(deployment.Labels) {
			c.ConnectionPooler[role].Deployment = deployment
		}
	} else if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get connection pooler deployment %s: %v", name, err)
	}

	service, err := c.KubeClient.Services(c.Namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err == nil {
		if isPoolerObject(service.Labels) {
			c.ConnectionPooler[role].Service = service
		}
	} else if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get connection pooler service %s: %v", name, err)
	}

	authSecret, err := c.KubeClient.Secrets(c.Namespace).Get(context.TODO(), authSecretName, metav1.GetOptions{})
	if err == nil {
		if isPoolerObject(authSecret.Labels) {
			c.ConnectionPooler[role].AuthSecret = authSecret
		}
	} else if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get connection pooler auth secret %s: %v", authSecretName, err)
	}

	return nil
}

// delete connection pooler secret
func (c *Cluster) deleteConnectionPoolerSecret() (err error) {
	// Repeat the same for the secret object
	secretName := c.credentialSecretName(c.OpConfig.ConnectionPooler.User)

	secret, err := c.KubeClient.
		Secrets(c.Namespace).
		Get(context.TODO(), secretName, metav1.GetOptions{})

	if err != nil {
		c.logger.Debugf("could not get connection pooler secret %s: %v", secretName, err)
	} else {
		if err = c.deleteSecret(secret.UID); err != nil {
			return fmt.Errorf("could not delete pooler secret: %v", err)
		}
	}

	return nil
}

// Perform actual patching of a connection pooler deployment, assuming that all
// the check were already done before.
func updateConnectionPoolerDeployment(KubeClient k8sutil.KubernetesClient, newDeployment *appsv1.Deployment, doUpdate bool) (*appsv1.Deployment, error) {
	if newDeployment == nil {
		return nil, fmt.Errorf("there is no connection pooler in the cluster")
	}

	if doUpdate {
		updatedDeployment, err := KubeClient.Deployments(newDeployment.Namespace).Update(context.TODO(), newDeployment, metav1.UpdateOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not update pooler deployment to match desired state: %v", err)
		}
		return updatedDeployment, nil
	}

	patchData, err := specPatch(newDeployment.Spec)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the connection pooler deployment: %v", err)
	}

	// An update probably requires RetryOnConflict, but since only one operator
	// worker at one time will try to update it chances of conflicts are
	// minimal.
	deployment, err := KubeClient.
		Deployments(newDeployment.Namespace).Patch(
		context.TODO(),
		newDeployment.Name,
		types.MergePatchType,
		patchData,
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch connection pooler deployment: %v", err)
	}

	return deployment, nil
}

// patchConnectionPoolerAnnotations updates the annotations of connection pooler deployment
func patchConnectionPoolerAnnotations(KubeClient k8sutil.KubernetesClient, deployment *appsv1.Deployment, annotations map[string]string) (*appsv1.Deployment, error) {
	patchData, err := metaAnnotationsPatch(annotations)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the connection pooler deployment metadata: %v", err)
	}
	result, err := KubeClient.Deployments(deployment.Namespace).Patch(
		context.TODO(),
		deployment.Name,
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch connection pooler annotations %q: %v", patchData, err)
	}
	return result, nil

}

// Test if two connection pooler configuration needs to be synced. For simplicity
// compare not the actual K8S objects, but the configuration itself and request
// sync if there is any difference.
func needSyncConnectionPoolerSpecs(oldSpec, newSpec *acidv1.ConnectionPooler, logger *logrus.Entry) (sync bool, reasons []string) {
	reasons = []string{}
	sync = false

	changelog, err := diff.Diff(oldSpec, newSpec)
	if err != nil {
		logger.Infof("cannot get diff, do not do anything, %+v", err)
		return false, reasons
	}

	if len(changelog) > 0 {
		sync = true
	}

	for _, change := range changelog {
		msg := fmt.Sprintf("%s %+v from '%+v' to '%+v'",
			change.Type, change.Path, change.From, change.To)
		reasons = append(reasons, msg)
	}

	return sync, reasons
}

// Check if we need to synchronize connection pooler deployment due to new
// defaults, that are different from what we see in the DeploymentSpec
func (c *Cluster) needSyncConnectionPoolerDefaults(Config *Config, spec *acidv1.ConnectionPooler, deployment *appsv1.Deployment) (sync bool, reasons []string) {

	reasons = []string{}
	sync = false

	config := Config.OpConfig.ConnectionPooler
	podTemplate := deployment.Spec.Template
	poolerContainer := podTemplate.Spec.Containers[constants.ConnectionPoolerContainer]

	if spec == nil {
		spec = &acidv1.ConnectionPooler{}
	}

	if spec.NumberOfInstances == nil &&
		*deployment.Spec.Replicas != *config.NumberOfInstances {

		sync = true
		msg := fmt.Sprintf("numberOfInstances is different (having %d, required %d)",
			*deployment.Spec.Replicas, *config.NumberOfInstances)
		reasons = append(reasons, msg)
	}

	if spec.DockerImage == "" &&
		poolerContainer.Image != config.Image {

		sync = true
		msg := fmt.Sprintf("dockerImage is different (having %s, required %s)",
			poolerContainer.Image, config.Image)
		reasons = append(reasons, msg)
	}

	expectedResources, err := c.generateResourceRequirements(spec.Resources,
		makeDefaultConnectionPoolerResources(&Config.OpConfig),
		connectionPoolerContainer)

	// An error to generate expected resources means something is not quite
	// right, but for the purpose of robustness do not panic here, just report
	// and ignore resources comparison (in the worst case there will be no
	// updates for new resource values).
	if err == nil && syncResources(&poolerContainer.Resources, expectedResources) {
		sync = true
		msg := fmt.Sprintf("resources are different (having %+v, required %+v)",
			poolerContainer.Resources, expectedResources)
		reasons = append(reasons, msg)
	}

	if err != nil {
		return false, reasons
	}

	authTypeFound := false
	passwordEncryption := passwordEncryptionFromSpec(&c.Spec)
	for _, env := range poolerContainer.Env {
		if spec.User == "" && env.Name == "PGUSER" {
			ref := env.ValueFrom.SecretKeyRef.LocalObjectReference
			secretName := Config.OpConfig.SecretNameTemplate.Format(
				"username", strings.Replace(config.User, "_", "-", -1),
				"cluster", c.Name,
				"tprkind", acidv1.PostgresCRDResourceKind,
				"tprgroup", acidzalando.GroupName)

			if ref.Name != secretName {
				sync = true
				msg := fmt.Sprintf("pooler user and secret are different (having %s, required %s)",
					ref.Name, secretName)
				reasons = append(reasons, msg)
			}
		}

		if spec.Schema == "" && env.Name == "PGSCHEMA" && env.Value != config.Schema {
			sync = true
			msg := fmt.Sprintf("pooler schema is different (having %s, required %s)",
				env.Value, config.Schema)
			reasons = append(reasons, msg)
		}

		if env.Name == "CONNECTION_POOLER_AUTH_TYPE" {
			authTypeFound = true
			if string(passwordEncryption) != env.Value {
				sync = true
				msg := fmt.Sprintf("pooler auth type is different (having %s, required %s)",
					env.Value, passwordEncryption)
				reasons = append(reasons, msg)
			}
		}
	}

	// env var is missing on deployments created before it was introduced
	if !authTypeFound {
		sync = true
		reasons = append(reasons, "pooler auth type env variable is missing")
	}

	return sync, reasons
}

// Generate default resource section for connection pooler deployment, to be
// used if nothing custom is specified in the manifest
func makeDefaultConnectionPoolerResources(config *config.Config) acidv1.Resources {

	defaultRequests := acidv1.ResourceDescription{
		CPU:    &config.ConnectionPooler.ConnectionPoolerDefaultCPURequest,
		Memory: &config.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest,
	}
	defaultLimits := acidv1.ResourceDescription{
		CPU:    &config.ConnectionPooler.ConnectionPoolerDefaultCPULimit,
		Memory: &config.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit,
	}

	return acidv1.Resources{
		ResourceRequests: defaultRequests,
		ResourceLimits:   defaultLimits,
	}
}

func logPoolerEssentials(log *logrus.Entry, oldSpec, newSpec *acidv1.Postgresql) {
	var v []string
	var input []*bool

	newMasterConnectionPoolerEnabled := needMasterConnectionPoolerWorker(&newSpec.Spec)
	if oldSpec == nil {
		input = []*bool{nil, nil, &newMasterConnectionPoolerEnabled, newSpec.Spec.EnableReplicaConnectionPooler}
	} else {
		oldMasterConnectionPoolerEnabled := needMasterConnectionPoolerWorker(&oldSpec.Spec)
		input = []*bool{&oldMasterConnectionPoolerEnabled, oldSpec.Spec.EnableReplicaConnectionPooler, &newMasterConnectionPoolerEnabled, newSpec.Spec.EnableReplicaConnectionPooler}
	}

	for _, b := range input {
		if b == nil {
			v = append(v, "nil")
		} else {
			v = append(v, fmt.Sprintf("%v", *b))
		}
	}

	log.Debugf("syncing connection pooler (master, replica) from (%v, %v) to (%v, %v)", v[0], v[1], v[2], v[3])
}

func (c *Cluster) syncConnectionPooler(oldSpec, newSpec *acidv1.Postgresql, LookupFunction InstallFunction) (SyncReason, error) {

	var reason SyncReason
	var err error
	var connectionPoolerNeeded bool

	logPoolerEssentials(c.logger, oldSpec, newSpec)

	// Check and perform the sync requirements for each of the roles.
	for _, role := range [2]PostgresRole{Master, Replica} {

		if role == Master {
			connectionPoolerNeeded = needMasterConnectionPoolerWorker(&newSpec.Spec)
		} else {
			connectionPoolerNeeded = needReplicaConnectionPoolerWorker(&newSpec.Spec)
		}

		// if the call is via createConnectionPooler, then it is required to initialize
		// the structure
		if c.ConnectionPooler == nil {
			c.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{}
		}
		if c.ConnectionPooler[role] == nil {
			c.ConnectionPooler[role] = &ConnectionPoolerObjects{
				Deployment:     nil,
				Service:        nil,
				Name:           c.connectionPoolerName(role),
				ClusterName:    c.Name,
				Namespace:      c.Namespace,
				LookupFunction: false,
				Role:           role,
			}

			// Nothing is remembered yet (first sync of this cluster, e.g.
			// after an operator restart). If the pooler is not needed, look
			// for objects left over from a previously enabled pooler, so that
			// they get removed below.
			if !connectionPoolerNeeded {
				if err = c.discoverConnectionPoolerObjects(role); err != nil {
					c.logger.Warningf("could not look up connection pooler objects: %v", err)
				}
			}
		}

		if connectionPoolerNeeded {
			// Try to sync in any case. If we didn't needed connection pooler before,
			// it means we want to create it. If it was already present, still sync
			// since it could happen that there is no difference in specs, and all
			// the resources are remembered, but the deployment was manually deleted
			// in between

			// in this case also do not forget to install lookup function
			// skip installation in standby clusters, since they are read-only
			if !c.ConnectionPooler[role].LookupFunction && !isStandbyCluster(&newSpec.Spec) {
				connectionPooler := c.Spec.ConnectionPooler
				specSchema := ""
				specUser := ""

				if connectionPooler != nil {
					specSchema = connectionPooler.Schema
					specUser = connectionPooler.User
				}

				schema := util.Coalesce(
					specSchema,
					c.OpConfig.ConnectionPooler.Schema)

				user := util.Coalesce(
					specUser,
					c.OpConfig.ConnectionPooler.User)

				if err = LookupFunction(schema, user); err != nil {
					return NoSync, err
				}
				c.ConnectionPooler[role].LookupFunction = true
			}

			if reason, err = c.syncConnectionPoolerWorker(oldSpec, newSpec, role); err != nil {
				c.logger.Errorf("could not sync connection pooler: %v", err)
				return reason, err
			}
		} else {
			// delete and cleanup resources if they are already detected
			if c.ConnectionPooler[role] != nil &&
				(c.ConnectionPooler[role].Deployment != nil ||
					c.ConnectionPooler[role].Service != nil ||
					c.ConnectionPooler[role].AuthSecret != nil) {

				if err = c.deleteConnectionPooler(role); err != nil {
					c.logger.Warningf("could not remove connection pooler: %v", err)
				}
			}
		}
	}
	if (needMasterConnectionPoolerWorker(&oldSpec.Spec) || needReplicaConnectionPoolerWorker(&oldSpec.Spec)) &&
		!needMasterConnectionPoolerWorker(&newSpec.Spec) && !needReplicaConnectionPoolerWorker(&newSpec.Spec) {
		if err = c.deleteConnectionPoolerSecret(); err != nil {
			c.logger.Warningf("could not remove connection pooler secret: %v", err)
		}
	}

	return reason, nil
}

// Synchronize connection pooler resources. Effectively we're interested only in
// synchronizing the corresponding deployment, but in case of deployment or
// service is missing, create it. After checking, also remember an object for
// the future references.
func (c *Cluster) syncConnectionPoolerWorker(oldSpec, newSpec *acidv1.Postgresql, role PostgresRole) (
	SyncReason, error) {

	var (
		deployment    *appsv1.Deployment
		newDeployment *appsv1.Deployment
		pods          []v1.Pod
		service       *v1.Service
		newService    *v1.Service
		authSecret    *v1.Secret
		newAuthSecret *v1.Secret
		err           error
	)

	updatedPodAnnotations := map[string]*string{}
	syncReason := make([]string, 0)

	// create extra secret for connection pooler authentication
	newAuthSecret = c.generateConnectionPoolerAuthSecret(c.ConnectionPooler[role])
	if authSecret, err = c.KubeClient.Secrets(c.Namespace).Get(context.TODO(), fmt.Sprintf("%s-u", c.connectionPoolerName(role)), metav1.GetOptions{}); err == nil {
		c.ConnectionPooler[role].AuthSecret = authSecret
		// make sure existing annotations are preserved
		newAuthSecret.Annotations = c.annotationsSet(authSecret.Annotations)
		authSecret, err = c.KubeClient.Secrets(authSecret.Namespace).Update(context.TODO(), newAuthSecret, metav1.UpdateOptions{})
		if err != nil {
			return NoSync, fmt.Errorf("could not update connection pooler auth secret: %v", err)
		}
		c.ConnectionPooler[role].AuthSecret = authSecret
	} else if !k8sutil.ResourceNotFound(err) {
		return NoSync, fmt.Errorf("could not get auth secret for connection pooler to sync: %v", err)
	}

	if k8sutil.ResourceNotFound(err) {
		c.logger.Warningf("auth secret %s for connection pooler is not found, create it", fmt.Sprintf("%s-u", c.connectionPoolerName(role)))
		authSecret, err = c.KubeClient.
			Secrets(newAuthSecret.Namespace).
			Create(context.TODO(), newAuthSecret, metav1.CreateOptions{})

		if err != nil {
			return NoSync, err
		}
		c.ConnectionPooler[role].AuthSecret = authSecret
	}

	// next the pooler deployment
	deployment, err = c.KubeClient.
		Deployments(c.Namespace).
		Get(context.TODO(), c.connectionPoolerName(role), metav1.GetOptions{})

	if err != nil && k8sutil.ResourceNotFound(err) {
		c.logger.Warningf("deployment %s for connection pooler synchronization is not found, create it", c.connectionPoolerName(role))

		newDeployment, err = c.generateConnectionPoolerDeployment(c.ConnectionPooler[role])
		if err != nil {
			return NoSync, fmt.Errorf("could not generate deployment for connection pooler: %v", err)
		}

		deployment, err = c.KubeClient.
			Deployments(newDeployment.Namespace).
			Create(context.TODO(), newDeployment, metav1.CreateOptions{})

		if err != nil {
			return NoSync, err
		}
		c.ConnectionPooler[role].Deployment = deployment
	} else if err != nil {
		return NoSync, fmt.Errorf("could not get connection pooler deployment to sync: %v", err)
	} else {
		c.ConnectionPooler[role].Deployment = deployment
		// actual synchronization

		var oldConnectionPooler *acidv1.ConnectionPooler

		if oldSpec != nil {
			oldConnectionPooler = oldSpec.Spec.ConnectionPooler
		}

		newConnectionPooler := newSpec.Spec.ConnectionPooler
		// sync implementation below assumes that both old and new specs are
		// not nil, but it can happen. To avoid any confusion like updating a
		// deployment because the specification changed from nil to an empty
		// struct (that was initialized somewhere before) replace any nil with
		// an empty spec.
		if oldConnectionPooler == nil {
			oldConnectionPooler = &acidv1.ConnectionPooler{}
		}

		if newConnectionPooler == nil {
			newConnectionPooler = &acidv1.ConnectionPooler{}
		}

		var specSync, updateDeployment bool
		var specReason []string

		if !reflect.DeepEqual(deployment.ObjectMeta.OwnerReferences, c.ownerReferences()) {
			c.logger.Info("new connection pooler owner references do not match the current ones")
			updateDeployment = true
		}

		if oldSpec != nil {
			specSync, specReason = needSyncConnectionPoolerSpecs(oldConnectionPooler, newConnectionPooler, c.logger)
			syncReason = append(syncReason, specReason...)
		}

		newPodAnnotations := c.annotationsSet(c.generatePodAnnotations(&c.Spec))
		deletedPodAnnotations := []string{}
		if changed, reason := c.compareAnnotations(deployment.Spec.Template.Annotations, newPodAnnotations, &deletedPodAnnotations); changed {
			specSync = true
			syncReason = append(syncReason, []string{"new connection pooler's pod template annotations do not match the current ones: " + reason}...)

			for _, anno := range deletedPodAnnotations {
				updatedPodAnnotations[anno] = nil
			}
			templateMetadataReq := map[string]map[string]map[string]map[string]map[string]*string{
				"spec": {"template": {"metadata": {"annotations": updatedPodAnnotations}}}}
			patch, err := json.Marshal(templateMetadataReq)
			if err != nil {
				return nil, fmt.Errorf("could not marshal ObjectMeta for %s connection pooler's pod template: %v", role, err)
			}
			deployment, err = c.KubeClient.Deployments(c.Namespace).Patch(context.TODO(),
				deployment.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "")
			if err != nil {
				c.logger.Errorf("failed to patch %s connection pooler's pod template: %v", role, err)
				return nil, err
			}

			deployment.Spec.Template.Annotations = newPodAnnotations
		}

		defaultsSync, defaultsReason := c.needSyncConnectionPoolerDefaults(&c.Config, newConnectionPooler, deployment)
		syncReason = append(syncReason, defaultsReason...)

		if specSync || defaultsSync || updateDeployment {
			c.logger.Infof("update connection pooler deployment %s, reason: %+v",
				c.connectionPoolerName(role), syncReason)
			newDeployment, err = c.generateConnectionPoolerDeployment(c.ConnectionPooler[role])
			if err != nil {
				return syncReason, fmt.Errorf("could not generate deployment for connection pooler: %v", err)
			}

			deployment, err = updateConnectionPoolerDeployment(c.KubeClient, newDeployment, updateDeployment)

			if err != nil {
				return syncReason, err
			}
			c.ConnectionPooler[role].Deployment = deployment
		}

		newAnnotations := c.AnnotationsToPropagate(c.annotationsSet(nil)) // including the downscaling annotations
		if changed, _ := c.compareAnnotations(deployment.Annotations, newAnnotations, nil); changed {
			deployment, err = patchConnectionPoolerAnnotations(c.KubeClient, deployment, newAnnotations)
			if err != nil {
				return nil, err
			}
			c.ConnectionPooler[role].Deployment = deployment
		}
	}

	// check if pooler pods must be replaced due to secret update
	listOptions := metav1.ListOptions{
		LabelSelector: labels.Set(c.connectionPoolerLabels(role, true).MatchLabels).String(),
	}
	pods, err = c.listPoolerPods(listOptions)
	if err != nil {
		return nil, err
	}
	for i, pod := range pods {
		if c.getRollingUpdateAnnotationFromPod(&pod) == "true" {
			podName := util.NameFromMeta(pods[i].ObjectMeta)
			err := retryutil.Retry(1*time.Second, 5*time.Second,
				func() (bool, error) {
					err2 := c.KubeClient.Pods(podName.Namespace).Delete(
						context.TODO(),
						podName.Name,
						c.deleteOptions)
					if err2 != nil {
						return false, err2
					}
					return true, nil
				})
			if err != nil {
				return nil, fmt.Errorf("could not delete pooler pod: %v", err)
			}
		} else if changed, _ := c.compareAnnotations(pod.Annotations, deployment.Spec.Template.Annotations, nil); changed {
			metadataReq := map[string]map[string]map[string]*string{"metadata": {}}

			for anno, val := range deployment.Spec.Template.Annotations {
				updatedPodAnnotations[anno] = &val
			}
			metadataReq["metadata"]["annotations"] = updatedPodAnnotations
			patch, err := json.Marshal(metadataReq)
			if err != nil {
				return nil, fmt.Errorf("could not marshal ObjectMeta for %s connection pooler's pods: %v", role, err)
			}
			_, err = c.KubeClient.Pods(pod.Namespace).Patch(context.TODO(), pod.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
			if err != nil {
				return nil, fmt.Errorf("could not patch annotations for %s connection pooler's pod %q: %v", role, pod.Name, err)
			}
		}
	}

	if service, err = c.KubeClient.Services(c.Namespace).Get(context.TODO(), c.connectionPoolerName(role), metav1.GetOptions{}); err == nil {
		c.ConnectionPooler[role].Service = service
		desiredSvc := c.generateConnectionPoolerService(c.ConnectionPooler[role])
		newService, err = c.updateService(role, service, desiredSvc)
		if err != nil {
			return syncReason, fmt.Errorf("could not update %s service to match desired state: %v", role, err)
		}
		c.ConnectionPooler[role].Service = newService
		return NoSync, nil
	}

	if !k8sutil.ResourceNotFound(err) {
		return NoSync, fmt.Errorf("could not get connection pooler service to sync: %v", err)
	}

	c.ConnectionPooler[role].Service = nil
	c.logger.Warningf("service %s for connection pooler synchronization is not found, create it", c.connectionPoolerName(role))

	serviceSpec := c.generateConnectionPoolerService(c.ConnectionPooler[role])
	newService, err = c.KubeClient.
		Services(serviceSpec.Namespace).
		Create(context.TODO(), serviceSpec, metav1.CreateOptions{})

	if err != nil {
		return NoSync, err
	}
	c.ConnectionPooler[role].Service = newService

	return NoSync, nil
}
