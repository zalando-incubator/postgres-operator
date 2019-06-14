package cluster

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"

	"k8s.io/api/apps/v1beta1"
	v1 "k8s.io/api/core/v1"
	policybeta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	pgBinariesLocationTemplate       = "/usr/lib/postgresql/%s/bin"
	patroniPGBinariesParameterName   = "bin_dir"
	patroniPGParametersParameterName = "parameters"
	patroniPGHBAConfParameterName    = "pg_hba"
	localHost                        = "127.0.0.1/32"
)

type pgUser struct {
	Password string   `json:"password"`
	Options  []string `json:"options"`
}

type patroniDCS struct {
	TTL                      uint32                       `json:"ttl,omitempty"`
	LoopWait                 uint32                       `json:"loop_wait,omitempty"`
	RetryTimeout             uint32                       `json:"retry_timeout,omitempty"`
	MaximumLagOnFailover     float32                      `json:"maximum_lag_on_failover,omitempty"`
	PGBootstrapConfiguration map[string]interface{}       `json:"postgresql,omitempty"`
	Slots                    map[string]map[string]string `json:"slots,omitempty"`
	StandbyClusterCfg        *acidv1.PatroniStandbyCluster `json:"standby_cluster,omitempty"`
}

type pgBootstrap struct {
	Initdb []interface{}     `json:"initdb"`
	Users  map[string]pgUser `json:"users"`
	DCS    patroniDCS        `json:"dcs,omitempty"`
}

type spiloConfiguration struct {
	PgLocalConfiguration map[string]interface{} `json:"postgresql"`
	Bootstrap            pgBootstrap            `json:"bootstrap"`
}

func (c *Cluster) containerName() string {
	return "postgres"
}

func (c *Cluster) statefulSetName() string {
	return c.Name
}

func (c *Cluster) endpointName(role PostgresRole) string {
	name := c.Name
	if role == Replica {
		name = name + "-repl"
	}

	return name
}

func (c *Cluster) serviceName(role PostgresRole) string {
	name := c.Name
	if role == Replica {
		name = name + "-repl"
	}

	return name
}

func (c *Cluster) podDisruptionBudgetName() string {
	return c.OpConfig.PDBNameFormat.Format("cluster", c.Name)
}

func (c *Cluster) makeDefaultResources() acidv1.Resources {

	config := c.OpConfig

	defaultRequests := acidv1.ResourceDescription{CPU: config.DefaultCPURequest, Memory: config.DefaultMemoryRequest}
	defaultLimits := acidv1.ResourceDescription{CPU: config.DefaultCPULimit, Memory: config.DefaultMemoryLimit}

	return acidv1.Resources{ResourceRequests: defaultRequests, ResourceLimits: defaultLimits}
}

func generateResourceRequirements(resources acidv1.Resources, defaultResources acidv1.Resources) (*v1.ResourceRequirements, error) {
	var err error

	specRequests := resources.ResourceRequests
	specLimits := resources.ResourceLimits

	result := v1.ResourceRequirements{}

	result.Requests, err = fillResourceList(specRequests, defaultResources.ResourceRequests)
	if err != nil {
		return nil, fmt.Errorf("could not fill resource requests: %v", err)
	}

	result.Limits, err = fillResourceList(specLimits, defaultResources.ResourceLimits)
	if err != nil {
		return nil, fmt.Errorf("could not fill resource limits: %v", err)
	}

	return &result, nil
}

func fillResourceList(spec acidv1.ResourceDescription, defaults acidv1.ResourceDescription) (v1.ResourceList, error) {
	var err error
	requests := v1.ResourceList{}

	if spec.CPU != "" {
		requests[v1.ResourceCPU], err = resource.ParseQuantity(spec.CPU)
		if err != nil {
			return nil, fmt.Errorf("could not parse CPU quantity: %v", err)
		}
	} else {
		requests[v1.ResourceCPU], err = resource.ParseQuantity(defaults.CPU)
		if err != nil {
			return nil, fmt.Errorf("could not parse default CPU quantity: %v", err)
		}
	}
	if spec.Memory != "" {
		requests[v1.ResourceMemory], err = resource.ParseQuantity(spec.Memory)
		if err != nil {
			return nil, fmt.Errorf("could not parse memory quantity: %v", err)
		}
	} else {
		requests[v1.ResourceMemory], err = resource.ParseQuantity(defaults.Memory)
		if err != nil {
			return nil, fmt.Errorf("could not parse default memory quantity: %v", err)
		}
	}

	return requests, nil
}

func generateSpiloJSONConfiguration(pg *acidv1.PostgresqlParam, patroni *acidv1.Patroni, pamRoleName string, logger *logrus.Entry) (string, error) {
	config := spiloConfiguration{}

	config.Bootstrap = pgBootstrap{}

	config.Bootstrap.Initdb = []interface{}{map[string]string{"auth-host": "md5"},
		map[string]string{"auth-local": "trust"}}

	config.Bootstrap.DCS.StandbyClusterCfg = patroni.StandbyCluster

	initdbOptionNames := []string{}

	for k := range patroni.InitDB {
		initdbOptionNames = append(initdbOptionNames, k)
	}
	/* We need to sort the user-defined options to more easily compare the resulting specs */
	sort.Strings(initdbOptionNames)

	// Initdb parameters in the manifest take priority over the default ones
	// The whole type switch dance is caused by the ability to specify both
	// maps and normal string items in the array of initdb options. We need
	// both to convert the initial key-value to strings when necessary, and
	// to de-duplicate the options supplied.
PatroniInitDBParams:
	for _, k := range initdbOptionNames {
		v := patroni.InitDB[k]
		for i, defaultParam := range config.Bootstrap.Initdb {
			switch defaultParam.(type) {
			case map[string]string:
				{
					for k1 := range defaultParam.(map[string]string) {
						if k1 == k {
							(config.Bootstrap.Initdb[i]).(map[string]string)[k] = v
							continue PatroniInitDBParams
						}
					}
				}
			case string:
				{
					/* if the option already occurs in the list */
					if defaultParam.(string) == v {
						continue PatroniInitDBParams
					}
				}
			default:
				logger.Warningf("unsupported type for initdb configuration item %s: %T", defaultParam, defaultParam)
				continue PatroniInitDBParams
			}
		}
		// The following options are known to have no parameters
		if v == "true" {
			switch k {
			case "data-checksums", "debug", "no-locale", "noclean", "nosync", "sync-only":
				config.Bootstrap.Initdb = append(config.Bootstrap.Initdb, k)
				continue
			}
		}
		config.Bootstrap.Initdb = append(config.Bootstrap.Initdb, map[string]string{k: v})
	}

	if patroni.MaximumLagOnFailover >= 0 {
		config.Bootstrap.DCS.MaximumLagOnFailover = patroni.MaximumLagOnFailover
	}
	if patroni.LoopWait != 0 {
		config.Bootstrap.DCS.LoopWait = patroni.LoopWait
	}
	if patroni.RetryTimeout != 0 {
		config.Bootstrap.DCS.RetryTimeout = patroni.RetryTimeout
	}
	if patroni.TTL != 0 {
		config.Bootstrap.DCS.TTL = patroni.TTL
	}
	if patroni.Slots != nil {
		config.Bootstrap.DCS.Slots = patroni.Slots
	}

	config.PgLocalConfiguration = make(map[string]interface{})
	config.PgLocalConfiguration[patroniPGBinariesParameterName] = fmt.Sprintf(pgBinariesLocationTemplate, pg.PgVersion)
	if len(pg.Parameters) > 0 {
		local, bootstrap := getLocalAndBoostrapPostgreSQLParameters(pg.Parameters)

		if len(local) > 0 {
			config.PgLocalConfiguration[patroniPGParametersParameterName] = local
		}
		if len(bootstrap) > 0 {
			config.Bootstrap.DCS.PGBootstrapConfiguration = make(map[string]interface{})
			config.Bootstrap.DCS.PGBootstrapConfiguration[patroniPGParametersParameterName] = bootstrap
		}
	}
	// Patroni gives us a choice of writing pg_hba.conf to either the bootstrap section or to the local postgresql one.
	// We choose the local one, because we need Patroni to change pg_hba.conf in PostgreSQL after the user changes the
	// relevant section in the manifest.
	if len(patroni.PgHba) > 0 {
		config.PgLocalConfiguration[patroniPGHBAConfParameterName] = patroni.PgHba
	}

	config.Bootstrap.Users = map[string]pgUser{
		pamRoleName: {
			Password: "",
			Options:  []string{constants.RoleFlagCreateDB, constants.RoleFlagNoLogin},
		},
	}

	res, err := json.Marshal(config)
	return string(res), err
}

func getLocalAndBoostrapPostgreSQLParameters(parameters map[string]string) (local, bootstrap map[string]string) {
	local = make(map[string]string)
	bootstrap = make(map[string]string)
	for param, val := range parameters {
		if isBootstrapOnlyParameter(param) {
			bootstrap[param] = val
		} else {
			local[param] = val
		}
	}
	return
}

func nodeAffinity(nodeReadinessLabel map[string]string) *v1.Affinity {
	matchExpressions := make([]v1.NodeSelectorRequirement, 0)
	if len(nodeReadinessLabel) == 0 {
		return nil
	}
	for k, v := range nodeReadinessLabel {
		matchExpressions = append(matchExpressions, v1.NodeSelectorRequirement{
			Key:      k,
			Operator: v1.NodeSelectorOpIn,
			Values:   []string{v},
		})
	}

	return &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: matchExpressions}},
			},
		},
	}
}

func generatePodAffinity(labels labels.Set, topologyKey string, nodeAffinity *v1.Affinity) *v1.Affinity {
	// generate pod anti-affinity to avoid multiple pods of the same Postgres cluster in the same topology , e.g. node
	podAffinity := v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: labels,
				},
				TopologyKey: topologyKey,
			}},
		},
	}

	if nodeAffinity != nil && nodeAffinity.NodeAffinity != nil {
		podAffinity.NodeAffinity = nodeAffinity.NodeAffinity
	}

	return &podAffinity
}

func tolerations(tolerationsSpec *[]v1.Toleration, podToleration map[string]string) []v1.Toleration {
	// allow to override tolerations by postgresql manifest
	if len(*tolerationsSpec) > 0 {
		return *tolerationsSpec
	}

	if len(podToleration["key"]) > 0 || len(podToleration["operator"]) > 0 || len(podToleration["value"]) > 0 || len(podToleration["effect"]) > 0 {
		return []v1.Toleration{
			{
				Key:      podToleration["key"],
				Operator: v1.TolerationOperator(podToleration["operator"]),
				Value:    podToleration["value"],
				Effect:   v1.TaintEffect(podToleration["effect"]),
			},
		}
	}

	return []v1.Toleration{}
}

// isBootstrapOnlyParameter checks against special Patroni bootstrap parameters.
// Those parameters must go to the bootstrap/dcs/postgresql/parameters section.
// See http://patroni.readthedocs.io/en/latest/dynamic_configuration.html.
func isBootstrapOnlyParameter(param string) bool {
	return param == "max_connections" ||
		param == "max_locks_per_transaction" ||
		param == "max_worker_processes" ||
		param == "max_prepared_transactions" ||
		param == "wal_level" ||
		param == "wal_log_hints" ||
		param == "track_commit_timestamp"
}

func generateVolumeMounts() []v1.VolumeMount {
	return []v1.VolumeMount{
		{
			Name:      constants.DataVolumeName,
			MountPath: constants.PostgresDataMount, //TODO: fetch from manifest
		},
	}
}

func generateContainer(
	name string,
	dockerImage *string,
	resourceRequirements *v1.ResourceRequirements,
	envVars []v1.EnvVar,
	volumeMounts []v1.VolumeMount,
	privilegedMode bool,
) *v1.Container {
	return &v1.Container{
		Name:            name,
		Image:           *dockerImage,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *resourceRequirements,
		Ports: []v1.ContainerPort{
			{
				ContainerPort: 8008,
				Protocol:      v1.ProtocolTCP,
			},
			{
				ContainerPort: 5432,
				Protocol:      v1.ProtocolTCP,
			},
			{
				ContainerPort: 8080,
				Protocol:      v1.ProtocolTCP,
			},
		},
		VolumeMounts: volumeMounts,
		Env:          envVars,
		SecurityContext: &v1.SecurityContext{
			Privileged: &privilegedMode,
		},
	}
}

func generateSidecarContainers(sidecars []acidv1.Sidecar,
	volumeMounts []v1.VolumeMount, defaultResources acidv1.Resources,
	superUserName string, credentialsSecretName string, logger *logrus.Entry) ([]v1.Container, error) {

	if len(sidecars) > 0 {
		result := make([]v1.Container, 0)
		for index, sidecar := range sidecars {

			resources, err := generateResourceRequirements(
				makeResources(
					sidecar.Resources.ResourceRequests.CPU,
					sidecar.Resources.ResourceRequests.Memory,
					sidecar.Resources.ResourceLimits.CPU,
					sidecar.Resources.ResourceLimits.Memory,
				),
				defaultResources,
			)
			if err != nil {
				return nil, err
			}

			sc := getSidecarContainer(sidecar, index, volumeMounts, resources, superUserName, credentialsSecretName, logger)
			result = append(result, *sc)
		}
		return result, nil
	}
	return nil, nil
}

// Check whether or not we're requested to mount an shm volume,
// taking into account that PostgreSQL manifest has precedence.
func mountShmVolumeNeeded(opConfig config.Config, pgSpec *acidv1.PostgresSpec) bool {
	if pgSpec.ShmVolume != nil {
		return *pgSpec.ShmVolume
	}

	return opConfig.ShmVolume
}

func generatePodTemplate(
	namespace string,
	labels labels.Set,
	spiloContainer *v1.Container,
	initContainers []v1.Container,
	sidecarContainers []v1.Container,
	tolerationsSpec *[]v1.Toleration,
	spiloFSGroup *int64,
	nodeAffinity *v1.Affinity,
	terminateGracePeriod int64,
	podServiceAccountName string,
	kubeIAMRole string,
	priorityClassName string,
	shmVolume bool,
	podAntiAffinity bool,
	podAntiAffinityTopologyKey string,
) (*v1.PodTemplateSpec, error) {

	terminateGracePeriodSeconds := terminateGracePeriod
	containers := []v1.Container{*spiloContainer}
	containers = append(containers, sidecarContainers...)
	securityContext := v1.PodSecurityContext{}

	if spiloFSGroup != nil {
		securityContext.FSGroup = spiloFSGroup
	}

	podSpec := v1.PodSpec{
		ServiceAccountName:            podServiceAccountName,
		TerminationGracePeriodSeconds: &terminateGracePeriodSeconds,
		Containers:                    containers,
		InitContainers:                initContainers,
		Tolerations:                   *tolerationsSpec,
		SecurityContext:               &securityContext,
	}

	if shmVolume {
		addShmVolume(&podSpec)
	}

	if podAntiAffinity {
		podSpec.Affinity = generatePodAffinity(labels, podAntiAffinityTopologyKey, nodeAffinity)
	} else if nodeAffinity != nil {
		podSpec.Affinity = nodeAffinity
	}

	if priorityClassName != "" {
		podSpec.PriorityClassName = priorityClassName
	}

	template := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:    labels,
			Namespace: namespace,
		},
		Spec: podSpec,
	}
	if kubeIAMRole != "" {
		template.Annotations = map[string]string{constants.KubeIAmAnnotation: kubeIAMRole}
	}

	return &template, nil
}

// generatePodEnvVars generates environment variables for the Spilo Pod
func (c *Cluster) generateSpiloPodEnvVars(uid types.UID, spiloConfiguration string, cloneDescription *acidv1.CloneDescription, customPodEnvVarsList []v1.EnvVar) []v1.EnvVar {
	envVars := []v1.EnvVar{
		{
			Name:  "SCOPE",
			Value: c.Name,
		},
		{
			Name:  "PGROOT",
			Value: constants.PostgresDataPath,
		},
		{
			Name: "POD_IP",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "status.podIP",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.namespace",
				},
			},
		},
		{
			Name:  "PGUSER_SUPERUSER",
			Value: c.OpConfig.SuperUsername,
		},
		{
			Name:  "KUBERNETES_SCOPE_LABEL",
			Value: c.OpConfig.ClusterNameLabel,
		},
		{
			Name:  "KUBERNETES_ROLE_LABEL",
			Value: c.OpConfig.PodRoleLabel,
		},
		{
			Name:  "KUBERNETES_LABELS",
			Value: labels.Set(c.OpConfig.ClusterLabels).String(),
		},
		{
			Name: "PGPASSWORD_SUPERUSER",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.credentialSecretName(c.OpConfig.SuperUsername),
					},
					Key: "password",
				},
			},
		},
		{
			Name:  "PGUSER_STANDBY",
			Value: c.OpConfig.ReplicationUsername,
		},
		{
			Name: "PGPASSWORD_STANDBY",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.credentialSecretName(c.OpConfig.ReplicationUsername),
					},
					Key: "password",
				},
			},
		},
		{
			Name:  "PAM_OAUTH2",
			Value: c.OpConfig.PamConfiguration,
		},
		{
			Name:  "HUMAN_ROLE",
			Value: c.OpConfig.PamRoleName,
		},
	}
	if spiloConfiguration != "" {
		envVars = append(envVars, v1.EnvVar{Name: "SPILO_CONFIGURATION", Value: spiloConfiguration})
	}
	if c.OpConfig.WALES3Bucket != "" {
		envVars = append(envVars, v1.EnvVar{Name: "WAL_S3_BUCKET", Value: c.OpConfig.WALES3Bucket})
		envVars = append(envVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		envVars = append(envVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.LogS3Bucket != "" {
		envVars = append(envVars, v1.EnvVar{Name: "LOG_S3_BUCKET", Value: c.OpConfig.LogS3Bucket})
		envVars = append(envVars, v1.EnvVar{Name: "LOG_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		envVars = append(envVars, v1.EnvVar{Name: "LOG_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.patroniUsesKubernetes() {
		envVars = append(envVars, v1.EnvVar{Name: "DCS_ENABLE_KUBERNETES_API", Value: "true"})
	} else {
		envVars = append(envVars, v1.EnvVar{Name: "ETCD_HOST", Value: c.OpConfig.EtcdHost})
	}

	if cloneDescription.ClusterName != "" {
		envVars = append(envVars, c.generateCloneEnvironment(cloneDescription)...)
	}

	if len(customPodEnvVarsList) > 0 {
		envVars = append(envVars, customPodEnvVarsList...)
	}

	return envVars
}

// deduplicateEnvVars makes sure there are no duplicate in the target envVar array. While Kubernetes already
// deduplicates variables defined in a container, it leaves the last definition in the list and this behavior is not
// well-documented, which means that the behavior can be reversed at some point (it may also start producing an error).
// Therefore, the merge is done by the operator, the entries that are ahead in the passed list take priority over those
// that are behind, and only the name is considered in order to eliminate duplicates.
func deduplicateEnvVars(input []v1.EnvVar, containerName string, logger *logrus.Entry) []v1.EnvVar {
	result := make([]v1.EnvVar, 0)
	names := make(map[string]int)

	for i, va := range input {
		if names[va.Name] == 0 {
			names[va.Name]++
			result = append(result, input[i])
		} else if names[va.Name] == 1 {
			names[va.Name]++
			logger.Warningf("variable %q is defined in %q more than once, the subsequent definitions are ignored",
				va.Name, containerName)
		}
	}
	return result
}

func getSidecarContainer(sidecar acidv1.Sidecar, index int, volumeMounts []v1.VolumeMount,
	resources *v1.ResourceRequirements, superUserName string, credentialsSecretName string, logger *logrus.Entry) *v1.Container {
	name := sidecar.Name
	if name == "" {
		name = fmt.Sprintf("sidecar-%d", index)
	}

	env := []v1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.namespace",
				},
			},
		},
		{
			Name:  "POSTGRES_USER",
			Value: superUserName,
		},
		{
			Name: "POSTGRES_PASSWORD",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: credentialsSecretName,
					},
					Key: "password",
				},
			},
		},
	}
	if len(sidecar.Env) > 0 {
		env = append(env, sidecar.Env...)
	}
	return &v1.Container{
		Name:            name,
		Image:           sidecar.DockerImage,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *resources,
		VolumeMounts:    volumeMounts,
		Env:             deduplicateEnvVars(env, name, logger),
		Ports:           sidecar.Ports,
	}
}

func getBucketScopeSuffix(uid string) string {
	if uid != "" {
		return fmt.Sprintf("/%s", uid)
	}
	return ""
}

func makeResources(cpuRequest, memoryRequest, cpuLimit, memoryLimit string) acidv1.Resources {
	return acidv1.Resources{
		ResourceRequests: acidv1.ResourceDescription{
			CPU:    cpuRequest,
			Memory: memoryRequest,
		},
		ResourceLimits: acidv1.ResourceDescription{
			CPU:    cpuLimit,
			Memory: memoryLimit,
		},
	}
}

func (c *Cluster) generateStatefulSet(spec *acidv1.PostgresSpec) (*v1beta1.StatefulSet, error) {

	var (
		err                 error
		sidecarContainers   []v1.Container
		podTemplate         *v1.PodTemplateSpec
		volumeClaimTemplate *v1.PersistentVolumeClaim
	)

	// Improve me. Please.
	if c.OpConfig.SetMemoryRequestToLimit {

		// controller adjusts the default memory request at operator startup

		request := spec.Resources.ResourceRequests.Memory
		if request == "" {
			request = c.OpConfig.DefaultMemoryRequest
		}

		limit := spec.Resources.ResourceLimits.Memory
		if limit == "" {
			limit = c.OpConfig.DefaultMemoryLimit
		}

		isSmaller, err := util.RequestIsSmallerThanLimit(request, limit)
		if err != nil {
			return nil, err
		}
		if isSmaller {
			c.logger.Warningf("The memory request of %v for the Postgres container is increased to match the memory limit of %v.", request, limit)
			spec.Resources.ResourceRequests.Memory = limit

		}

		// controller adjusts the Scalyr sidecar request at operator startup
		// as this sidecar is managed separately

		// adjust sidecar containers defined for that particular cluster
		for _, sidecar := range spec.Sidecars {

			// TODO #413
			sidecarRequest := sidecar.Resources.ResourceRequests.Memory
			if request == "" {
				request = c.OpConfig.DefaultMemoryRequest
			}

			sidecarLimit := sidecar.Resources.ResourceLimits.Memory
			if limit == "" {
				limit = c.OpConfig.DefaultMemoryLimit
			}

			isSmaller, err := util.RequestIsSmallerThanLimit(sidecarRequest, sidecarLimit)
			if err != nil {
				return nil, err
			}
			if isSmaller {
				c.logger.Warningf("The memory request of %v for the %v sidecar container is increased to match the memory limit of %v.", sidecar.Resources.ResourceRequests.Memory, sidecar.Name, sidecar.Resources.ResourceLimits.Memory)
				sidecar.Resources.ResourceRequests.Memory = sidecar.Resources.ResourceLimits.Memory
			}
		}

	}

	defaultResources := c.makeDefaultResources()

	resourceRequirements, err := generateResourceRequirements(spec.Resources, defaultResources)
	if err != nil {
		return nil, fmt.Errorf("could not generate resource requirements: %v", err)
	}

	customPodEnvVarsList := make([]v1.EnvVar, 0)

	if c.OpConfig.PodEnvironmentConfigMap != "" {
		var cm *v1.ConfigMap
		cm, err = c.KubeClient.ConfigMaps(c.Namespace).Get(c.OpConfig.PodEnvironmentConfigMap, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not read PodEnvironmentConfigMap: %v", err)
		}
		for k, v := range cm.Data {
			customPodEnvVarsList = append(customPodEnvVarsList, v1.EnvVar{Name: k, Value: v})
		}
		sort.Slice(customPodEnvVarsList,
			func(i, j int) bool { return customPodEnvVarsList[i].Name < customPodEnvVarsList[j].Name })
	}

	spiloConfiguration, err := generateSpiloJSONConfiguration(&spec.PostgresqlParam, &spec.Patroni, c.OpConfig.PamRoleName, c.logger)
	if err != nil {
		return nil, fmt.Errorf("could not generate Spilo JSON configuration: %v", err)
	}

	// generate environment variables for the spilo container
	spiloEnvVars := deduplicateEnvVars(
		c.generateSpiloPodEnvVars(c.Postgresql.GetUID(), spiloConfiguration, &spec.Clone,
			customPodEnvVarsList), c.containerName(), c.logger)

	// pickup the docker image for the spilo container
	effectiveDockerImage := util.Coalesce(spec.DockerImage, c.OpConfig.DockerImage)

	volumeMounts := generateVolumeMounts()

	// generate the spilo container
	c.logger.Debugf("Generating Spilo container, environment variables: %v", spiloEnvVars)
	spiloContainer := generateContainer(c.containerName(),
		&effectiveDockerImage,
		resourceRequirements,
		spiloEnvVars,
		volumeMounts,
		c.OpConfig.Resources.SpiloPrivileged,
	)

	// resolve conflicts between operator-global and per-cluster sidecars
	sideCars := c.mergeSidecars(spec.Sidecars)

	resourceRequirementsScalyrSidecar := makeResources(
		c.OpConfig.ScalyrCPURequest,
		c.OpConfig.ScalyrMemoryRequest,
		c.OpConfig.ScalyrCPULimit,
		c.OpConfig.ScalyrMemoryLimit,
	)

	// generate scalyr sidecar container
	if scalyrSidecar :=
		generateScalyrSidecarSpec(c.Name,
			c.OpConfig.ScalyrAPIKey,
			c.OpConfig.ScalyrServerURL,
			c.OpConfig.ScalyrImage,
			&resourceRequirementsScalyrSidecar, c.logger); scalyrSidecar != nil {
		sideCars = append(sideCars, *scalyrSidecar)
	}

	// generate sidecar containers
	if sidecarContainers, err = generateSidecarContainers(sideCars, volumeMounts, defaultResources,
		c.OpConfig.SuperUsername, c.credentialSecretName(c.OpConfig.SuperUsername), c.logger); err != nil {
		return nil, fmt.Errorf("could not generate sidecar containers: %v", err)
	}

	tolerationSpec := tolerations(&spec.Tolerations, c.OpConfig.PodToleration)
	effectivePodPriorityClassName := util.Coalesce(spec.PodPriorityClassName, c.OpConfig.PodPriorityClassName)

	// determine the FSGroup for the spilo pod
	effectiveFSGroup := c.OpConfig.Resources.SpiloFSGroup
	if spec.SpiloFSGroup != nil {
		effectiveFSGroup = spec.SpiloFSGroup
	}

	// generate pod template for the statefulset, based on the spilo container and sidecars
	if podTemplate, err = generatePodTemplate(
		c.Namespace,
		c.labelsSet(true),
		spiloContainer,
		spec.InitContainers,
		sidecarContainers,
		&tolerationSpec,
		effectiveFSGroup,
		nodeAffinity(c.OpConfig.NodeReadinessLabel),
		int64(c.OpConfig.PodTerminateGracePeriod.Seconds()),
		c.OpConfig.PodServiceAccountName,
		c.OpConfig.KubeIAMRole,
		effectivePodPriorityClassName,
		mountShmVolumeNeeded(c.OpConfig, spec),
		c.OpConfig.EnablePodAntiAffinity,
		c.OpConfig.PodAntiAffinityTopologyKey); err != nil {
		return nil, fmt.Errorf("could not generate pod template: %v", err)
	}

	if err != nil {
		return nil, fmt.Errorf("could not generate pod template: %v", err)
	}

	if volumeClaimTemplate, err = generatePersistentVolumeClaimTemplate(spec.Volume.Size,
		spec.Volume.StorageClass); err != nil {
		return nil, fmt.Errorf("could not generate volume claim template: %v", err)
	}

	numberOfInstances := c.getNumberOfInstances(spec)

	// the operator has domain-specific logic on how to do rolling updates of PG clusters
	// so we do not use default rolling updates implemented by stateful sets
	// that leaves the legacy "OnDelete" update strategy as the only option
	updateStrategy := v1beta1.StatefulSetUpdateStrategy{Type: v1beta1.OnDeleteStatefulSetStrategyType}

	var podManagementPolicy v1beta1.PodManagementPolicyType
	if c.OpConfig.PodManagementPolicy == "ordered_ready" {
		podManagementPolicy = v1beta1.OrderedReadyPodManagement
	} else if c.OpConfig.PodManagementPolicy == "parallel" {
		podManagementPolicy = v1beta1.ParallelPodManagement
	} else {
		return nil, fmt.Errorf("could not set the pod management policy to the unknown value: %v", c.OpConfig.PodManagementPolicy)
	}

	statefulSet := &v1beta1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        c.statefulSetName(),
			Namespace:   c.Namespace,
			Labels:      c.labelsSet(true),
			Annotations: map[string]string{rollingUpdateStatefulsetAnnotationKey: "false"},
		},
		Spec: v1beta1.StatefulSetSpec{
			Replicas:             &numberOfInstances,
			Selector:             c.labelsSelector(),
			ServiceName:          c.serviceName(Master),
			Template:             *podTemplate,
			VolumeClaimTemplates: []v1.PersistentVolumeClaim{*volumeClaimTemplate},
			UpdateStrategy:       updateStrategy,
			PodManagementPolicy:  podManagementPolicy,
		},
	}

	return statefulSet, nil
}

func generateScalyrSidecarSpec(clusterName, APIKey, serverURL, dockerImage string,
	containerResources *acidv1.Resources, logger *logrus.Entry) *acidv1.Sidecar {
	if APIKey == "" || dockerImage == "" {
		if APIKey == "" && dockerImage != "" {
			logger.Warning("Not running Scalyr sidecar: SCALYR_API_KEY must be defined")
		}
		return nil
	}
	scalarSpec := &acidv1.Sidecar{
		Name:        "scalyr-sidecar",
		DockerImage: dockerImage,
		Env: []v1.EnvVar{
			{
				Name:  "SCALYR_API_KEY",
				Value: APIKey,
			},
			{
				Name:  "SCALYR_SERVER_HOST",
				Value: clusterName,
			},
		},
		Resources: *containerResources,
	}
	if serverURL != "" {
		scalarSpec.Env = append(scalarSpec.Env, v1.EnvVar{Name: "SCALYR_SERVER_URL", Value: serverURL})
	}
	return scalarSpec
}

// mergeSidecar merges globally-defined sidecars with those defined in the cluster manifest
func (c *Cluster) mergeSidecars(sidecars []acidv1.Sidecar) []acidv1.Sidecar {
	globalSidecarsToSkip := map[string]bool{}
	result := make([]acidv1.Sidecar, 0)

	for i, sidecar := range sidecars {
		dockerImage, ok := c.OpConfig.Sidecars[sidecar.Name]
		if ok {
			if dockerImage != sidecar.DockerImage {
				c.logger.Warningf("merging definitions for sidecar %q: "+
					"ignoring %q in the global scope in favor of %q defined in the cluster",
					sidecar.Name, dockerImage, sidecar.DockerImage)
			}
			globalSidecarsToSkip[sidecar.Name] = true
		}
		result = append(result, sidecars[i])
	}
	for name, dockerImage := range c.OpConfig.Sidecars {
		if !globalSidecarsToSkip[name] {
			result = append(result, acidv1.Sidecar{Name: name, DockerImage: dockerImage})
		}
	}
	return result
}

func (c *Cluster) getNumberOfInstances(spec *acidv1.PostgresSpec) int32 {
	min := c.OpConfig.MinInstances
	max := c.OpConfig.MaxInstances
	cur := spec.NumberOfInstances
	newcur := cur

	if max >= 0 && newcur > max {
		newcur = max
	}
	if min >= 0 && newcur < min {
		newcur = min
	}
	if newcur != cur {
		c.logger.Infof("adjusted number of instances from %d to %d (min: %d, max: %d)", cur, newcur, min, max)
	}

	return newcur
}

// To avoid issues with limited /dev/shm inside docker environment, when
// PostgreSQL can't allocate enough of dsa segments from it, we can
// mount an extra memory volume
//
// see https://docs.okd.io/latest/dev_guide/shared_memory.html
func addShmVolume(podSpec *v1.PodSpec) {
	volumes := append(podSpec.Volumes, v1.Volume{
		Name: constants.ShmVolumeName,
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{
				Medium: "Memory",
			},
		},
	})

	pgIdx := constants.PostgresContainerIdx
	mounts := append(podSpec.Containers[pgIdx].VolumeMounts,
		v1.VolumeMount{
			Name:      constants.ShmVolumeName,
			MountPath: constants.ShmVolumePath,
		})

	podSpec.Containers[0].VolumeMounts = mounts
	podSpec.Volumes = volumes
}

func generatePersistentVolumeClaimTemplate(volumeSize, volumeStorageClass string) (*v1.PersistentVolumeClaim, error) {

	var storageClassName *string

	metadata := metav1.ObjectMeta{
		Name: constants.DataVolumeName,
	}
	if volumeStorageClass != "" {
		// TODO: remove the old annotation, switching completely to the StorageClassName field.
		metadata.Annotations = map[string]string{"volume.beta.kubernetes.io/storage-class": volumeStorageClass}
		storageClassName = &volumeStorageClass
	} else {
		metadata.Annotations = map[string]string{"volume.alpha.kubernetes.io/storage-class": "default"}
		storageClassName = nil
	}

	quantity, err := resource.ParseQuantity(volumeSize)
	if err != nil {
		return nil, fmt.Errorf("could not parse volume size: %v", err)
	}

	volumeMode := v1.PersistentVolumeFilesystem
	volumeClaim := &v1.PersistentVolumeClaim{
		ObjectMeta: metadata,
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: quantity,
				},
			},
			StorageClassName: storageClassName,
			VolumeMode:       &volumeMode,
		},
	}

	return volumeClaim, nil
}

func (c *Cluster) generateUserSecrets() map[string]*v1.Secret {
	secrets := make(map[string]*v1.Secret, len(c.pgUsers))
	namespace := c.Namespace
	for username, pgUser := range c.pgUsers {
		//Skip users with no password i.e. human users (they'll be authenticated using pam)
		secret := c.generateSingleUserSecret(namespace, pgUser)
		if secret != nil {
			secrets[username] = secret
		}
	}
	/* special case for the system user */
	for _, systemUser := range c.systemUsers {
		secret := c.generateSingleUserSecret(namespace, systemUser)
		if secret != nil {
			secrets[systemUser.Name] = secret
		}
	}

	return secrets
}

func (c *Cluster) generateSingleUserSecret(namespace string, pgUser spec.PgUser) *v1.Secret {
	//Skip users with no password i.e. human users (they'll be authenticated using pam)
	if pgUser.Password == "" {
		if pgUser.Origin != spec.RoleOriginTeamsAPI {
			c.logger.Warningf("could not generate secret for a non-teamsAPI role %q: role has no password",
				pgUser.Name)
		}
		return nil
	}

	username := pgUser.Name
	secret := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.credentialSecretName(username),
			Namespace: namespace,
			Labels:    c.labelsSet(true),
		},
		Type: v1.SecretTypeOpaque,
		Data: map[string][]byte{
			"username": []byte(pgUser.Name),
			"password": []byte(pgUser.Password),
		},
	}
	return &secret
}

func (c *Cluster) shouldCreateLoadBalancerForService(role PostgresRole, spec *acidv1.PostgresSpec) bool {

	switch role {

	case Replica:

		// if the value is explicitly set in a Postgresql manifest, follow this setting
		if spec.EnableReplicaLoadBalancer != nil {
			return *spec.EnableReplicaLoadBalancer
		}

		// otherwise, follow the operator configuration
		return c.OpConfig.EnableReplicaLoadBalancer

	case Master:

		if spec.EnableMasterLoadBalancer != nil {
			return *spec.EnableMasterLoadBalancer
		}

		return c.OpConfig.EnableMasterLoadBalancer

	default:
		panic(fmt.Sprintf("Unknown role %v", role))
	}

}

func (c *Cluster) generateService(role PostgresRole, spec *acidv1.PostgresSpec) *v1.Service {
	var dnsName string

	if role == Master {
		dnsName = c.masterDNSName()
	} else {
		dnsName = c.replicaDNSName()
	}

	serviceSpec := v1.ServiceSpec{
		Ports: []v1.ServicePort{{Name: "postgresql", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}}},
		Type:  v1.ServiceTypeClusterIP,
	}

	if role == Replica {
		serviceSpec.Selector = c.roleLabelsSet(false, role)
	}

	var annotations map[string]string

	if c.shouldCreateLoadBalancerForService(role, spec) {

		// spec.AllowedSourceRanges evaluates to the empty slice of zero length
		// when omitted or set to 'null'/empty sequence in the PG manifest
		if len(spec.AllowedSourceRanges) > 0 {
			serviceSpec.LoadBalancerSourceRanges = spec.AllowedSourceRanges
		} else {
			// safe default value: lock a load balancer only to the local address unless overridden explicitly
			serviceSpec.LoadBalancerSourceRanges = []string{localHost}
		}

		c.logger.Debugf("final load balancer source ranges as seen in a service spec (not necessarily applied): %q", serviceSpec.LoadBalancerSourceRanges)
		serviceSpec.Type = v1.ServiceTypeLoadBalancer

		annotations = map[string]string{
			constants.ZalandoDNSNameAnnotation: dnsName,
			constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
		}

		if len(c.OpConfig.CustomServiceAnnotations) != 0 {
			c.logger.Debugf("There are custom annotations defined, creating them.")
			for customAnnotationKey, customAnnotationValue := range c.OpConfig.CustomServiceAnnotations {
				annotations[customAnnotationKey] = customAnnotationValue
			}
		}
	} else if role == Replica {
		// before PR #258, the replica service was only created if allocated a LB
		// now we always create the service but warn if the LB is absent
		c.logger.Debugf("No load balancer created for the replica service")
	}

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        c.serviceName(role),
			Namespace:   c.Namespace,
			Labels:      c.roleLabelsSet(true, role),
			Annotations: annotations,
		},
		Spec: serviceSpec,
	}

	return service
}

func (c *Cluster) generateEndpoint(role PostgresRole, subsets []v1.EndpointSubset) *v1.Endpoints {
	endpoints := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.endpointName(role),
			Namespace: c.Namespace,
			Labels:    c.roleLabelsSet(true, role),
		},
	}
	if len(subsets) > 0 {
		endpoints.Subsets = subsets
	}

	return endpoints
}

func (c *Cluster) generateCloneEnvironment(description *acidv1.CloneDescription) []v1.EnvVar {
	result := make([]v1.EnvVar, 0)

	if description.ClusterName == "" {
		return result
	}

	cluster := description.ClusterName
	result = append(result, v1.EnvVar{Name: "CLONE_SCOPE", Value: cluster})
	if description.EndTimestamp == "" {
		// cloning with basebackup, make a connection string to the cluster to clone from
		host, port := c.getClusterServiceConnectionParameters(cluster)
		// TODO: make some/all of those constants
		result = append(result, v1.EnvVar{Name: "CLONE_METHOD", Value: "CLONE_WITH_BASEBACKUP"})
		result = append(result, v1.EnvVar{Name: "CLONE_HOST", Value: host})
		result = append(result, v1.EnvVar{Name: "CLONE_PORT", Value: port})
		// TODO: assume replication user name is the same for all clusters, fetch it from secrets otherwise
		result = append(result, v1.EnvVar{Name: "CLONE_USER", Value: c.OpConfig.ReplicationUsername})
		result = append(result,
			v1.EnvVar{Name: "CLONE_PASSWORD",
				ValueFrom: &v1.EnvVarSource{
					SecretKeyRef: &v1.SecretKeySelector{
						LocalObjectReference: v1.LocalObjectReference{
							Name: c.credentialSecretNameForCluster(c.OpConfig.ReplicationUsername,
								description.ClusterName),
						},
						Key: "password",
					},
				},
			})
	} else {
		// cloning with S3, find out the bucket to clone
		msg := "Clone from S3 bucket"
		c.logger.Info(msg, description.S3WalPath)

		if description.S3WalPath == "" {
			msg := "Figure out which S3 bucket to use from env"
			c.logger.Info(msg, description.S3WalPath)

			envs := []v1.EnvVar{
				v1.EnvVar{
					Name:  "CLONE_WAL_S3_BUCKET",
					Value: c.OpConfig.WALES3Bucket,
				},
				v1.EnvVar{
					Name:  "CLONE_WAL_BUCKET_SCOPE_SUFFIX",
					Value: getBucketScopeSuffix(description.UID),
				},
			}

			result = append(result, envs...)
		} else {
			msg := "Use custom parsed S3WalPath %s from the manifest"
			c.logger.Warningf(msg, description.S3WalPath)

			result = append(result, v1.EnvVar{
				Name:  "CLONE_WALE_S3_PREFIX",
				Value: description.S3WalPath,
			})
		}

		result = append(result, v1.EnvVar{Name: "CLONE_METHOD", Value: "CLONE_WITH_WALE"})
		result = append(result, v1.EnvVar{Name: "CLONE_TARGET_TIME", Value: description.EndTimestamp})
		result = append(result, v1.EnvVar{Name: "CLONE_WAL_BUCKET_SCOPE_PREFIX", Value: ""})

		if description.S3Endpoint != "" {
		   result = append(result, v1.EnvVar{Name: "CLONE_AWS_ENDPOINT", Value: description.S3Endpoint})
		   result = append(result, v1.EnvVar{Name: "CLONE_WALE_S3_ENDPOINT", Value: description.S3Endpoint})
		}

		if description.S3AccessKeyId != "" {
		   result = append(result, v1.EnvVar{Name: "CLONE_AWS_ACCESS_KEY_ID", Value: description.S3AccessKeyId})
		}

		if description.S3SecretAccessKey != "" {
		   result = append(result, v1.EnvVar{Name: "CLONE_AWS_SECRET_ACCESS_KEY", Value: description.S3SecretAccessKey})
		}

		if description.S3ForcePathStyle != nil {
		   s3ForcePathStyle := "0"

		   if *description.S3ForcePathStyle {
			   s3ForcePathStyle = "1"
		   }

		   result = append(result, v1.EnvVar{Name: "CLONE_AWS_S3_FORCE_PATH_STYLE", Value: s3ForcePathStyle})
		}
	}

	return result
}

func (c *Cluster) generatePodDisruptionBudget() *policybeta1.PodDisruptionBudget {
	minAvailable := intstr.FromInt(1)

	return &policybeta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.podDisruptionBudgetName(),
			Namespace: c.Namespace,
			Labels:    c.labelsSet(true),
		},
		Spec: policybeta1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: c.roleLabelsSet(false, Master),
			},
		},
	}
}

// getClusterServiceConnectionParameters fetches cluster host name and port
// TODO: perhaps we need to query the service (i.e. if non-standard port is used?)
// TODO: handle clusters in different namespaces
func (c *Cluster) getClusterServiceConnectionParameters(clusterName string) (host string, port string) {
	host = clusterName
	port = "5432"
	return
}

func (c *Cluster) generateLogicalBackupJob() (*batchv1beta1.CronJob, error) {

	var (
		err                  error
		podTemplate          *v1.PodTemplateSpec
		resourceRequirements *v1.ResourceRequirements
	)

	// NB: a cron job creates standard batch jobs according to schedule; these batch jobs manage pods and clean-up

	c.logger.Debug("Generating logical backup pod template")

	// allocate for the backup pod the same amount of resources as for normal DB pods
	defaultResources := c.makeDefaultResources()
	resourceRequirements, err = generateResourceRequirements(c.Spec.Resources, defaultResources)
	if err != nil {
		return nil, fmt.Errorf("could not generate resource requirements for logical backup pods: %v", err)
	}

	envVars := c.generateLogicalBackupPodEnvVars()
	logicalBackupContainer := generateContainer(
		"logical-backup",
		&c.OpConfig.LogicalBackup.LogicalBackupDockerImage,
		resourceRequirements,
		envVars,
		[]v1.VolumeMount{},
		c.OpConfig.SpiloPrivileged, // use same value as for normal DB pods
	)

	labels := map[string]string{
		"version":     c.Name,
		"application": "spilo-logical-backup",
	}
	podAffinityTerm := v1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		TopologyKey: "kubernetes.io/hostname",
	}
	podAffinity := v1.Affinity{
		PodAffinity: &v1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{{
				Weight:          1,
				PodAffinityTerm: podAffinityTerm,
			},
			},
		}}

	// re-use the method that generates DB pod templates
	if podTemplate, err = generatePodTemplate(
		c.Namespace,
		c.labelsSet(true),
		logicalBackupContainer,
		[]v1.Container{},
		[]v1.Container{},
		&[]v1.Toleration{},
		nil,
		nodeAffinity(c.OpConfig.NodeReadinessLabel),
		int64(c.OpConfig.PodTerminateGracePeriod.Seconds()),
		c.OpConfig.PodServiceAccountName,
		c.OpConfig.KubeIAMRole,
		"",
		false,
		false,
		""); err != nil {
		return nil, fmt.Errorf("could not generate pod template for logical backup pod: %v", err)
	}

	// overwrite specific params of logical backups pods
	podTemplate.Spec.Affinity = &podAffinity
	podTemplate.Spec.RestartPolicy = "Never" // affects containers within a pod

	// configure a batch job

	jobSpec := batchv1.JobSpec{
		Template: *podTemplate,
	}

	// configure a cron job

	jobTemplateSpec := batchv1beta1.JobTemplateSpec{
		Spec: jobSpec,
	}

	schedule := c.Postgresql.Spec.LogicalBackupSchedule
	if schedule == "" {
		schedule = c.OpConfig.LogicalBackupSchedule
	}

	cronJob := &batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.getLogicalBackupJobName(),
			Namespace: c.Namespace,
			Labels:    c.labelsSet(true),
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:          schedule,
			JobTemplate:       jobTemplateSpec,
			ConcurrencyPolicy: batchv1beta1.ForbidConcurrent,
		},
	}

	return cronJob, nil
}

func (c *Cluster) generateLogicalBackupPodEnvVars() []v1.EnvVar {

	envVars := []v1.EnvVar{
		{
			Name:  "SCOPE",
			Value: c.Name,
		},
		// Bucket env vars
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3Bucket,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX",
			Value: getBucketScopeSuffix(string(c.Postgresql.GetUID())),
		},
		// Postgres env vars
		{
			Name:  "PG_VERSION",
			Value: c.Spec.PgVersion,
		},
		{
			Name:  "PGPORT",
			Value: "5432",
		},
		{
			Name:  "PGUSER",
			Value: c.OpConfig.SuperUsername,
		},
		{
			Name:  "PGDATABASE",
			Value: c.OpConfig.SuperUsername,
		},
		{
			Name:  "PGSSLMODE",
			Value: "require",
		},
		{
			Name: "PGPASSWORD",
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.credentialSecretName(c.OpConfig.SuperUsername),
					},
					Key: "password",
				},
			},
		},
	}

	c.logger.Debugf("Generated logical backup env vars %v", envVars)

	return envVars
}

// getLogicalBackupJobName returns the name; the job itself may not exists
func (c *Cluster) getLogicalBackupJobName() (jobName string) {
	return "logical-backup-" + c.clusterName().Name
}
