package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
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
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	pgBinariesLocationTemplate       = "/usr/lib/postgresql/%v/bin"
	patroniPGBinariesParameterName   = "bin_dir"
	patroniPGParametersParameterName = "parameters"
	patroniPGHBAConfParameterName    = "pg_hba"
	localHost                        = "127.0.0.1/32"
	connectionPoolerContainer        = "connection-pooler"
	pgPort                           = 5432
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
	SynchronousMode          bool                         `json:"synchronous_mode,omitempty"`
	SynchronousModeStrict    bool                         `json:"synchronous_mode_strict,omitempty"`
	PGBootstrapConfiguration map[string]interface{}       `json:"postgresql,omitempty"`
	Slots                    map[string]map[string]string `json:"slots,omitempty"`
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

func (c *Cluster) serviceAddress(role PostgresRole) string {
	service, exist := c.Services[role]

	if exist {
		return service.ObjectMeta.Name
	}

	c.logger.Warningf("No service for role %s", role)
	return ""
}

func (c *Cluster) servicePort(role PostgresRole) string {
	service, exist := c.Services[role]

	if exist {
		return fmt.Sprint(service.Spec.Ports[0].Port)
	}

	c.logger.Warningf("No service for role %s", role)
	return ""
}

func (c *Cluster) podDisruptionBudgetName() string {
	return c.OpConfig.PDBNameFormat.Format("cluster", c.Name)
}

func (c *Cluster) makeDefaultResources() acidv1.Resources {

	config := c.OpConfig

	defaultRequests := acidv1.ResourceDescription{
		CPU:    config.Resources.DefaultCPURequest,
		Memory: config.Resources.DefaultMemoryRequest,
	}
	defaultLimits := acidv1.ResourceDescription{
		CPU:    config.Resources.DefaultCPULimit,
		Memory: config.Resources.DefaultMemoryLimit,
	}

	return acidv1.Resources{
		ResourceRequests: defaultRequests,
		ResourceLimits:   defaultLimits,
	}
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

func generateSpiloJSONConfiguration(pg *acidv1.PostgresqlParam, patroni *acidv1.Patroni, pamRoleName string, EnablePgVersionEnvVar bool, logger *logrus.Entry) (string, error) {
	config := spiloConfiguration{}

	config.Bootstrap = pgBootstrap{}

	config.Bootstrap.Initdb = []interface{}{map[string]string{"auth-host": "md5"},
		map[string]string{"auth-local": "trust"}}

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
			switch t := defaultParam.(type) {
			case map[string]string:
				{
					for k1 := range t {
						if k1 == k {
							(config.Bootstrap.Initdb[i]).(map[string]string)[k] = v
							continue PatroniInitDBParams
						}
					}
				}
			case string:
				{
					/* if the option already occurs in the list */
					if t == v {
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
	if patroni.SynchronousMode {
		config.Bootstrap.DCS.SynchronousMode = patroni.SynchronousMode
	}
	if patroni.SynchronousModeStrict {
		config.Bootstrap.DCS.SynchronousModeStrict = patroni.SynchronousModeStrict
	}

	config.PgLocalConfiguration = make(map[string]interface{})

	// the newer and preferred way to specify the PG version is to use the `PGVERSION` env variable
	// setting postgresq.bin_dir in the SPILO_CONFIGURATION still works and takes precedence over PGVERSION
	// so we add postgresq.bin_dir only if PGVERSION is unused
	// see PR 222 in Spilo
	if !EnablePgVersionEnvVar {
		config.PgLocalConfiguration[patroniPGBinariesParameterName] = fmt.Sprintf(pgBinariesLocationTemplate, pg.PgVersion)
	}
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

func generateCapabilities(capabilities []string) *v1.Capabilities {
	additionalCapabilities := make([]v1.Capability, 0, len(capabilities))
	for _, capability := range capabilities {
		additionalCapabilities = append(additionalCapabilities, v1.Capability(strings.ToUpper(capability)))
	}
	if len(additionalCapabilities) > 0 {
		return &v1.Capabilities{
			Add: additionalCapabilities,
		}
	}
	return nil
}

func nodeAffinity(nodeReadinessLabel map[string]string, nodeAffinity *v1.NodeAffinity) *v1.Affinity {
	if len(nodeReadinessLabel) == 0 && nodeAffinity == nil {
		return nil
	}
	nodeAffinityCopy := v1.NodeAffinity{}
	if nodeAffinity != nil {
		nodeAffinityCopy = *nodeAffinity.DeepCopy()
	}
	if len(nodeReadinessLabel) > 0 {
		matchExpressions := make([]v1.NodeSelectorRequirement, 0)
		for k, v := range nodeReadinessLabel {
			matchExpressions = append(matchExpressions, v1.NodeSelectorRequirement{
				Key:      k,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{v},
			})
		}
		nodeReadinessSelectorTerm := v1.NodeSelectorTerm{MatchExpressions: matchExpressions}
		if nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					nodeReadinessSelectorTerm,
				},
			}
		} else {
			nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{
				NodeSelectorTerms: append(nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms, nodeReadinessSelectorTerm),
			}
		}
	}

	return &v1.Affinity{
		NodeAffinity: &nodeAffinityCopy,
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

	if len(podToleration["key"]) > 0 ||
		len(podToleration["operator"]) > 0 ||
		len(podToleration["value"]) > 0 ||
		len(podToleration["effect"]) > 0 {

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
	params := map[string]bool{
		"archive_command":                  false,
		"shared_buffers":                   false,
		"logging_collector":                false,
		"log_destination":                  false,
		"log_directory":                    false,
		"log_filename":                     false,
		"log_file_mode":                    false,
		"log_rotation_age":                 false,
		"log_truncate_on_rotation":         false,
		"ssl":                              false,
		"ssl_ca_file":                      false,
		"ssl_crl_file":                     false,
		"ssl_cert_file":                    false,
		"ssl_key_file":                     false,
		"shared_preload_libraries":         false,
		"bg_mon.listen_address":            false,
		"bg_mon.history_buckets":           false,
		"pg_stat_statements.track_utility": false,
		"extwlist.extensions":              false,
		"extwlist.custom_path":             false,
	}
	result, ok := params[param]
	if !ok {
		result = true
	}
	return result
}

func generateVolumeMounts(volume acidv1.Volume) []v1.VolumeMount {
	return []v1.VolumeMount{
		{
			Name:      constants.DataVolumeName,
			MountPath: constants.PostgresDataMount, //TODO: fetch from manifest
			SubPath:   volume.SubPath,
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
	privilegeEscalationMode *bool,
	additionalPodCapabilities *v1.Capabilities,
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
			AllowPrivilegeEscalation: privilegeEscalationMode,
			Privileged:               &privilegedMode,
			ReadOnlyRootFilesystem:   util.False(),
			Capabilities:             additionalPodCapabilities,
		},
	}
}

func generateSidecarContainers(sidecars []acidv1.Sidecar,
	defaultResources acidv1.Resources, startIndex int, logger *logrus.Entry) ([]v1.Container, error) {

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

			sc := getSidecarContainer(sidecar, startIndex+index, resources)
			result = append(result, *sc)
		}
		return result, nil
	}
	return nil, nil
}

// adds common fields to sidecars
func patchSidecarContainers(in []v1.Container, volumeMounts []v1.VolumeMount, superUserName string, credentialsSecretName string, logger *logrus.Entry) []v1.Container {
	result := []v1.Container{}

	for _, container := range in {
		container.VolumeMounts = append(container.VolumeMounts, volumeMounts...)
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
		mergedEnv := append(env, container.Env...)
		container.Env = deduplicateEnvVars(mergedEnv, container.Name, logger)
		result = append(result, container)
	}

	return result
}

// Check whether or not we're requested to mount an shm volume,
// taking into account that PostgreSQL manifest has precedence.
func mountShmVolumeNeeded(opConfig config.Config, spec *acidv1.PostgresSpec) *bool {
	if spec.ShmVolume != nil && *spec.ShmVolume {
		return spec.ShmVolume
	}

	return opConfig.ShmVolume
}

func (c *Cluster) generatePodTemplate(
	namespace string,
	labels labels.Set,
	annotations map[string]string,
	spiloContainer *v1.Container,
	initContainers []v1.Container,
	sidecarContainers []v1.Container,
	tolerationsSpec *[]v1.Toleration,
	spiloRunAsUser *int64,
	spiloRunAsGroup *int64,
	spiloFSGroup *int64,
	nodeAffinity *v1.Affinity,
	schedulerName *string,
	terminateGracePeriod int64,
	podServiceAccountName string,
	kubeIAMRole string,
	priorityClassName string,
	shmVolume *bool,
	podAntiAffinity bool,
	podAntiAffinityTopologyKey string,
	additionalSecretMount string,
	additionalSecretMountPath string,
	additionalVolumes []acidv1.AdditionalVolume,
) (*v1.PodTemplateSpec, error) {

	terminateGracePeriodSeconds := terminateGracePeriod
	containers := []v1.Container{*spiloContainer}
	containers = append(containers, sidecarContainers...)
	securityContext := v1.PodSecurityContext{}

	if spiloRunAsUser != nil {
		securityContext.RunAsUser = spiloRunAsUser
	}

	if spiloRunAsGroup != nil {
		securityContext.RunAsGroup = spiloRunAsGroup
	}

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

	if schedulerName != nil {
		podSpec.SchedulerName = *schedulerName
	}

	if shmVolume != nil && *shmVolume {
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

	if additionalSecretMount != "" {
		addSecretVolume(&podSpec, additionalSecretMount, additionalSecretMountPath)
	}

	if additionalVolumes != nil {
		c.addAdditionalVolumes(&podSpec, additionalVolumes)
	}

	template := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Namespace:   namespace,
			Annotations: annotations,
		},
		Spec: podSpec,
	}
	if kubeIAMRole != "" {
		if template.Annotations == nil {
			template.Annotations = make(map[string]string)
		}
		template.Annotations[constants.KubeIAmAnnotation] = kubeIAMRole
	}

	return &template, nil
}

// generatePodEnvVars generates environment variables for the Spilo Pod
func (c *Cluster) generateSpiloPodEnvVars(uid types.UID, spiloConfiguration string, cloneDescription *acidv1.CloneDescription, standbyDescription *acidv1.StandbyDescription, customPodEnvVarsList []v1.EnvVar) []v1.EnvVar {
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
	if c.OpConfig.EnablePgVersionEnvVar {
		envVars = append(envVars, v1.EnvVar{Name: "PGVERSION", Value: c.GetDesiredMajorVersion()})
	}
	// Spilo expects cluster labels as JSON
	if clusterLabels, err := json.Marshal(labels.Set(c.OpConfig.ClusterLabels)); err != nil {
		envVars = append(envVars, v1.EnvVar{Name: "KUBERNETES_LABELS", Value: labels.Set(c.OpConfig.ClusterLabels).String()})
	} else {
		envVars = append(envVars, v1.EnvVar{Name: "KUBERNETES_LABELS", Value: string(clusterLabels)})
	}
	if spiloConfiguration != "" {
		envVars = append(envVars, v1.EnvVar{Name: "SPILO_CONFIGURATION", Value: spiloConfiguration})
	}

	if c.patroniUsesKubernetes() {
		envVars = append(envVars, v1.EnvVar{Name: "DCS_ENABLE_KUBERNETES_API", Value: "true"})
	} else {
		envVars = append(envVars, v1.EnvVar{Name: "ETCD_HOST", Value: c.OpConfig.EtcdHost})
	}

	if c.patroniKubernetesUseConfigMaps() {
		envVars = append(envVars, v1.EnvVar{Name: "KUBERNETES_USE_CONFIGMAPS", Value: "true"})
	}

	if cloneDescription != nil && cloneDescription.ClusterName != "" {
		envVars = append(envVars, c.generateCloneEnvironment(cloneDescription)...)
	}

	if c.Spec.StandbyCluster != nil {
		envVars = append(envVars, c.generateStandbyEnvironment(standbyDescription)...)
	}

	// add vars taken from pod_environment_configmap and pod_environment_secret first
	// (to allow them to override the globals set in the operator config)
	if len(customPodEnvVarsList) > 0 {
		envVars = append(envVars, customPodEnvVarsList...)
	}

	if c.OpConfig.WALES3Bucket != "" {
		envVars = append(envVars, v1.EnvVar{Name: "WAL_S3_BUCKET", Value: c.OpConfig.WALES3Bucket})
		envVars = append(envVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		envVars = append(envVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.WALGSBucket != "" {
		envVars = append(envVars, v1.EnvVar{Name: "WAL_GS_BUCKET", Value: c.OpConfig.WALGSBucket})
		envVars = append(envVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		envVars = append(envVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.GCPCredentials != "" {
		envVars = append(envVars, v1.EnvVar{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: c.OpConfig.GCPCredentials})
	}

	if c.OpConfig.LogS3Bucket != "" {
		envVars = append(envVars, v1.EnvVar{Name: "LOG_S3_BUCKET", Value: c.OpConfig.LogS3Bucket})
		envVars = append(envVars, v1.EnvVar{Name: "LOG_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		envVars = append(envVars, v1.EnvVar{Name: "LOG_BUCKET_SCOPE_PREFIX", Value: ""})
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

			// Some variables (those to configure the WAL_ and LOG_ shipping) may be overwritten, only log as info
			if strings.HasPrefix(va.Name, "WAL_") || strings.HasPrefix(va.Name, "LOG_") {
				logger.Infof("global variable %q has been overwritten by configmap/secret for container %q",
					va.Name, containerName)
			} else {
				logger.Warningf("variable %q is defined in %q more than once, the subsequent definitions are ignored",
					va.Name, containerName)
			}
		}
	}
	return result
}

// Return list of variables the pod recieved from the configured ConfigMap
func (c *Cluster) getPodEnvironmentConfigMapVariables() ([]v1.EnvVar, error) {
	configMapPodEnvVarsList := make([]v1.EnvVar, 0)

	if c.OpConfig.PodEnvironmentConfigMap.Name == "" {
		return configMapPodEnvVarsList, nil
	}

	cm, err := c.KubeClient.ConfigMaps(c.OpConfig.PodEnvironmentConfigMap.Namespace).Get(
		context.TODO(),
		c.OpConfig.PodEnvironmentConfigMap.Name,
		metav1.GetOptions{})
	if err != nil {
		// if not found, try again using the cluster's namespace if it's different (old behavior)
		if k8sutil.ResourceNotFound(err) && c.Namespace != c.OpConfig.PodEnvironmentConfigMap.Namespace {
			cm, err = c.KubeClient.ConfigMaps(c.Namespace).Get(
				context.TODO(),
				c.OpConfig.PodEnvironmentConfigMap.Name,
				metav1.GetOptions{})
		}
		if err != nil {
			return nil, fmt.Errorf("could not read PodEnvironmentConfigMap: %v", err)
		}
	}
	for k, v := range cm.Data {
		configMapPodEnvVarsList = append(configMapPodEnvVarsList, v1.EnvVar{Name: k, Value: v})
	}
	return configMapPodEnvVarsList, nil
}

// Return list of variables the pod received from the configured Secret
func (c *Cluster) getPodEnvironmentSecretVariables() ([]v1.EnvVar, error) {
	secretPodEnvVarsList := make([]v1.EnvVar, 0)

	if c.OpConfig.PodEnvironmentSecret == "" {
		return secretPodEnvVarsList, nil
	}

	secret, err := c.KubeClient.Secrets(c.Namespace).Get(
		context.TODO(),
		c.OpConfig.PodEnvironmentSecret,
		metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not read Secret PodEnvironmentSecretName: %v", err)
	}

	for k := range secret.Data {
		secretPodEnvVarsList = append(secretPodEnvVarsList,
			v1.EnvVar{Name: k, ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.OpConfig.PodEnvironmentSecret,
					},
					Key: k,
				},
			}})
	}

	return secretPodEnvVarsList, nil
}

func getSidecarContainer(sidecar acidv1.Sidecar, index int, resources *v1.ResourceRequirements) *v1.Container {
	name := sidecar.Name
	if name == "" {
		name = fmt.Sprintf("sidecar-%d", index)
	}

	return &v1.Container{
		Name:            name,
		Image:           sidecar.DockerImage,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *resources,
		Env:             sidecar.Env,
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

func extractPgVersionFromBinPath(binPath string, template string) (string, error) {
	var pgVersion float32
	_, err := fmt.Sscanf(binPath, template, &pgVersion)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", pgVersion), nil
}

func (c *Cluster) generateStatefulSet(spec *acidv1.PostgresSpec) (*appsv1.StatefulSet, error) {

	var (
		err                 error
		initContainers      []v1.Container
		sidecarContainers   []v1.Container
		podTemplate         *v1.PodTemplateSpec
		volumeClaimTemplate *v1.PersistentVolumeClaim
		additionalVolumes   = spec.AdditionalVolumes
	)

	// Improve me. Please.
	if c.OpConfig.SetMemoryRequestToLimit {

		// controller adjusts the default memory request at operator startup

		request := spec.Resources.ResourceRequests.Memory
		if request == "" {
			request = c.OpConfig.Resources.DefaultMemoryRequest
		}

		limit := spec.Resources.ResourceLimits.Memory
		if limit == "" {
			limit = c.OpConfig.Resources.DefaultMemoryLimit
		}

		isSmaller, err := util.IsSmallerQuantity(request, limit)
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
				request = c.OpConfig.Resources.DefaultMemoryRequest
			}

			sidecarLimit := sidecar.Resources.ResourceLimits.Memory
			if limit == "" {
				limit = c.OpConfig.Resources.DefaultMemoryLimit
			}

			isSmaller, err := util.IsSmallerQuantity(sidecarRequest, sidecarLimit)
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

	if spec.InitContainers != nil && len(spec.InitContainers) > 0 {
		if c.OpConfig.EnableInitContainers != nil && !(*c.OpConfig.EnableInitContainers) {
			c.logger.Warningf("initContainers specified but disabled in configuration - next statefulset creation would fail")
		}
		initContainers = spec.InitContainers
	}

	spiloCompathWalPathList := make([]v1.EnvVar, 0)
	if c.OpConfig.EnableSpiloWalPathCompat {
		spiloCompathWalPathList = append(spiloCompathWalPathList,
			v1.EnvVar{
				Name:  "ENABLE_WAL_PATH_COMPAT",
				Value: "true",
			},
		)
	}

	// fetch env vars from custom ConfigMap
	configMapEnvVarsList, err := c.getPodEnvironmentConfigMapVariables()
	if err != nil {
		return nil, err
	}

	// fetch env vars from custom ConfigMap
	secretEnvVarsList, err := c.getPodEnvironmentSecretVariables()
	if err != nil {
		return nil, err
	}

	// concat all custom pod env vars and sort them
	customPodEnvVarsList := append(spiloCompathWalPathList, configMapEnvVarsList...)
	customPodEnvVarsList = append(customPodEnvVarsList, secretEnvVarsList...)
	sort.Slice(customPodEnvVarsList,
		func(i, j int) bool { return customPodEnvVarsList[i].Name < customPodEnvVarsList[j].Name })

	if spec.StandbyCluster != nil && spec.StandbyCluster.S3WalPath == "" && spec.StandbyCluster.ClusterName == "" {
		return nil, fmt.Errorf("s3_wal_path and cluster are both empty for standby cluster")
	}

	// backward compatible check for InitContainers
	if spec.InitContainersOld != nil {
		msg := "Manifest parameter init_containers is deprecated."
		if spec.InitContainers == nil {
			c.logger.Warningf("%s Consider using initContainers instead.", msg)
			spec.InitContainers = spec.InitContainersOld
		} else {
			c.logger.Warningf("%s Only value from initContainers is used", msg)
		}
	}

	// backward compatible check for PodPriorityClassName
	if spec.PodPriorityClassNameOld != "" {
		msg := "Manifest parameter pod_priority_class_name is deprecated."
		if spec.PodPriorityClassName == "" {
			c.logger.Warningf("%s Consider using podPriorityClassName instead.", msg)
			spec.PodPriorityClassName = spec.PodPriorityClassNameOld
		} else {
			c.logger.Warningf("%s Only value from podPriorityClassName is used", msg)
		}
	}

	spiloConfiguration, err := generateSpiloJSONConfiguration(&spec.PostgresqlParam, &spec.Patroni, c.OpConfig.PamRoleName, c.OpConfig.EnablePgVersionEnvVar, c.logger)
	if err != nil {
		return nil, fmt.Errorf("could not generate Spilo JSON configuration: %v", err)
	}

	// generate environment variables for the spilo container
	spiloEnvVars := c.generateSpiloPodEnvVars(
		c.Postgresql.GetUID(),
		spiloConfiguration,
		spec.Clone,
		spec.StandbyCluster,
		customPodEnvVarsList,
	)

	// pickup the docker image for the spilo container
	effectiveDockerImage := util.Coalesce(spec.DockerImage, c.OpConfig.DockerImage)

	// determine the User, Group and FSGroup for the spilo pod
	effectiveRunAsUser := c.OpConfig.Resources.SpiloRunAsUser
	if spec.SpiloRunAsUser != nil {
		effectiveRunAsUser = spec.SpiloRunAsUser
	}

	effectiveRunAsGroup := c.OpConfig.Resources.SpiloRunAsGroup
	if spec.SpiloRunAsGroup != nil {
		effectiveRunAsGroup = spec.SpiloRunAsGroup
	}

	effectiveFSGroup := c.OpConfig.Resources.SpiloFSGroup
	if spec.SpiloFSGroup != nil {
		effectiveFSGroup = spec.SpiloFSGroup
	}

	volumeMounts := generateVolumeMounts(spec.Volume)

	// configure TLS with a custom secret volume
	if spec.TLS != nil && spec.TLS.SecretName != "" {
		// this is combined with the FSGroup in the section above
		// to give read access to the postgres user
		defaultMode := int32(0640)
		mountPath := "/tls"
		additionalVolumes = append(additionalVolumes, acidv1.AdditionalVolume{
			Name:      spec.TLS.SecretName,
			MountPath: mountPath,
			VolumeSource: v1.VolumeSource{
				Secret: &v1.SecretVolumeSource{
					SecretName:  spec.TLS.SecretName,
					DefaultMode: &defaultMode,
				},
			},
		})

		// use the same filenames as Secret resources by default
		certFile := ensurePath(spec.TLS.CertificateFile, mountPath, "tls.crt")
		privateKeyFile := ensurePath(spec.TLS.PrivateKeyFile, mountPath, "tls.key")
		spiloEnvVars = append(
			spiloEnvVars,
			v1.EnvVar{Name: "SSL_CERTIFICATE_FILE", Value: certFile},
			v1.EnvVar{Name: "SSL_PRIVATE_KEY_FILE", Value: privateKeyFile},
		)

		if spec.TLS.CAFile != "" {
			// support scenario when the ca.crt resides in a different secret, diff path
			mountPathCA := mountPath
			if spec.TLS.CASecretName != "" {
				mountPathCA = mountPath + "ca"
			}

			caFile := ensurePath(spec.TLS.CAFile, mountPathCA, "")
			spiloEnvVars = append(
				spiloEnvVars,
				v1.EnvVar{Name: "SSL_CA_FILE", Value: caFile},
			)

			// the ca file from CASecretName secret takes priority
			if spec.TLS.CASecretName != "" {
				additionalVolumes = append(additionalVolumes, acidv1.AdditionalVolume{
					Name:      spec.TLS.CASecretName,
					MountPath: mountPathCA,
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName:  spec.TLS.CASecretName,
							DefaultMode: &defaultMode,
						},
					},
				})
			}
		}
	}

	// generate the spilo container
	c.logger.Debugf("Generating Spilo container, environment variables")
	c.logger.Debugf("%v", spiloEnvVars)

	spiloContainer := generateContainer(constants.PostgresContainerName,
		&effectiveDockerImage,
		resourceRequirements,
		deduplicateEnvVars(spiloEnvVars, constants.PostgresContainerName, c.logger),
		volumeMounts,
		c.OpConfig.Resources.SpiloPrivileged,
		c.OpConfig.Resources.SpiloAllowPrivilegeEscalation,
		generateCapabilities(c.OpConfig.AdditionalPodCapabilities),
	)

	// generate container specs for sidecars specified in the cluster manifest
	clusterSpecificSidecars := []v1.Container{}
	if spec.Sidecars != nil && len(spec.Sidecars) > 0 {
		// warn if sidecars are defined, but globally disabled (does not apply to globally defined sidecars)
		if c.OpConfig.EnableSidecars != nil && !(*c.OpConfig.EnableSidecars) {
			c.logger.Warningf("sidecars specified but disabled in configuration - next statefulset creation would fail")
		}

		if clusterSpecificSidecars, err = generateSidecarContainers(spec.Sidecars, defaultResources, 0, c.logger); err != nil {
			return nil, fmt.Errorf("could not generate sidecar containers: %v", err)
		}
	}

	// decrapted way of providing global sidecars
	var globalSidecarContainersByDockerImage []v1.Container
	var globalSidecarsByDockerImage []acidv1.Sidecar
	for name, dockerImage := range c.OpConfig.SidecarImages {
		globalSidecarsByDockerImage = append(globalSidecarsByDockerImage, acidv1.Sidecar{Name: name, DockerImage: dockerImage})
	}
	if globalSidecarContainersByDockerImage, err = generateSidecarContainers(globalSidecarsByDockerImage, defaultResources, len(clusterSpecificSidecars), c.logger); err != nil {
		return nil, fmt.Errorf("could not generate sidecar containers: %v", err)
	}
	// make the resulting list reproducible
	// c.OpConfig.SidecarImages is unsorted by Golang definition
	// .Name is unique
	sort.Slice(globalSidecarContainersByDockerImage, func(i, j int) bool {
		return globalSidecarContainersByDockerImage[i].Name < globalSidecarContainersByDockerImage[j].Name
	})

	// generate scalyr sidecar container
	var scalyrSidecars []v1.Container
	if scalyrSidecar, err :=
		generateScalyrSidecarSpec(c.Name,
			c.OpConfig.ScalyrAPIKey,
			c.OpConfig.ScalyrServerURL,
			c.OpConfig.ScalyrImage,
			c.OpConfig.ScalyrCPURequest,
			c.OpConfig.ScalyrMemoryRequest,
			c.OpConfig.ScalyrCPULimit,
			c.OpConfig.ScalyrMemoryLimit,
			defaultResources,
			c.logger); err != nil {
		return nil, fmt.Errorf("could not generate Scalyr sidecar: %v", err)
	} else {
		if scalyrSidecar != nil {
			scalyrSidecars = append(scalyrSidecars, *scalyrSidecar)
		}
	}

	sidecarContainers, conflicts := mergeContainers(clusterSpecificSidecars, c.Config.OpConfig.SidecarContainers, globalSidecarContainersByDockerImage, scalyrSidecars)
	for containerName := range conflicts {
		c.logger.Warningf("a sidecar is specified twice. Ignoring sidecar %q in favor of %q with high a precendence",
			containerName, containerName)
	}

	sidecarContainers = patchSidecarContainers(sidecarContainers, volumeMounts, c.OpConfig.SuperUsername, c.credentialSecretName(c.OpConfig.SuperUsername), c.logger)

	tolerationSpec := tolerations(&spec.Tolerations, c.OpConfig.PodToleration)
	effectivePodPriorityClassName := util.Coalesce(spec.PodPriorityClassName, c.OpConfig.PodPriorityClassName)

	podAnnotations := c.generatePodAnnotations(spec)

	// generate pod template for the statefulset, based on the spilo container and sidecars
	podTemplate, err = c.generatePodTemplate(
		c.Namespace,
		c.labelsSet(true),
		c.annotationsSet(podAnnotations),
		spiloContainer,
		initContainers,
		sidecarContainers,
		&tolerationSpec,
		effectiveRunAsUser,
		effectiveRunAsGroup,
		effectiveFSGroup,
		nodeAffinity(c.OpConfig.NodeReadinessLabel, spec.NodeAffinity),
		spec.SchedulerName,
		int64(c.OpConfig.PodTerminateGracePeriod.Seconds()),
		c.OpConfig.PodServiceAccountName,
		c.OpConfig.KubeIAMRole,
		effectivePodPriorityClassName,
		mountShmVolumeNeeded(c.OpConfig, spec),
		c.OpConfig.EnablePodAntiAffinity,
		c.OpConfig.PodAntiAffinityTopologyKey,
		c.OpConfig.AdditionalSecretMount,
		c.OpConfig.AdditionalSecretMountPath,
		additionalVolumes)

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
	updateStrategy := appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}

	var podManagementPolicy appsv1.PodManagementPolicyType
	if c.OpConfig.PodManagementPolicy == "ordered_ready" {
		podManagementPolicy = appsv1.OrderedReadyPodManagement
	} else if c.OpConfig.PodManagementPolicy == "parallel" {
		podManagementPolicy = appsv1.ParallelPodManagement
	} else {
		return nil, fmt.Errorf("could not set the pod management policy to the unknown value: %v", c.OpConfig.PodManagementPolicy)
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        c.statefulSetName(),
			Namespace:   c.Namespace,
			Labels:      c.labelsSet(true),
			Annotations: c.AnnotationsToPropagate(c.annotationsSet(nil)),
		},
		Spec: appsv1.StatefulSetSpec{
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

func (c *Cluster) generatePodAnnotations(spec *acidv1.PostgresSpec) map[string]string {
	annotations := make(map[string]string)
	for k, v := range c.OpConfig.CustomPodAnnotations {
		annotations[k] = v
	}
	if spec != nil || spec.PodAnnotations != nil {
		for k, v := range spec.PodAnnotations {
			annotations[k] = v
		}
	}

	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

func generateScalyrSidecarSpec(clusterName, APIKey, serverURL, dockerImage string,
	scalyrCPURequest string, scalyrMemoryRequest string, scalyrCPULimit string, scalyrMemoryLimit string,
	defaultResources acidv1.Resources, logger *logrus.Entry) (*v1.Container, error) {
	if APIKey == "" || dockerImage == "" {
		if APIKey == "" && dockerImage != "" {
			logger.Warning("Not running Scalyr sidecar: SCALYR_API_KEY must be defined")
		}
		return nil, nil
	}
	resourcesScalyrSidecar := makeResources(
		scalyrCPURequest,
		scalyrMemoryRequest,
		scalyrCPULimit,
		scalyrMemoryLimit,
	)
	resourceRequirementsScalyrSidecar, err := generateResourceRequirements(resourcesScalyrSidecar, defaultResources)
	if err != nil {
		return nil, fmt.Errorf("invalid resources for Scalyr sidecar: %v", err)
	}
	env := []v1.EnvVar{
		{
			Name:  "SCALYR_API_KEY",
			Value: APIKey,
		},
		{
			Name:  "SCALYR_SERVER_HOST",
			Value: clusterName,
		},
	}
	if serverURL != "" {
		env = append(env, v1.EnvVar{Name: "SCALYR_SERVER_URL", Value: serverURL})
	}
	return &v1.Container{
		Name:            "scalyr-sidecar",
		Image:           dockerImage,
		Env:             env,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *resourceRequirementsScalyrSidecar,
	}, nil
}

func (c *Cluster) getNumberOfInstances(spec *acidv1.PostgresSpec) int32 {
	min := c.OpConfig.MinInstances
	max := c.OpConfig.MaxInstances
	cur := spec.NumberOfInstances
	newcur := cur

	if spec.StandbyCluster != nil {
		if newcur == 1 {
			min = newcur
			max = newcur
		} else {
			c.logger.Warningf("operator only supports standby clusters with 1 pod")
		}
	}
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

	postgresContainerIdx := 0

	volumes := append(podSpec.Volumes, v1.Volume{
		Name: constants.ShmVolumeName,
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{
				Medium: "Memory",
			},
		},
	})

	for i, container := range podSpec.Containers {
		if container.Name == constants.PostgresContainerName {
			postgresContainerIdx = i
		}
	}

	mounts := append(podSpec.Containers[postgresContainerIdx].VolumeMounts,
		v1.VolumeMount{
			Name:      constants.ShmVolumeName,
			MountPath: constants.ShmVolumePath,
		})

	podSpec.Containers[postgresContainerIdx].VolumeMounts = mounts

	podSpec.Volumes = volumes
}

func addSecretVolume(podSpec *v1.PodSpec, additionalSecretMount string, additionalSecretMountPath string) {
	volumes := append(podSpec.Volumes, v1.Volume{
		Name: additionalSecretMount,
		VolumeSource: v1.VolumeSource{
			Secret: &v1.SecretVolumeSource{
				SecretName: additionalSecretMount,
			},
		},
	})

	for i := range podSpec.Containers {
		mounts := append(podSpec.Containers[i].VolumeMounts,
			v1.VolumeMount{
				Name:      additionalSecretMount,
				MountPath: additionalSecretMountPath,
			})
		podSpec.Containers[i].VolumeMounts = mounts
	}

	podSpec.Volumes = volumes
}

func (c *Cluster) addAdditionalVolumes(podSpec *v1.PodSpec,
	additionalVolumes []acidv1.AdditionalVolume) {

	volumes := podSpec.Volumes
	mountPaths := map[string]acidv1.AdditionalVolume{}
	for i, additionalVolume := range additionalVolumes {
		if previousVolume, exist := mountPaths[additionalVolume.MountPath]; exist {
			msg := "Volume %+v cannot be mounted to the same path as %+v"
			c.logger.Warningf(msg, additionalVolume, previousVolume)
			continue
		}

		if additionalVolume.MountPath == constants.PostgresDataMount {
			msg := "Cannot mount volume on postgresql data directory, %+v"
			c.logger.Warningf(msg, additionalVolume)
			continue
		}

		// if no target container is defined assign it to postgres container
		if len(additionalVolume.TargetContainers) == 0 {
			postgresContainer := getPostgresContainer(podSpec)
			additionalVolumes[i].TargetContainers = []string{postgresContainer.Name}
		}

		for _, target := range additionalVolume.TargetContainers {
			if target == "all" && len(additionalVolume.TargetContainers) != 1 {
				msg := `Target containers could be either "all" or a list
						of containers, mixing those is not allowed, %+v`
				c.logger.Warningf(msg, additionalVolume)
				continue
			}
		}

		volumes = append(volumes,
			v1.Volume{
				Name:         additionalVolume.Name,
				VolumeSource: additionalVolume.VolumeSource,
			},
		)

		mountPaths[additionalVolume.MountPath] = additionalVolume
	}

	c.logger.Infof("Mount additional volumes: %+v", additionalVolumes)

	for i := range podSpec.Containers {
		mounts := podSpec.Containers[i].VolumeMounts
		for _, additionalVolume := range additionalVolumes {
			for _, target := range additionalVolume.TargetContainers {
				if podSpec.Containers[i].Name == target || target == "all" {
					mounts = append(mounts, v1.VolumeMount{
						Name:      additionalVolume.Name,
						MountPath: additionalVolume.MountPath,
						SubPath:   additionalVolume.SubPath,
					})
				}
			}
		}
		podSpec.Containers[i].VolumeMounts = mounts
	}

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
		secret := c.generateSingleUserSecret(pgUser.Namespace, pgUser)
		if secret != nil {
			secrets[username] = secret
		}
		namespace = pgUser.Namespace
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

	//skip NOLOGIN users
	for _, flag := range pgUser.Flags {
		if flag == constants.RoleFlagNoLogin {
			return nil
		}
	}

	username := pgUser.Name
	lbls := c.labelsSet(true)

	if username == constants.ConnectionPoolerUserName {
		lbls = c.connectionPoolerLabels("", false).MatchLabels
	}

	secret := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        c.credentialSecretName(username),
			Namespace:   pgUser.Namespace,
			Labels:      lbls,
			Annotations: c.annotationsSet(nil),
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
	serviceSpec := v1.ServiceSpec{
		Ports: []v1.ServicePort{{Name: "postgresql", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}}},
		Type:  v1.ServiceTypeClusterIP,
	}

	if role == Replica || c.patroniKubernetesUseConfigMaps() {
		serviceSpec.Selector = c.roleLabelsSet(false, role)
	}

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
		serviceSpec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyType(c.OpConfig.ExternalTrafficPolicy)
		serviceSpec.Type = v1.ServiceTypeLoadBalancer
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
			Annotations: c.annotationsSet(c.generateServiceAnnotations(role, spec)),
		},
		Spec: serviceSpec,
	}

	return service
}

func (c *Cluster) generateServiceAnnotations(role PostgresRole, spec *acidv1.PostgresSpec) map[string]string {
	annotations := make(map[string]string)

	for k, v := range c.OpConfig.CustomServiceAnnotations {
		annotations[k] = v
	}
	if spec != nil || spec.ServiceAnnotations != nil {
		for k, v := range spec.ServiceAnnotations {
			annotations[k] = v
		}
	}

	if c.shouldCreateLoadBalancerForService(role, spec) {
		var dnsName string
		if role == Master {
			dnsName = c.masterDNSName()
		} else {
			dnsName = c.replicaDNSName()
		}

		// Just set ELB Timeout annotation with default value, if it does not
		// have a cutom value
		if _, ok := annotations[constants.ElbTimeoutAnnotationName]; !ok {
			annotations[constants.ElbTimeoutAnnotationName] = constants.ElbTimeoutAnnotationValue
		}
		// External DNS name annotation is not customizable
		annotations[constants.ZalandoDNSNameAnnotation] = dnsName
	}

	if len(annotations) == 0 {
		return nil
	}

	return annotations
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

			if c.OpConfig.WALES3Bucket != "" {
				envs := []v1.EnvVar{
					{
						Name:  "CLONE_WAL_S3_BUCKET",
						Value: c.OpConfig.WALES3Bucket,
					},
				}
				result = append(result, envs...)
			} else if c.OpConfig.WALGSBucket != "" {
				envs := []v1.EnvVar{
					{
						Name:  "CLONE_WAL_GS_BUCKET",
						Value: c.OpConfig.WALGSBucket,
					},
					{
						Name:  "CLONE_GOOGLE_APPLICATION_CREDENTIALS",
						Value: c.OpConfig.GCPCredentials,
					},
				}
				result = append(result, envs...)
			} else {
				c.logger.Error("Cannot figure out S3 or GS bucket. Both are empty.")
			}

			envs := []v1.EnvVar{
				{
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

func (c *Cluster) generateStandbyEnvironment(description *acidv1.StandbyDescription) []v1.EnvVar {
	result := make([]v1.EnvVar, 0)

	if description.S3WalPath == "" {
		msg := "Figure out which S3 bucket to use from env"
		c.logger.Info(msg, description.S3WalPath)

		if c.OpConfig.WALES3Bucket != "" {
			envs := []v1.EnvVar{
				{
					Name:  "STANDBY_WAL_S3_BUCKET",
					Value: c.OpConfig.WALES3Bucket,
				},
			}
			result = append(result, envs...)
		} else if c.OpConfig.WALGSBucket != "" {
			envs := []v1.EnvVar{
				{
					Name:  "STANDBY_WAL_GS_BUCKET",
					Value: c.OpConfig.WALGSBucket,
				},
				{
					Name:  "STANDBY_GOOGLE_APPLICATION_CREDENTIALS",
					Value: c.OpConfig.GCPCredentials,
				},
			}
			result = append(result, envs...)
		} else {
			c.logger.Error("Cannot figure out S3 or GS bucket. Both are empty.")
		}

		// TODO add UID support for standby
		/*
			envs := []v1.EnvVar{
				{
					Name:  "CLONE_WAL_BUCKET_SCOPE_SUFFIX",
					Value: getBucketScopeSuffix(description.UID),
				},
			}

			result = append(result, envs...)
		*/

	} else {
		// standby with S3, find out the bucket to setup standby
		msg := "Standby from S3 bucket using custom parsed S3WalPath from the manifest %s "
		c.logger.Infof(msg, description.S3WalPath)

		result = append(result, v1.EnvVar{
			Name:  "STANDBY_WALE_S3_PREFIX",
			Value: description.S3WalPath,
		})
	}

	result = append(result, v1.EnvVar{Name: "STANDBY_METHOD", Value: "STANDBY_WITH_WALE"})
	result = append(result, v1.EnvVar{Name: "STANDBY_WAL_BUCKET_SCOPE_PREFIX", Value: ""})

	return result
}

func (c *Cluster) generatePodDisruptionBudget() *policybeta1.PodDisruptionBudget {
	minAvailable := intstr.FromInt(1)
	pdbEnabled := c.OpConfig.EnablePodDisruptionBudget

	// if PodDisruptionBudget is disabled or if there are no DB pods, set the budget to 0.
	if (pdbEnabled != nil && !(*pdbEnabled)) || c.Spec.NumberOfInstances <= 0 {
		minAvailable = intstr.FromInt(0)
	}

	return &policybeta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:        c.podDisruptionBudgetName(),
			Namespace:   c.Namespace,
			Labels:      c.labelsSet(true),
			Annotations: c.annotationsSet(nil),
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
		c.OpConfig.SpiloAllowPrivilegeEscalation,
		nil,
	)

	labels := map[string]string{
		c.OpConfig.ClusterNameLabel: c.Name,
		"application":               "spilo-logical-backup",
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

	annotations := c.generatePodAnnotations(&c.Spec)

	// re-use the method that generates DB pod templates
	if podTemplate, err = c.generatePodTemplate(
		c.Namespace,
		labels,
		annotations,
		logicalBackupContainer,
		[]v1.Container{},
		[]v1.Container{},
		&[]v1.Toleration{},
		nil,
		nil,
		nil,
		nodeAffinity(c.OpConfig.NodeReadinessLabel, nil),
		nil,
		int64(c.OpConfig.PodTerminateGracePeriod.Seconds()),
		c.OpConfig.PodServiceAccountName,
		c.OpConfig.KubeIAMRole,
		"",
		util.False(),
		false,
		"",
		c.OpConfig.AdditionalSecretMount,
		c.OpConfig.AdditionalSecretMountPath,
		[]acidv1.AdditionalVolume{}); err != nil {
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
			Name:        c.getLogicalBackupJobName(),
			Namespace:   c.Namespace,
			Labels:      c.labelsSet(true),
			Annotations: c.annotationsSet(nil),
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
		{
			Name:  "CLUSTER_NAME_LABEL",
			Value: c.OpConfig.ClusterNameLabel,
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
		// Bucket env vars
		{
			Name:  "LOGICAL_BACKUP_PROVIDER",
			Value: c.OpConfig.LogicalBackup.LogicalBackupProvider,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3Bucket,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_REGION",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3Region,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_ENDPOINT",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3Endpoint,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_SSE",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3SSE,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX",
			Value: getBucketScopeSuffix(string(c.Postgresql.GetUID())),
		},
		{
			Name:  "LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS",
			Value: c.OpConfig.LogicalBackup.LogicalBackupGoogleApplicationCredentials,
		},
		// Postgres env vars
		{
			Name:  "PG_VERSION",
			Value: c.Spec.PostgresqlParam.PgVersion,
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

	if c.OpConfig.LogicalBackup.LogicalBackupS3AccessKeyID != "" {
		envVars = append(envVars, v1.EnvVar{Name: "AWS_ACCESS_KEY_ID", Value: c.OpConfig.LogicalBackup.LogicalBackupS3AccessKeyID})
	}

	if c.OpConfig.LogicalBackup.LogicalBackupS3SecretAccessKey != "" {
		envVars = append(envVars, v1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", Value: c.OpConfig.LogicalBackup.LogicalBackupS3SecretAccessKey})
	}

	c.logger.Debugf("Generated logical backup env vars")
	c.logger.Debugf("%v", envVars)
	return envVars
}

// getLogicalBackupJobName returns the name; the job itself may not exists
func (c *Cluster) getLogicalBackupJobName() (jobName string) {
	return trimCronjobName(c.OpConfig.LogicalBackupJobPrefix + c.clusterName().Name)
}

// Return an array of ownerReferences to make an arbitraty object dependent on
// the StatefulSet. Dependency is made on StatefulSet instead of PostgreSQL CRD
// while the former is represent the actual state, and only it's deletion means
// we delete the cluster (e.g. if CRD was deleted, StatefulSet somehow
// survived, we can't delete an object because it will affect the functioning
// cluster).
func (c *Cluster) ownerReferences() []metav1.OwnerReference {
	controller := true

	if c.Statefulset == nil {
		c.logger.Warning("Cannot get owner reference, no statefulset")
		return []metav1.OwnerReference{}
	}

	return []metav1.OwnerReference{
		{
			UID:        c.Statefulset.ObjectMeta.UID,
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
			Name:       c.Statefulset.ObjectMeta.Name,
			Controller: &controller,
		},
	}
}

func ensurePath(file string, defaultDir string, defaultFile string) string {
	if file == "" {
		return path.Join(defaultDir, defaultFile)
	}
	if !path.IsAbs(file) {
		return path.Join(defaultDir, file)
	}
	return file
}
