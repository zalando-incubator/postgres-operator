package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/config"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/patroni"
	"github.com/zalando/postgres-operator/v2/pkg/util/retryutil"
)

const (
	pgBinariesLocationTemplate     = "/usr/lib/postgresql/%v/bin"
	patroniPGBinariesParameterName = "bin_dir"
	patroniPGHBAConfParameterName  = "pg_hba"
	localHost                      = "127.0.0.1/32"
	scalyrSidecarName              = "scalyr-sidecar"
	logicalBackupContainerName     = "logical-backup"
	connectionPoolerContainer      = "connection-pooler"
	pgPort                         = 5432
	operatorPort                   = 8080
)

type patroniDCS struct {
	TTL                      uint32                       `json:"ttl,omitempty"`
	LoopWait                 uint32                       `json:"loop_wait,omitempty"`
	RetryTimeout             uint32                       `json:"retry_timeout,omitempty"`
	MaximumLagOnFailover     float32                      `json:"maximum_lag_on_failover,omitempty"`
	SynchronousMode          bool                         `json:"synchronous_mode,omitempty"`
	SynchronousModeStrict    bool                         `json:"synchronous_mode_strict,omitempty"`
	SynchronousNodeCount     uint32                       `json:"synchronous_node_count,omitempty"`
	PGBootstrapConfiguration map[string]interface{}       `json:"postgresql,omitempty"`
	Slots                    map[string]map[string]string `json:"slots,omitempty"`
	FailsafeMode             *bool                        `json:"failsafe_mode,omitempty"`
}

type pgBootstrap struct {
	Initdb []interface{} `json:"initdb"`
	DCS    patroniDCS    `json:"dcs,omitempty"`
}

type spiloConfiguration struct {
	PgLocalConfiguration map[string]interface{} `json:"postgresql"`
	Bootstrap            pgBootstrap            `json:"bootstrap"`
}

func (c *Cluster) statefulSetName() string {
	return c.Name
}

func (c *Cluster) serviceName(role PostgresRole) string {
	name := c.Name
	switch role {
	case Replica:
		name = fmt.Sprintf("%s-%s", name, "repl")
	case Patroni:
		name = fmt.Sprintf("%s-%s", name, "config")
	}

	return name
}

func (c *Cluster) serviceAddress(role PostgresRole) string {
	service, exist := c.Services[role]

	if exist {
		return service.ObjectMeta.Name
	}

	defaultAddress := c.serviceName(role)
	c.logger.Warningf("No service for role %s - defaulting to %s", role, defaultAddress)
	return defaultAddress
}

func (c *Cluster) servicePort(role PostgresRole) int32 {
	service, exist := c.Services[role]

	if exist {
		return service.Spec.Ports[0].Port
	}

	c.logger.Warningf("No service for role %s - defaulting to port %d", role, pgPort)
	return pgPort
}

func (c *Cluster) PrimaryPodDisruptionBudgetName() string {
	return c.OpConfig.PDBNameFormat.Format("cluster", c.Name)
}

func (c *Cluster) criticalOpPodDisruptionBudgetName() string {
	pdbTemplate := config.StringTemplate("postgres-{cluster}-critical-op-pdb")
	return pdbTemplate.Format("cluster", c.Name)
}

func makeDefaultResources(config *config.Config) acidv1.Resources {

	defaultRequests := acidv1.ResourceDescription{
		CPU:    &config.Resources.DefaultCPURequest,
		Memory: &config.Resources.DefaultMemoryRequest,
	}
	defaultLimits := acidv1.ResourceDescription{
		CPU:    &config.Resources.DefaultCPULimit,
		Memory: &config.Resources.DefaultMemoryLimit,
	}

	return acidv1.Resources{
		ResourceRequests: defaultRequests,
		ResourceLimits:   defaultLimits,
	}
}

func makeLogicalBackupResources(config *config.Config) acidv1.Resources {

	logicalBackupResourceRequests := acidv1.ResourceDescription{
		CPU:    &config.LogicalBackup.LogicalBackupCPURequest,
		Memory: &config.LogicalBackup.LogicalBackupMemoryRequest,
	}
	logicalBackupResourceLimits := acidv1.ResourceDescription{
		CPU:    &config.LogicalBackup.LogicalBackupCPULimit,
		Memory: &config.LogicalBackup.LogicalBackupMemoryLimit,
	}

	return acidv1.Resources{
		ResourceRequests: logicalBackupResourceRequests,
		ResourceLimits:   logicalBackupResourceLimits,
	}
}

func (c *Cluster) enforceMinResourceLimits(resources *v1.ResourceRequirements) error {
	var (
		isSmaller bool
		err       error
		msg       string
	)

	// setting limits too low can cause unnecessary evictions / OOM kills
	cpuLimit := resources.Limits[v1.ResourceCPU]
	minCPULimit := c.OpConfig.MinCPULimit
	if minCPULimit != "" {
		isSmaller, err = util.IsSmallerQuantity(cpuLimit.String(), minCPULimit)
		if err != nil {
			return fmt.Errorf("could not compare defined CPU limit %s for %q container with configured minimum value %s: %v",
				cpuLimit.String(), constants.PostgresContainerName, minCPULimit, err)
		}
		if isSmaller {
			msg = fmt.Sprintf("defined CPU limit %s for %q container is below required minimum %s and will be increased",
				cpuLimit.String(), constants.PostgresContainerName, minCPULimit)
			c.logger.Warningf("%s", msg)
			c.eventRecorder.Event(c.GetReference(), v1.EventTypeWarning, "ResourceLimits", msg)
			resources.Limits[v1.ResourceCPU], _ = resource.ParseQuantity(minCPULimit)
		}
	}

	memoryLimit := resources.Limits[v1.ResourceMemory]
	minMemoryLimit := c.OpConfig.MinMemoryLimit
	if minMemoryLimit != "" {
		isSmaller, err = util.IsSmallerQuantity(memoryLimit.String(), minMemoryLimit)
		if err != nil {
			return fmt.Errorf("could not compare defined memory limit %s for %q container with configured minimum value %s: %v",
				memoryLimit.String(), constants.PostgresContainerName, minMemoryLimit, err)
		}
		if isSmaller {
			msg = fmt.Sprintf("defined memory limit %s for %q container is below required minimum %s and will be increased",
				memoryLimit.String(), constants.PostgresContainerName, minMemoryLimit)
			c.logger.Warningf("%s", msg)
			c.eventRecorder.Event(c.GetReference(), v1.EventTypeWarning, "ResourceLimits", msg)
			resources.Limits[v1.ResourceMemory], _ = resource.ParseQuantity(minMemoryLimit)
		}
	}

	return nil
}

func (c *Cluster) enforceMaxResourceRequests(resources *v1.ResourceRequirements) error {
	var (
		err error
	)

	cpuRequest := resources.Requests[v1.ResourceCPU]
	maxCPURequest := c.OpConfig.MaxCPURequest
	maxCPU, err := util.MinResource(maxCPURequest, cpuRequest.String())
	if err != nil {
		return fmt.Errorf("could not compare defined CPU request %s for %q container with configured maximum value %s: %v",
			cpuRequest.String(), constants.PostgresContainerName, maxCPURequest, err)
	}
	if !maxCPU.IsZero() {
		resources.Requests[v1.ResourceCPU] = maxCPU
	}

	memoryRequest := resources.Requests[v1.ResourceMemory]
	maxMemoryRequest := c.OpConfig.MaxMemoryRequest
	maxMemory, err := util.MinResource(maxMemoryRequest, memoryRequest.String())
	if err != nil {
		return fmt.Errorf("could not compare defined memory request %s for %q container with configured maximum value %s: %v",
			memoryRequest.String(), constants.PostgresContainerName, maxMemoryRequest, err)
	}
	if !maxMemory.IsZero() {
		resources.Requests[v1.ResourceMemory] = maxMemory
	}

	return nil
}

func setMemoryRequestToLimit(resources *v1.ResourceRequirements, containerName string, logger *logrus.Entry) {

	requests := resources.Requests[v1.ResourceMemory]
	limits := resources.Limits[v1.ResourceMemory]
	isSmaller := requests.Cmp(limits) == -1
	if isSmaller {
		logger.Warningf("memory request of %s for %q container is increased to match memory limit of %s",
			requests.String(), containerName, limits.String())
		resources.Requests[v1.ResourceMemory] = limits
	}
}

func matchLimitsWithRequestsIfSmaller(resources *v1.ResourceRequirements, containerName string, logger *logrus.Entry) {
	requests := resources.Requests
	limits := resources.Limits
	requestCPU, cpuRequestsExists := requests[v1.ResourceCPU]
	limitCPU, cpuLimitExists := limits[v1.ResourceCPU]
	if cpuRequestsExists && cpuLimitExists && limitCPU.Cmp(requestCPU) == -1 {
		logger.Warningf("CPU limit of %s for %q container is increased to match CPU requests of %s", limitCPU.String(), containerName, requestCPU.String())
		resources.Limits[v1.ResourceCPU] = requestCPU
	}

	requestMemory, memoryRequestsExists := requests[v1.ResourceMemory]
	limitMemory, memoryLimitExists := limits[v1.ResourceMemory]
	if memoryRequestsExists && memoryLimitExists && limitMemory.Cmp(requestMemory) == -1 {
		logger.Warningf("memory limit of %s for %q container is increased to match memory requests of %s", limitMemory.String(), containerName, requestMemory.String())
		resources.Limits[v1.ResourceMemory] = requestMemory
	}
}

func fillResourceList(spec acidv1.ResourceDescription, defaults acidv1.ResourceDescription) (v1.ResourceList, error) {
	var err error
	requests := v1.ResourceList{}
	emptyResourceExamples := []string{"", "0", "null"}

	if spec.CPU != nil && !slices.Contains(emptyResourceExamples, *spec.CPU) {
		requests[v1.ResourceCPU], err = resource.ParseQuantity(*spec.CPU)
		if err != nil {
			return nil, fmt.Errorf("could not parse CPU quantity: %v", err)
		}
	} else {
		if defaults.CPU != nil && !slices.Contains(emptyResourceExamples, *defaults.CPU) {
			requests[v1.ResourceCPU], err = resource.ParseQuantity(*defaults.CPU)
			if err != nil {
				return nil, fmt.Errorf("could not parse default CPU quantity: %v", err)
			}
		}
	}
	if spec.Memory != nil && !slices.Contains(emptyResourceExamples, *spec.Memory) {
		requests[v1.ResourceMemory], err = resource.ParseQuantity(*spec.Memory)
		if err != nil {
			return nil, fmt.Errorf("could not parse memory quantity: %v", err)
		}
	} else {
		if defaults.Memory != nil && !slices.Contains(emptyResourceExamples, *defaults.Memory) {
			requests[v1.ResourceMemory], err = resource.ParseQuantity(*defaults.Memory)
			if err != nil {
				return nil, fmt.Errorf("could not parse default memory quantity: %v", err)
			}
		}
	}

	if spec.HugePages2Mi != nil {
		requests[v1.ResourceHugePagesPrefix+"2Mi"], err = resource.ParseQuantity(*spec.HugePages2Mi)
		if err != nil {
			return nil, fmt.Errorf("could not parse hugepages-2Mi quantity: %v", err)
		}
	}
	if spec.HugePages1Gi != nil {
		requests[v1.ResourceHugePagesPrefix+"1Gi"], err = resource.ParseQuantity(*spec.HugePages1Gi)
		if err != nil {
			return nil, fmt.Errorf("could not parse hugepages-1Gi quantity: %v", err)
		}
	}

	return requests, nil
}

func (c *Cluster) generateResourceRequirements(
	resources *acidv1.Resources,
	defaultResources acidv1.Resources,
	containerName string) (*v1.ResourceRequirements, error) {
	var err error
	specRequests := acidv1.ResourceDescription{}
	specLimits := acidv1.ResourceDescription{}
	result := v1.ResourceRequirements{}

	enforceThresholds := true
	resourcesLimitAnnotationKey := c.OpConfig.IgnoreResourcesLimitsAnnotationKey
	if resourcesLimitAnnotationKey != "" {
		if value, exists := c.ObjectMeta.Annotations[resourcesLimitAnnotationKey]; exists && value == "true" {
			enforceThresholds = false
		}
	}

	if resources != nil {
		specRequests = resources.ResourceRequests
		specLimits = resources.ResourceLimits
	}

	result.Requests, err = fillResourceList(specRequests, defaultResources.ResourceRequests)
	if err != nil {
		return nil, fmt.Errorf("could not fill resource requests: %v", err)
	}

	result.Limits, err = fillResourceList(specLimits, defaultResources.ResourceLimits)
	if err != nil {
		return nil, fmt.Errorf("could not fill resource limits: %v", err)
	}

	// enforce minimum cpu and memory limits for Postgres containers only
	if containerName == constants.PostgresContainerName && enforceThresholds {
		if err = c.enforceMinResourceLimits(&result); err != nil {
			return nil, fmt.Errorf("could not enforce minimum resource limits: %v", err)
		}
	}

	// make sure after reflecting default and enforcing min limit values we don't have requests > limits
	matchLimitsWithRequestsIfSmaller(&result, containerName, c.logger)

	// vice versa set memory requests to limit if option is enabled
	if c.OpConfig.SetMemoryRequestToLimit {
		setMemoryRequestToLimit(&result, containerName, c.logger)
	}

	// enforce maximum cpu and memory requests for Postgres containers only
	if containerName == constants.PostgresContainerName && enforceThresholds {
		if err = c.enforceMaxResourceRequests(&result); err != nil {
			return nil, fmt.Errorf("could not enforce maximum resource requests: %v", err)
		}
	}

	return &result, nil
}

func generateSpiloJSONConfiguration(pg *acidv1.PostgresqlParam, patroni *acidv1.Patroni, opConfig *config.Config, logger *logrus.Entry) (string, error) {
	config := spiloConfiguration{}

	config.Bootstrap = pgBootstrap{}

	config.Bootstrap.Initdb = []interface{}{map[string]string{"auth-host": "scram-sha-256"},
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
		config.Bootstrap.DCS.MaximumLagOnFailover = float32(patroni.MaximumLagOnFailover)
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
	if patroni.SynchronousNodeCount >= 1 {
		config.Bootstrap.DCS.SynchronousNodeCount = patroni.SynchronousNodeCount
	}
	if patroni.FailsafeMode != nil {
		config.Bootstrap.DCS.FailsafeMode = patroni.FailsafeMode
	} else if opConfig.EnablePatroniFailsafeMode != nil {
		config.Bootstrap.DCS.FailsafeMode = opConfig.EnablePatroniFailsafeMode
	}

	config.PgLocalConfiguration = make(map[string]interface{})

	// the newer and preferred way to specify the PG version is to use the `PGVERSION` env variable
	// setting postgresq.bin_dir in the SPILO_CONFIGURATION still works and takes precedence over PGVERSION
	// so we add postgresq.bin_dir only if PGVERSION is unused
	// see PR 222 in Spilo
	if !opConfig.EnablePgVersionEnvVar {
		config.PgLocalConfiguration[patroniPGBinariesParameterName] = fmt.Sprintf(pgBinariesLocationTemplate, pg.PgVersion)
	}
	if len(pg.Parameters) > 0 {
		local, bootstrap := getLocalAndBoostrapPostgreSQLParameters(pg.Parameters)

		if len(local) > 0 {
			config.PgLocalConfiguration[constants.PatroniPGParametersParameterName] = local
		}
		if len(bootstrap) > 0 {
			config.Bootstrap.DCS.PGBootstrapConfiguration = make(map[string]interface{})
			config.Bootstrap.DCS.PGBootstrapConfiguration[constants.PatroniPGParametersParameterName] = bootstrap
		}
	}
	// Patroni gives us a choice of writing pg_hba.conf to either the bootstrap section or to the local postgresql one.
	// We choose the local one, because we need Patroni to change pg_hba.conf in PostgreSQL after the user changes the
	// relevant section in the manifest.
	if len(patroni.PgHba) > 0 {
		config.PgLocalConfiguration[patroniPGHBAConfParameterName] = patroni.PgHba
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

func (c *Cluster) nodeAffinity(nodeReadinessLabel map[string]string, nodeAffinity *v1.NodeAffinity) *v1.Affinity {
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
			switch c.OpConfig.NodeReadinessLabelMerge {
			case "OR":
				manifestTerms := nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
				manifestTerms = append(manifestTerms, nodeReadinessSelectorTerm)
				nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{
					NodeSelectorTerms: manifestTerms,
				}
			case "AND":
				for i, nodeSelectorTerm := range nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
					manifestExpressions := nodeSelectorTerm.MatchExpressions
					manifestExpressions = append(manifestExpressions, matchExpressions...)
					nodeAffinityCopy.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i] = v1.NodeSelectorTerm{MatchExpressions: manifestExpressions}
				}
			}
		}
	}

	return &v1.Affinity{
		NodeAffinity: &nodeAffinityCopy,
	}
}

func podAffinity(
	labels labels.Set,
	topologyKey string,
	nodeAffinity *v1.Affinity,
	preferredDuringScheduling bool,
	anti bool) *v1.Affinity {

	var podAffinity v1.Affinity

	podAffinityTerm := v1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		TopologyKey: topologyKey,
	}

	if anti {
		podAffinity.PodAntiAffinity = generatePodAntiAffinity(podAffinityTerm, preferredDuringScheduling)
	} else {
		podAffinity.PodAffinity = generatePodAffinity(podAffinityTerm, preferredDuringScheduling)
	}

	if nodeAffinity != nil && nodeAffinity.NodeAffinity != nil {
		podAffinity.NodeAffinity = nodeAffinity.NodeAffinity
	}

	return &podAffinity
}

func generatePodAffinity(podAffinityTerm v1.PodAffinityTerm, preferredDuringScheduling bool) *v1.PodAffinity {
	podAffinity := &v1.PodAffinity{}

	if preferredDuringScheduling {
		podAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []v1.WeightedPodAffinityTerm{{
			Weight:          1,
			PodAffinityTerm: podAffinityTerm,
		}}
	} else {
		podAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []v1.PodAffinityTerm{podAffinityTerm}
	}

	return podAffinity
}

func generatePodAntiAffinity(podAffinityTerm v1.PodAffinityTerm, preferredDuringScheduling bool) *v1.PodAntiAffinity {
	podAntiAffinity := &v1.PodAntiAffinity{}

	if preferredDuringScheduling {
		podAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []v1.WeightedPodAffinityTerm{{
			Weight:          1,
			PodAffinityTerm: podAffinityTerm,
		}}
	} else {
		podAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []v1.PodAffinityTerm{podAffinityTerm}
	}

	return podAntiAffinity
}

func generateTopologySpreadConstraints(labels labels.Set, topologySpreadConstraints []v1.TopologySpreadConstraint) []v1.TopologySpreadConstraint {
	for _, topologySpreadConstraint := range topologySpreadConstraints {
		topologySpreadConstraint.LabelSelector = &metav1.LabelSelector{MatchLabels: labels}
	}
	return topologySpreadConstraints
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
	volumeMount := []v1.VolumeMount{
		{
			Name:      constants.DataVolumeName,
			MountPath: constants.PostgresDataMount, //TODO: fetch from manifest
		},
	}

	if volume.IsSubPathExpr != nil && *volume.IsSubPathExpr {
		volumeMount[0].SubPathExpr = volume.SubPath
	} else {
		volumeMount[0].SubPath = volume.SubPath
	}
	return volumeMount
}

func generateContainer(
	name string,
	dockerImage *string,
	resourceRequirements *v1.ResourceRequirements,
	envVars []v1.EnvVar,
	envFrom []v1.EnvFromSource,
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
				ContainerPort: patroni.ApiPort,
				Protocol:      v1.ProtocolTCP,
			},
			{
				ContainerPort: pgPort,
				Protocol:      v1.ProtocolTCP,
			},
			{
				ContainerPort: operatorPort,
				Protocol:      v1.ProtocolTCP,
			},
		},
		VolumeMounts: volumeMounts,
		Env:          envVars,
		EnvFrom:      envFrom,
		SecurityContext: &v1.SecurityContext{
			AllowPrivilegeEscalation: privilegeEscalationMode,
			Privileged:               &privilegedMode,
			ReadOnlyRootFilesystem:   util.False(),
			Capabilities:             additionalPodCapabilities,
		},
	}
}

func (c *Cluster) generateSidecarContainers(sidecars []acidv1.Sidecar,
	defaultResources acidv1.Resources, startIndex int) ([]v1.Container, error) {

	if len(sidecars) > 0 {
		result := make([]v1.Container, 0)
		for index, sidecar := range sidecars {
			var resourcesSpec acidv1.Resources
			if sidecar.Resources == nil {
				resourcesSpec = acidv1.Resources{}
			} else {
				sidecar.Resources.DeepCopyInto(&resourcesSpec)
			}

			resources, err := c.generateResourceRequirements(&resourcesSpec, defaultResources, sidecar.Name)
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
func patchSidecarContainers(in []v1.Container, volumeMounts []v1.VolumeMount, superUserName string, credentialsSecretName string) []v1.Container {
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
		container.Env = appendEnvVars(env, container.Env...)
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
	sharePgSocketWithSidecars *bool,
	topologySpreadConstraintsSpec []v1.TopologySpreadConstraint,
	tolerationsSpec *[]v1.Toleration,
	nodeAffinity *v1.Affinity,
	schedulerName *string,
	terminateGracePeriod int64,
	podServiceAccountName string,
	kubeIAMRole string,
	priorityClassName string,
	shmVolume *bool,
	podAntiAffinity bool,
	podAntiAffinityTopologyKey string,
	podAntiAffinityPreferredDuringScheduling bool,
	additionalSecretMount string,
	additionalSecretMountPath string,
	additionalVolumes []acidv1.AdditionalVolume,
) (*v1.PodTemplateSpec, error) {

	terminateGracePeriodSeconds := terminateGracePeriod
	containers := []v1.Container{*spiloContainer}
	containers = append(containers, sidecarContainers...)
	securityContext := v1.PodSecurityContext{
		RunAsUser:  c.OpConfig.Resources.SpiloRunAsUser,
		RunAsGroup: c.OpConfig.Resources.SpiloRunAsGroup,
		FSGroup:    c.OpConfig.Resources.SpiloFSGroup,
	}

	if c.Spec.SpiloRunAsUser != nil {
		securityContext.RunAsUser = c.Spec.SpiloRunAsUser
	}

	if c.Spec.SpiloRunAsGroup != nil {
		securityContext.RunAsGroup = c.Spec.SpiloRunAsGroup
	}

	if c.Spec.SpiloFSGroup != nil {
		securityContext.FSGroup = c.Spec.SpiloFSGroup
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
		podSpec.Affinity = podAffinity(
			labels,
			podAntiAffinityTopologyKey,
			nodeAffinity,
			podAntiAffinityPreferredDuringScheduling,
			true,
		)
	} else if nodeAffinity != nil {
		podSpec.Affinity = nodeAffinity
	}

	if priorityClassName != "" {
		podSpec.PriorityClassName = priorityClassName
	}

	podSpec.TopologySpreadConstraints = generateTopologySpreadConstraints(labels, topologySpreadConstraintsSpec)

	if sharePgSocketWithSidecars != nil && *sharePgSocketWithSidecars {
		addVarRunVolume(&podSpec)
	}

	if additionalSecretMount != "" {
		addSecretVolume(&podSpec, additionalSecretMount, additionalSecretMountPath)
	}

	if len(additionalVolumes) > 0 {
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
func (c *Cluster) generateSpiloPodEnvVars(
	spec *acidv1.PostgresSpec,
	uid types.UID,
	spiloConfiguration string) ([]v1.EnvVar, error) {

	// hard-coded set of environment variables we need
	// to guarantee core functionality of the operator
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

	if c.OpConfig.EnableSpiloWalPathCompat {
		envVars = append(envVars, v1.EnvVar{Name: "ENABLE_WAL_PATH_COMPAT", Value: "true"})
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
		if c.OpConfig.EnablePodDisruptionBudget != nil && *c.OpConfig.EnablePodDisruptionBudget {
			envVars = append(envVars, v1.EnvVar{Name: "KUBERNETES_BOOTSTRAP_LABELS", Value: "{\"critical-operation\":\"true\"}"})
		}
	} else {
		envVars = append(envVars, v1.EnvVar{Name: "ETCD_HOST", Value: c.OpConfig.EtcdHost})
	}

	if c.patroniKubernetesUseConfigMaps() {
		envVars = append(envVars, v1.EnvVar{Name: "KUBERNETES_USE_CONFIGMAPS", Value: "true"})
	}

	// fetch cluster-specific variables that will override all subsequent global variables
	if len(spec.Env) > 0 {
		envVars = appendEnvVars(envVars, spec.Env...)
	}

	if spec.Clone != nil && spec.Clone.ClusterName != "" {
		envVars = append(envVars, c.generateCloneEnvironment(spec.Clone)...)
	}

	if spec.StandbyCluster != nil {
		envVars = append(envVars, c.generateStandbyEnvironment(spec.StandbyCluster)...)
	}

	// fetch variables from custom environment Secret
	// that will override all subsequent global variables
	secretEnvVarsList, err := c.getPodEnvironmentSecretVariables()
	if err != nil {
		return nil, err
	}
	envVars = appendEnvVars(envVars, secretEnvVarsList...)

	// fetch variables from custom environment ConfigMap
	// that will override all subsequent global variables
	configMapEnvVarsList, err := c.getPodEnvironmentConfigMapVariables()
	if err != nil {
		return nil, err
	}
	envVars = appendEnvVars(envVars, configMapEnvVarsList...)

	// global variables derived from operator configuration
	opConfigEnvVars := make([]v1.EnvVar, 0)
	if c.OpConfig.WALES3Bucket != "" {
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_S3_BUCKET", Value: c.OpConfig.WALES3Bucket})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.WALGSBucket != "" {
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_GS_BUCKET", Value: c.OpConfig.WALGSBucket})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.WALAZStorageAccount != "" {
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "AZURE_STORAGE_ACCOUNT", Value: c.OpConfig.WALAZStorageAccount})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.GCPCredentials != "" {
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "GOOGLE_APPLICATION_CREDENTIALS", Value: c.OpConfig.GCPCredentials})
	}

	if c.OpConfig.LogS3Bucket != "" {
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "LOG_S3_BUCKET", Value: c.OpConfig.LogS3Bucket})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "LOG_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(string(uid))})
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "LOG_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.IRSARoleARN != "" {
		opConfigEnvVars = append(opConfigEnvVars, v1.EnvVar{Name: "SPILO_PROVIDER", Value: "aws"})
	}

	envVars = appendEnvVars(envVars, opConfigEnvVars...)

	return envVars, nil
}

func appendEnvVars(envs []v1.EnvVar, appEnv ...v1.EnvVar) []v1.EnvVar {
	collectedEnvs := envs
	for _, env := range appEnv {
		if !isEnvVarPresent(collectedEnvs, env.Name) {
			collectedEnvs = append(collectedEnvs, env)
		}
	}
	return collectedEnvs
}

func isEnvVarPresent(envs []v1.EnvVar, key string) bool {
	for _, env := range envs {
		if strings.EqualFold(env.Name, key) {
			return true
		}
	}
	return false
}

// Return list of variables the pod received from the configured ConfigMap
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
	sort.Slice(configMapPodEnvVarsList, func(i, j int) bool { return configMapPodEnvVarsList[i].Name < configMapPodEnvVarsList[j].Name })
	return configMapPodEnvVarsList, nil
}

// Return list of variables the pod received from the configured Secret
func (c *Cluster) getPodEnvironmentSecretVariables() ([]v1.EnvVar, error) {
	secretPodEnvVarsList := make([]v1.EnvVar, 0)

	if c.OpConfig.PodEnvironmentSecret == "" {
		return secretPodEnvVarsList, nil
	}

	secret := &v1.Secret{}
	var notFoundErr error
	err := retryutil.Retry(c.OpConfig.ResourceCheckInterval.Duration, c.OpConfig.ResourceCheckTimeout.Duration,
		func() (bool, error) {
			var err error
			secret, err = c.KubeClient.Secrets(c.Namespace).Get(
				context.TODO(),
				c.OpConfig.PodEnvironmentSecret,
				metav1.GetOptions{})
			if err != nil {
				if apierrors.IsNotFound(err) {
					notFoundErr = err
					return false, nil
				}
				return false, err
			}
			return true, nil
		},
	)
	if notFoundErr != nil && err != nil {
		err = errors.Wrap(notFoundErr, err.Error())
	}
	if err != nil {
		return nil, errors.Wrap(err, "could not read Secret PodEnvironmentSecretName")
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

	sort.Slice(secretPodEnvVarsList, func(i, j int) bool { return secretPodEnvVarsList[i].Name < secretPodEnvVarsList[j].Name })
	return secretPodEnvVarsList, nil
}

// Return list of variables the cronjob received from the configured Secret
func (c *Cluster) getCronjobEnvironmentSecretVariables() ([]v1.EnvVar, error) {
	secretCronjobEnvVarsList := make([]v1.EnvVar, 0)

	if c.OpConfig.LogicalBackupCronjobEnvironmentSecret == "" {
		return secretCronjobEnvVarsList, nil
	}

	secret, err := c.KubeClient.Secrets(c.Namespace).Get(
		context.TODO(),
		c.OpConfig.LogicalBackupCronjobEnvironmentSecret,
		metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not read Secret CronjobEnvironmentSecretName: %v", err)
	}

	for k := range secret.Data {
		secretCronjobEnvVarsList = append(secretCronjobEnvVarsList,
			v1.EnvVar{Name: k, ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: c.OpConfig.LogicalBackupCronjobEnvironmentSecret,
					},
					Key: k,
				},
			}})
	}

	return secretCronjobEnvVarsList, nil
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
		Command:         sidecar.Command,
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
			CPU:    &cpuRequest,
			Memory: &memoryRequest,
		},
		ResourceLimits: acidv1.ResourceDescription{
			CPU:    &cpuLimit,
			Memory: &memoryLimit,
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

func generateSpiloReadinessProbe() *v1.Probe {
	return &v1.Probe{
		FailureThreshold: 3,
		ProbeHandler: v1.ProbeHandler{
			HTTPGet: &v1.HTTPGetAction{
				Path:   "/readiness",
				Port:   intstr.IntOrString{IntVal: patroni.ApiPort},
				Scheme: v1.URISchemeHTTP,
			},
		},
		InitialDelaySeconds: 6,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		TimeoutSeconds:      5,
	}
}

func generateSpiloLivenessProbe(probe, defaultProbe *v1.Probe) *v1.Probe {

	if probe != nil {
		return probe
	}

	if defaultProbe != nil {
		return defaultProbe
	}

	return nil
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

	defaultResources := makeDefaultResources(&c.OpConfig)
	resourceRequirements, err := c.generateResourceRequirements(
		spec.Resources, defaultResources, constants.PostgresContainerName)
	if err != nil {
		return nil, fmt.Errorf("could not generate resource requirements: %v", err)
	}

	if len(spec.InitContainers) > 0 {
		if c.OpConfig.EnableInitContainers != nil && !(*c.OpConfig.EnableInitContainers) {
			c.logger.Warningf("initContainers specified but disabled in configuration - next statefulset creation would fail")
		}
		initContainers = spec.InitContainers
		if err := c.validateContainers(initContainers); err != nil {
			return nil, fmt.Errorf("invalid init containers: %v", err)
		}
	}

	spiloConfiguration, err := generateSpiloJSONConfiguration(&spec.PostgresqlParam, &spec.Patroni, &c.OpConfig, c.logger)
	if err != nil {
		return nil, fmt.Errorf("could not generate Spilo JSON configuration: %v", err)
	}

	// generate environment variables for the spilo container
	spiloEnvVars, err := c.generateSpiloPodEnvVars(spec, c.Postgresql.GetUID(), spiloConfiguration)
	if err != nil {
		return nil, fmt.Errorf("could not generate Spilo env vars: %v", err)
	}

	// pickup the docker image for the spilo container
	effectiveDockerImage := util.Coalesce(spec.DockerImage, c.OpConfig.DockerImage)

	volumeMounts := generateVolumeMounts(spec.Volume)

	// configure TLS with a custom secret volume
	if spec.TLS != nil && spec.TLS.SecretName != "" {
		getSpiloTLSEnv := func(k string) string {
			keyName := ""
			switch k {
			case "tls.crt":
				keyName = "SSL_CERTIFICATE_FILE"
			case "tls.key":
				keyName = "SSL_PRIVATE_KEY_FILE"
			case "tls.ca":
				keyName = "SSL_CA_FILE"
			default:
				panic(fmt.Sprintf("TLS env key unknown %s", k))
			}

			return keyName
		}
		tlsEnv, tlsVolumes := generateTlsMounts(spec, getSpiloTLSEnv)
		for _, env := range tlsEnv {
			spiloEnvVars = appendEnvVars(spiloEnvVars, env)
		}
		additionalVolumes = append(additionalVolumes, tlsVolumes...)
	}

	// generate the spilo container
	spiloContainer := generateContainer(constants.PostgresContainerName,
		&effectiveDockerImage,
		resourceRequirements,
		spiloEnvVars,
		spec.EnvFrom,
		volumeMounts,
		c.OpConfig.Resources.SpiloPrivileged,
		c.OpConfig.Resources.SpiloAllowPrivilegeEscalation,
		generateCapabilities(c.OpConfig.AdditionalPodCapabilities),
	)

	// Patroni responds 200 to probe only if it either owns the leader lock or postgres is running and DCS is accessible
	if c.OpConfig.EnableReadinessProbe {
		spiloContainer.ReadinessProbe = generateSpiloReadinessProbe()
	}

	spiloContainer.LivenessProbe = generateSpiloLivenessProbe(spec.LivenessProbe, c.OpConfig.LivenessProbe)

	// generate container specs for sidecars specified in the cluster manifest
	clusterSpecificSidecars := []v1.Container{}
	if len(spec.Sidecars) > 0 {
		// warn if sidecars are defined, but globally disabled (does not apply to globally defined sidecars)
		if c.OpConfig.EnableSidecars != nil && !(*c.OpConfig.EnableSidecars) {
			c.logger.Warningf("sidecars specified but disabled in configuration - next statefulset creation would fail")
		}

		if clusterSpecificSidecars, err = c.generateSidecarContainers(spec.Sidecars, defaultResources, 0); err != nil {
			return nil, fmt.Errorf("could not generate sidecar containers: %v", err)
		}
	}

	// decrapted way of providing global sidecars
	var globalSidecarContainersByDockerImage []v1.Container
	var globalSidecarsByDockerImage []acidv1.Sidecar
	for name, dockerImage := range c.OpConfig.SidecarImages {
		globalSidecarsByDockerImage = append(globalSidecarsByDockerImage, acidv1.Sidecar{Name: name, DockerImage: dockerImage})
	}
	if globalSidecarContainersByDockerImage, err = c.generateSidecarContainers(globalSidecarsByDockerImage, defaultResources, len(clusterSpecificSidecars)); err != nil {
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
		c.generateScalyrSidecarSpec(c.Name,
			c.OpConfig.ScalyrAPIKey,
			c.OpConfig.ScalyrServerURL,
			c.OpConfig.ScalyrImage,
			c.OpConfig.ScalyrCPURequest,
			c.OpConfig.ScalyrMemoryRequest,
			c.OpConfig.ScalyrCPULimit,
			c.OpConfig.ScalyrMemoryLimit,
			defaultResources); err != nil {
		return nil, fmt.Errorf("could not generate Scalyr sidecar: %v", err)
	} else {
		if scalyrSidecar != nil {
			scalyrSidecars = append(scalyrSidecars, *scalyrSidecar)
		}
	}

	sidecarContainers, conflicts := mergeContainers(clusterSpecificSidecars, c.Config.OpConfig.SidecarContainers, globalSidecarContainersByDockerImage, scalyrSidecars)
	for _, containerName := range conflicts {
		c.logger.Warningf("a sidecar is specified twice. Ignoring sidecar %q in favor of %q with high a precedence",
			containerName, containerName)
	}

	sidecarContainers = patchSidecarContainers(sidecarContainers, volumeMounts, c.OpConfig.SuperUsername, c.credentialSecretName(c.OpConfig.SuperUsername))

	if err := c.validateContainers(sidecarContainers); err != nil {
		return nil, fmt.Errorf("invalid sidecar containers: %v", err)
	}

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
		c.OpConfig.SharePgSocketWithSidecars,
		spec.TopologySpreadConstraints,
		&tolerationSpec,
		c.nodeAffinity(c.OpConfig.NodeReadinessLabel, spec.NodeAffinity),
		spec.SchedulerName,
		int64(util.CoalesceDuration(c.OpConfig.PodTerminateGracePeriod, "5m").Seconds()),
		c.OpConfig.PodServiceAccountName,
		c.OpConfig.KubeIAMRole,
		effectivePodPriorityClassName,
		mountShmVolumeNeeded(c.OpConfig, spec),
		c.OpConfig.EnablePodAntiAffinity,
		c.OpConfig.PodAntiAffinityTopologyKey,
		c.OpConfig.PodAntiAffinityPreferredDuringScheduling,
		c.OpConfig.AdditionalSecretMount,
		c.OpConfig.AdditionalSecretMountPath,
		additionalVolumes)

	if err != nil {
		return nil, fmt.Errorf("could not generate pod template: %v", err)
	}

	if volumeClaimTemplate, err = c.generatePersistentVolumeClaimTemplate(spec.Volume.Size,
		spec.Volume.StorageClass, spec.Volume.Selector); err != nil {
		return nil, fmt.Errorf("could not generate volume claim template: %v", err)
	}

	// global minInstances and maxInstances settings can overwrite manifest
	numberOfInstances := c.getNumberOfInstances(spec)

	// the operator has domain-specific logic on how to do rolling updates of PG clusters
	// so we do not use default rolling updates implemented by stateful sets
	// that leaves the legacy "OnDelete" update strategy as the only option
	updateStrategy := appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}

	var podManagementPolicy appsv1.PodManagementPolicyType
	switch c.OpConfig.PodManagementPolicy {
	case "ordered_ready":
		podManagementPolicy = appsv1.OrderedReadyPodManagement
	case "parallel":
		podManagementPolicy = appsv1.ParallelPodManagement
	default:
		return nil, fmt.Errorf("could not set the pod management policy to the unknown value: %v", c.OpConfig.PodManagementPolicy)
	}

	var persistentVolumeClaimRetentionPolicy appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy
	if c.OpConfig.PersistentVolumeClaimRetentionPolicy["when_deleted"] == "delete" {
		persistentVolumeClaimRetentionPolicy.WhenDeleted = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	} else {
		persistentVolumeClaimRetentionPolicy.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}

	if c.OpConfig.PersistentVolumeClaimRetentionPolicy["when_scaled"] == "delete" {
		persistentVolumeClaimRetentionPolicy.WhenScaled = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	} else {
		persistentVolumeClaimRetentionPolicy.WhenScaled = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.statefulSetName(),
			Namespace:       c.Namespace,
			Labels:          c.labelsSet(true),
			Annotations:     c.AnnotationsToPropagate(c.annotationsSet(nil)),
			OwnerReferences: c.ownerReferences(),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:                             &numberOfInstances,
			Selector:                             c.labelsSelector(),
			ServiceName:                          c.serviceName(Master),
			Template:                             *podTemplate,
			VolumeClaimTemplates:                 []v1.PersistentVolumeClaim{*volumeClaimTemplate},
			UpdateStrategy:                       updateStrategy,
			PodManagementPolicy:                  podManagementPolicy,
			PersistentVolumeClaimRetentionPolicy: &persistentVolumeClaimRetentionPolicy,
		},
	}

	return statefulSet, nil
}

func generateTlsMounts(spec *acidv1.PostgresSpec, tlsEnv func(key string) string) ([]v1.EnvVar, []acidv1.AdditionalVolume) {
	// this is combined with the FSGroup in the section above
	// to give read access to the postgres user
	defaultMode := int32(0640)
	mountPath := "/tls"
	env := make([]v1.EnvVar, 0)
	volumes := make([]acidv1.AdditionalVolume, 0)

	volumes = append(volumes, acidv1.AdditionalVolume{
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
	env = append(env, v1.EnvVar{Name: tlsEnv("tls.crt"), Value: certFile})
	env = append(env, v1.EnvVar{Name: tlsEnv("tls.key"), Value: privateKeyFile})

	if spec.TLS.CAFile != "" {
		// support scenario when the ca.crt resides in a different secret, diff path
		mountPathCA := mountPath
		if spec.TLS.CASecretName != "" {
			mountPathCA = mountPath + "ca"
		}

		caFile := ensurePath(spec.TLS.CAFile, mountPathCA, "")
		env = append(env, v1.EnvVar{Name: tlsEnv("tls.ca"), Value: caFile})

		// the ca file from CASecretName secret takes priority
		if spec.TLS.CASecretName != "" {
			volumes = append(volumes, acidv1.AdditionalVolume{
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

	return env, volumes
}

func (c *Cluster) generatePodAnnotations(spec *acidv1.PostgresSpec) map[string]string {
	annotations := make(map[string]string)
	for k, v := range c.OpConfig.CustomPodAnnotations {
		annotations[k] = v
	}
	if spec.PodAnnotations != nil {
		for k, v := range spec.PodAnnotations {
			annotations[k] = v
		}
	}

	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

func (c *Cluster) generateScalyrSidecarSpec(clusterName, APIKey, serverURL, dockerImage string,
	scalyrCPURequest string, scalyrMemoryRequest string, scalyrCPULimit string, scalyrMemoryLimit string,
	defaultResources acidv1.Resources) (*v1.Container, error) {
	if APIKey == "" || dockerImage == "" {
		if APIKey == "" && dockerImage != "" {
			c.logger.Warning("Not running Scalyr sidecar: SCALYR_API_KEY must be defined")
		}
		return nil, nil
	}
	resourcesScalyrSidecar := makeResources(
		scalyrCPURequest,
		scalyrMemoryRequest,
		scalyrCPULimit,
		scalyrMemoryLimit,
	)
	resourceRequirementsScalyrSidecar, err := c.generateResourceRequirements(
		&resourcesScalyrSidecar, defaultResources, scalyrSidecarName)
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
		Name:            scalyrSidecarName,
		Image:           dockerImage,
		Env:             env,
		ImagePullPolicy: v1.PullIfNotPresent,
		Resources:       *resourceRequirementsScalyrSidecar,
	}, nil
}

func (c *Cluster) getNumberOfInstances(spec *acidv1.PostgresSpec) int32 {
	min := c.OpConfig.MinInstances
	max := c.OpConfig.MaxInstances
	instanceLimitAnnotationKey := c.OpConfig.IgnoreInstanceLimitsAnnotationKey
	cur := spec.NumberOfInstances
	newcur := cur

	if instanceLimitAnnotationKey != "" {
		if value, exists := c.ObjectMeta.Annotations[instanceLimitAnnotationKey]; exists && value == "true" {
			return cur
		}
	}

	if isStandbyCluster(spec) {
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

func addVarRunVolume(podSpec *v1.PodSpec) {
	volumes := append(podSpec.Volumes, v1.Volume{
		Name: "postgresql-run",
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{
				Medium: "Memory",
			},
		},
	})

	for i := range podSpec.Containers {
		mounts := append(podSpec.Containers[i].VolumeMounts,
			v1.VolumeMount{
				Name:      constants.RunVolumeName,
				MountPath: constants.RunVolumePath,
			})
		podSpec.Containers[i].VolumeMounts = mounts
	}

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
			msg := "volume %+v cannot be mounted to the same path as %+v"
			c.logger.Warningf(msg, additionalVolume, previousVolume)
			continue
		}

		if additionalVolume.MountPath == constants.PostgresDataMount {
			msg := "cannot mount volume on postgresql data directory, %+v"
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
				msg := `target containers could be either "all" or a list
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
					v := v1.VolumeMount{
						Name:      additionalVolume.Name,
						MountPath: additionalVolume.MountPath,
					}

					if additionalVolume.IsSubPathExpr != nil && *additionalVolume.IsSubPathExpr {
						v.SubPathExpr = additionalVolume.SubPath
					} else {
						v.SubPath = additionalVolume.SubPath
					}

					mounts = append(mounts, v)
				}
			}
		}
		podSpec.Containers[i].VolumeMounts = mounts
	}

	podSpec.Volumes = volumes
}

func (c *Cluster) generatePersistentVolumeClaimTemplate(volumeSize, volumeStorageClass string,
	volumeSelector *metav1.LabelSelector) (*v1.PersistentVolumeClaim, error) {

	var storageClassName *string
	if volumeStorageClass != "" {
		storageClassName = &volumeStorageClass
	}

	quantity, err := resource.ParseQuantity(volumeSize)
	if err != nil {
		return nil, fmt.Errorf("could not parse volume size: %v", err)
	}

	volumeMode := v1.PersistentVolumeFilesystem
	volumeClaim := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        constants.DataVolumeName,
			Annotations: c.annotationsSet(nil),
			Labels:      c.labelsSet(true),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: quantity,
				},
			},
			StorageClassName: storageClassName,
			VolumeMode:       &volumeMode,
			Selector:         volumeSelector,
		},
	}

	return volumeClaim, nil
}

func (c *Cluster) generateUserSecrets() map[string]*v1.Secret {
	secrets := make(map[string]*v1.Secret, len(c.pgUsers)+len(c.systemUsers))
	for username, pgUser := range c.pgUsers {
		//Skip users with no password i.e. human users (they'll be authenticated using pam)
		secret := c.generateSingleUserSecret(pgUser)
		if secret != nil {
			secrets[username] = secret
		}
	}
	/* special case for the system user */
	for _, systemUser := range c.systemUsers {
		secret := c.generateSingleUserSecret(systemUser)
		if secret != nil {
			secrets[systemUser.Name] = secret
		}
	}

	return secrets
}

func (c *Cluster) generateSingleUserSecret(pgUser spec.PgUser) *v1.Secret {
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

	// Skip a controller ownerReference on user-credential secrets when the
	// operator is configured to keep them (enable_secrets_deletion=false);
	// otherwise Kubernetes garbage collection would still cascade-delete them
	// once the owning Postgresql CR is removed, defeating that setting.
	// Cross-namespace secrets also have no ownerReference because K8s forbids
	// cross-namespace ownerRefs by design.
	var ownerReferences []metav1.OwnerReference
	secretsDeletionDisabled := c.OpConfig.EnableSecretsDeletion != nil && !*c.OpConfig.EnableSecretsDeletion
	if secretsDeletionDisabled || (c.Config.OpConfig.EnableCrossNamespaceSecret && c.Postgresql.ObjectMeta.Namespace != pgUser.Namespace) {
		ownerReferences = nil
	} else {
		ownerReferences = c.ownerReferences()
	}

	secret := v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.credentialSecretName(username),
			Namespace:       pgUser.Namespace,
			Labels:          lbls,
			Annotations:     c.annotationsSet(nil),
			OwnerReferences: ownerReferences,
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

func (c *Cluster) shouldCreateNodePortForService(role PostgresRole, spec *acidv1.PostgresSpec) (bool, int32) {
	switch role {
	case Replica:
		// if the value is explicitly set in a Postgresql manifest, follow this setting
		if spec.EnableReplicaNodePort != nil {
			port := int32(0)
			if spec.ReplicaNodePort != nil {
				port = *spec.ReplicaNodePort
			}

			return *spec.EnableReplicaNodePort, port
		}

		// otherwise, follow the operator configuration
		return c.OpConfig.EnableReplicaNodePort, 0
	case Master:
		if spec.EnableMasterNodePort != nil {
			port := int32(0)
			if spec.MasterNodePort != nil {
				port = *spec.MasterNodePort
			}

			return *spec.EnableMasterNodePort, port
		}

		return c.OpConfig.EnableMasterNodePort, 0
	default:
		panic(fmt.Sprintf("Unknown role %v", role))
	}
}

func (c *Cluster) generateService(role PostgresRole, spec *acidv1.PostgresSpec) *v1.Service {
	serviceSpec := v1.ServiceSpec{
		Ports: []v1.ServicePort{{Name: "postgresql", Port: pgPort, TargetPort: intstr.IntOrString{IntVal: pgPort}}},
		Type:  v1.ServiceTypeClusterIP,
	}

	// no selector for master, see https://github.com/zalando/postgres-operator/issues/340
	// if kubernetes_use_configmaps is set master service needs a selector
	if role == Replica || c.patroniKubernetesUseConfigMaps() {
		serviceSpec.Selector = c.roleLabelsSet(false, role)
	}

	if ok, port := c.shouldCreateNodePortForService(role, spec); ok {
		c.configureNodePortService(&serviceSpec, port)
	} else if c.shouldCreateLoadBalancerForService(role, spec) {
		c.configureLoadBalanceService(&serviceSpec, spec.AllowedSourceRanges)
	}

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.serviceName(role),
			Namespace:       c.Namespace,
			Labels:          c.roleLabelsSet(true, role),
			Annotations:     c.annotationsSet(c.generateServiceAnnotations(role, spec)),
			OwnerReferences: c.ownerReferences(),
		},
		Spec: serviceSpec,
	}

	return service
}

func (c *Cluster) configureLoadBalanceService(serviceSpec *v1.ServiceSpec, sourceRanges []string) {
	// spec.AllowedSourceRanges evaluates to the empty slice of zero length
	// when omitted or set to 'null'/empty sequence in the PG manifest
	if len(sourceRanges) > 0 {
		serviceSpec.LoadBalancerSourceRanges = sourceRanges
	} else {
		// safe default value: lock a load balancer only to the local address unless overridden explicitly
		serviceSpec.LoadBalancerSourceRanges = []string{localHost}
	}

	c.logger.Debugf("final load balancer source ranges as seen in a service spec (not necessarily applied): %q", serviceSpec.LoadBalancerSourceRanges)
	serviceSpec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyType(c.OpConfig.ExternalTrafficPolicy)
	serviceSpec.Type = v1.ServiceTypeLoadBalancer
}

func (c *Cluster) configureNodePortService(serviceSpec *v1.ServiceSpec, port int32) {
	serviceSpec.ExternalTrafficPolicy = v1.ServiceExternalTrafficPolicyType(c.OpConfig.ExternalTrafficPolicy)
	serviceSpec.Type = v1.ServiceTypeNodePort

	if port != 0 && len(serviceSpec.Ports) > 0 {
		serviceSpec.Ports[0].NodePort = port
	}
}

func (c *Cluster) generateServiceAnnotations(role PostgresRole, spec *acidv1.PostgresSpec) map[string]string {
	annotations := c.getCustomServiceAnnotations(role, spec)

	nodePort, _ := c.shouldCreateNodePortForService(role, spec)

	if !nodePort && c.shouldCreateLoadBalancerForService(role, spec) {
		dnsName := c.dnsName(role)

		// External DNS name annotation is not customizable
		annotations[constants.ZalandoDNSNameAnnotation] = dnsName
	}

	if len(annotations) == 0 {
		return nil
	}

	return annotations
}

func (c *Cluster) getCustomServiceAnnotations(role PostgresRole, spec *acidv1.PostgresSpec) map[string]string {
	annotations := make(map[string]string)
	maps.Copy(annotations, c.OpConfig.CustomServiceAnnotations)

	if spec != nil {
		maps.Copy(annotations, spec.ServiceAnnotations)

		switch role {
		case Master:
			maps.Copy(annotations, spec.MasterServiceAnnotations)
		case Replica:
			maps.Copy(annotations, spec.ReplicaServiceAnnotations)
		}
	}

	return annotations
}

func (c *Cluster) generateEndpoint(role PostgresRole, subsets []v1.EndpointSubset) *v1.Endpoints {
	endpoints := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.serviceName(role),
			Namespace:       c.Namespace,
			Annotations:     c.annotationsSet(nil),
			Labels:          c.roleLabelsSet(true, role),
			OwnerReferences: c.ownerReferences(),
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
		c.logger.Infof("cloning with basebackup from %s", cluster)
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
		c.logger.Info("cloning from WAL location")
		if description.S3WalPath == "" {
			c.logger.Info("no S3 WAL path defined - taking value from global config", description.S3WalPath)

			if c.OpConfig.WALES3Bucket != "" {
				c.logger.Debugf("found WALES3Bucket %s - will set CLONE_WAL_S3_BUCKET", c.OpConfig.WALES3Bucket)
				result = append(result, v1.EnvVar{Name: "CLONE_WAL_S3_BUCKET", Value: c.OpConfig.WALES3Bucket})
			} else if c.OpConfig.WALGSBucket != "" {
				c.logger.Debugf("found WALGSBucket %s - will set CLONE_WAL_GS_BUCKET", c.OpConfig.WALGSBucket)
				result = append(result, v1.EnvVar{Name: "CLONE_WAL_GS_BUCKET", Value: c.OpConfig.WALGSBucket})
				if c.OpConfig.GCPCredentials != "" {
					result = append(result, v1.EnvVar{Name: "CLONE_GOOGLE_APPLICATION_CREDENTIALS", Value: c.OpConfig.GCPCredentials})
				}
			} else if c.OpConfig.WALAZStorageAccount != "" {
				c.logger.Debugf("found WALAZStorageAccount %s - will set CLONE_AZURE_STORAGE_ACCOUNT", c.OpConfig.WALAZStorageAccount)
				result = append(result, v1.EnvVar{Name: "CLONE_AZURE_STORAGE_ACCOUNT", Value: c.OpConfig.WALAZStorageAccount})
			} else {
				c.logger.Error("cannot figure out S3 or GS bucket or AZ storage account. All options are empty in the config.")
			}

			// append suffix because WAL location name is not the whole path
			result = append(result, v1.EnvVar{Name: "CLONE_WAL_BUCKET_SCOPE_SUFFIX", Value: getBucketScopeSuffix(description.UID)})
		} else {
			c.logger.Debugf("use S3WalPath %s from the manifest", description.S3WalPath)

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

	if c.OpConfig.IRSARoleARN != "" {
		result = append(result, v1.EnvVar{Name: "CLONE_AWS_ROLE_ARN", Value: c.OpConfig.IRSARoleARN})
		result = append(result, v1.EnvVar{Name: "CLONE_AWS_WEB_IDENTITY_TOKEN_FILE", Value: "/var/run/secrets/eks.amazonaws.com/serviceaccount/token"})
		result = append(result, v1.EnvVar{Name: "CLONE_AWS_REGION", Value: c.OpConfig.AWSRegion})
	}

	return result
}

func (c *Cluster) generateStandbyEnvironment(description *acidv1.StandbyDescription) []v1.EnvVar {
	result := make([]v1.EnvVar, 0)

	if description.StandbyHost != "" {
		c.logger.Info("standby cluster streaming from remote primary")
		result = append(result, v1.EnvVar{
			Name:  "STANDBY_HOST",
			Value: description.StandbyHost,
		})
		if description.StandbyPort != "" {
			result = append(result, v1.EnvVar{
				Name:  "STANDBY_PORT",
				Value: description.StandbyPort,
			})
		}
		if description.StandbyPrimarySlotName != "" {
			result = append(result, v1.EnvVar{
				Name:  "STANDBY_PRIMARY_SLOT_NAME",
				Value: description.StandbyPrimarySlotName,
			})
		}
	}

	// WAL archive can be specified with or without standby_host
	if description.S3WalPath != "" {
		c.logger.Info("standby cluster using S3 WAL archive")
		result = append(result, v1.EnvVar{
			Name:  "STANDBY_WALE_S3_PREFIX",
			Value: description.S3WalPath,
		})
		result = append(result, v1.EnvVar{Name: "STANDBY_METHOD", Value: "STANDBY_WITH_WALE"})
		result = append(result, v1.EnvVar{Name: "STANDBY_WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	} else if description.GSWalPath != "" {
		c.logger.Info("standby cluster using GCS WAL archive")
		result = append(result, v1.EnvVar{
			Name:  "STANDBY_WALE_GS_PREFIX",
			Value: description.GSWalPath,
		})
		result = append(result, v1.EnvVar{Name: "STANDBY_METHOD", Value: "STANDBY_WITH_WALE"})
		result = append(result, v1.EnvVar{Name: "STANDBY_WAL_BUCKET_SCOPE_PREFIX", Value: ""})
	}

	if c.OpConfig.IRSARoleARN != "" {
		result = append(result, v1.EnvVar{Name: "STANDBY_AWS_ROLE_ARN", Value: c.OpConfig.IRSARoleARN})
		result = append(result, v1.EnvVar{Name: "STANDBY_AWS_WEB_IDENTITY_TOKEN_FILE", Value: "/var/run/secrets/eks.amazonaws.com/serviceaccount/token"})
		result = append(result, v1.EnvVar{Name: "STANDBY_AWS_REGION", Value: c.OpConfig.AWSRegion})
	}

	return result
}

func (c *Cluster) generatePrimaryPodDisruptionBudget() *policyv1.PodDisruptionBudget {
	minAvailable := intstr.FromInt(1)
	pdbEnabled := c.OpConfig.EnablePodDisruptionBudget
	pdbMasterLabelSelector := c.OpConfig.PDBMasterLabelSelector

	// if PodDisruptionBudget is disabled or if there are more than one DB pods, set the budget to 0.
	if (pdbEnabled != nil && !(*pdbEnabled)) || c.Spec.NumberOfInstances <= 1 {
		minAvailable = intstr.FromInt(0)
	}

	// define label selector and add the master role selector if enabled
	labels := c.labelsSet(false)
	if pdbMasterLabelSelector == nil || *c.OpConfig.PDBMasterLabelSelector {
		labels[c.OpConfig.PodRoleLabel] = string(Master)
	}

	// When master selector is disabled and synchronous_mode_strict is on, require
	// master + synchronous_node_count (default 1) healthy pods for write quorum.
	if pdbMasterLabelSelector != nil && !*pdbMasterLabelSelector && minAvailable.IntVal > 0 && c.Spec.SynchronousModeStrict {
		minAvailable = intstr.FromInt32(int32(c.Spec.SynchronousNodeCount + 1))
	}

	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.PrimaryPodDisruptionBudgetName(),
			Namespace:       c.Namespace,
			Labels:          c.labelsSet(true),
			Annotations:     c.annotationsSet(nil),
			OwnerReferences: c.ownerReferences(),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		},
	}
}

func (c *Cluster) generateCriticalOpPodDisruptionBudget() *policyv1.PodDisruptionBudget {
	// MaxUnavailable: 0 blocks voluntary disruption of any pod carrying the
	// critical-operation label, while keeping the budget satisfied when no
	// pod matches (status.desiredHealthy stays 0 outside critical
	// operations). The previous MinAvailable: N spec left desiredHealthy at
	// N with zero matching pods during normal operation, permanently firing
	// alerts like kube-prometheus-stack's KubePdbNotEnoughHealthyPods (#3020).
	maxUnavailable := intstr.FromInt32(0)
	pdbEnabled := c.OpConfig.EnablePodDisruptionBudget

	// if PodDisruptionBudget is disabled or if there are no DB pods, allow all disruptions.
	if (pdbEnabled != nil && !(*pdbEnabled)) || c.Spec.NumberOfInstances <= 0 {
		maxUnavailable = intstr.FromString("100%")
	}

	labels := c.labelsSet(false)
	labels["critical-operation"] = "true"

	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.criticalOpPodDisruptionBudgetName(),
			Namespace:       c.Namespace,
			Labels:          c.labelsSet(true),
			Annotations:     c.annotationsSet(nil),
			OwnerReferences: c.ownerReferences(),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &maxUnavailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
		},
	}
}

// getClusterServiceConnectionParameters fetches cluster host name and port
// TODO: perhaps we need to query the service (i.e. if non-standard port is used?)
// TODO: handle clusters in different namespaces
func (c *Cluster) getClusterServiceConnectionParameters(clusterName string) (host string, port string) {
	host = clusterName
	port = fmt.Sprintf("%d", pgPort)
	return
}

func (c *Cluster) generateLogicalBackupJob() (*batchv1.CronJob, error) {

	var (
		err                  error
		podTemplate          *v1.PodTemplateSpec
		resourceRequirements *v1.ResourceRequirements
	)

	spec := &c.Spec

	// NB: a cron job creates standard batch jobs according to schedule; these batch jobs manage pods and clean-up

	c.logger.Debug("Generating logical backup pod template")

	// allocate configured resources for logical backup pod
	logicalBackupResources := makeLogicalBackupResources(&c.OpConfig)
	// if not defined only default resources from spilo pods are used
	resourceRequirements, err = c.generateResourceRequirements(
		&logicalBackupResources, makeDefaultResources(&c.OpConfig), logicalBackupContainerName)

	if err != nil {
		return nil, fmt.Errorf("could not generate resource requirements for logical backup pods: %v", err)
	}

	secretEnvVarsList, err := c.getCronjobEnvironmentSecretVariables()
	if err != nil {
		return nil, err
	}

	envVars := c.generateLogicalBackupPodEnvVars()
	envVars = append(envVars, secretEnvVarsList...)
	logicalBackupContainer := generateContainer(
		logicalBackupContainerName,
		&c.OpConfig.LogicalBackup.LogicalBackupDockerImage,
		resourceRequirements,
		envVars,
		nil,
		[]v1.VolumeMount{},
		c.OpConfig.SpiloPrivileged, // use same value as for normal DB pods
		c.OpConfig.SpiloAllowPrivilegeEscalation,
		nil,
	)

	logicalBackupJobLabel := map[string]string{
		"application": "spilo-logical-backup",
	}

	labels := labels.Merge(c.labelsSet(true), logicalBackupJobLabel)

	nodeAffinity := c.nodeAffinity(c.OpConfig.NodeReadinessLabel, nil)
	podAffinity := podAffinity(
		labels,
		"kubernetes.io/hostname",
		nodeAffinity,
		true,
		false,
	)

	annotations := c.generatePodAnnotations(&c.Spec)

	tolerationsSpec := tolerations(&spec.Tolerations, c.OpConfig.PodToleration)

	topologySpreadConstraintsSpec := generateTopologySpreadConstraints(labels, spec.TopologySpreadConstraints)

	// re-use the method that generates DB pod templates
	if podTemplate, err = c.generatePodTemplate(
		c.Namespace,
		labels,
		c.annotationsSet(annotations),
		logicalBackupContainer,
		[]v1.Container{},
		[]v1.Container{},
		util.False(),
		topologySpreadConstraintsSpec,
		&tolerationsSpec,
		c.nodeAffinity(c.OpConfig.NodeReadinessLabel, nil),
		nil,
		int64(util.CoalesceDuration(c.OpConfig.PodTerminateGracePeriod, "5m").Seconds()),
		c.OpConfig.PodServiceAccountName,
		c.OpConfig.KubeIAMRole,
		"",
		util.False(),
		false,
		"",
		false,
		c.OpConfig.AdditionalSecretMount,
		c.OpConfig.AdditionalSecretMountPath,
		[]acidv1.AdditionalVolume{}); err != nil {
		return nil, fmt.Errorf("could not generate pod template for logical backup pod: %v", err)
	}

	// overwrite specific params of logical backups pods
	podTemplate.Spec.Affinity = podAffinity
	podTemplate.Spec.RestartPolicy = "Never" // affects containers within a pod

	// configure a batch job

	jobSpec := batchv1.JobSpec{
		Template:                *podTemplate,
		TTLSecondsAfterFinished: c.OpConfig.LogicalBackup.LogicalBackupTTLSecondsAfterFinished,
	}

	if jobSpec.TTLSecondsAfterFinished == nil {
		defaultTTL := int32(86400)
		jobSpec.TTLSecondsAfterFinished = &defaultTTL
	}

	// configure a cron job

	jobTemplateSpec := batchv1.JobTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: c.annotationsSet(annotations),
		},
		Spec: jobSpec,
	}

	schedule := c.Postgresql.Spec.LogicalBackupSchedule
	if schedule == "" {
		schedule = c.OpConfig.LogicalBackupSchedule
	}

	successfulJobsHistoryLimit := c.OpConfig.LogicalBackup.LogicalBackupSuccessfulJobsHistoryLimit
	if successfulJobsHistoryLimit == nil {
		defaultLimit := int32(3)
		successfulJobsHistoryLimit = &defaultLimit
	}

	failedJobsHistoryLimit := c.OpConfig.LogicalBackup.LogicalBackupFailedJobsHistoryLimit
	if failedJobsHistoryLimit == nil {
		defaultLimit := int32(3)
		failedJobsHistoryLimit = &defaultLimit
	}

	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:            c.getLogicalBackupJobName(),
			Namespace:       c.Namespace,
			Labels:          labels,
			Annotations:     c.annotationsSet(annotations),
			OwnerReferences: c.ownerReferences(),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   schedule,
			JobTemplate:                jobTemplateSpec,
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			SuccessfulJobsHistoryLimit: successfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     failedJobsHistoryLimit,
		},
	}

	return cronJob, nil
}

func (c *Cluster) generateLogicalBackupPodEnvVars() []v1.EnvVar {

	backupProvider := c.OpConfig.LogicalBackup.LogicalBackupProvider

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
		// Postgres env vars
		{
			Name:  "PG_VERSION",
			Value: c.Spec.PostgresqlParam.PgVersion,
		},
		{
			Name:  "PGPORT",
			Value: fmt.Sprintf("%d", pgPort),
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
		// Bucket env vars
		{
			Name:  "LOGICAL_BACKUP_PROVIDER",
			Value: backupProvider,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3Bucket,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET_PREFIX",
			Value: c.OpConfig.LogicalBackup.LogicalBackupS3BucketPrefix,
		},
		{
			Name:  "LOGICAL_BACKUP_S3_BUCKET_SCOPE_SUFFIX",
			Value: getBucketScopeSuffix(string(c.Postgresql.GetUID())),
		},
	}

	switch backupProvider {
	case "s3":
		envVars = appendEnvVars(envVars, []v1.EnvVar{
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
				Name:  "LOGICAL_BACKUP_S3_RETENTION_TIME",
				Value: c.getLogicalBackupRetentionTime(),
			}}...)

		if c.OpConfig.LogicalBackup.LogicalBackupS3AccessKeyID != "" {
			envVars = append(envVars, v1.EnvVar{Name: "AWS_ACCESS_KEY_ID", Value: c.OpConfig.LogicalBackup.LogicalBackupS3AccessKeyID})
		}

		if c.OpConfig.LogicalBackup.LogicalBackupS3SecretAccessKey != "" {
			envVars = append(envVars, v1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", Value: c.OpConfig.LogicalBackup.LogicalBackupS3SecretAccessKey})
		}

	case "gcs":
		if c.OpConfig.LogicalBackup.LogicalBackupGoogleApplicationCredentials != "" {
			envVars = append(envVars, v1.EnvVar{Name: "LOGICAL_BACKUP_GOOGLE_APPLICATION_CREDENTIALS", Value: c.OpConfig.LogicalBackup.LogicalBackupGoogleApplicationCredentials})
		}

	case "az":
		envVars = appendEnvVars(envVars, []v1.EnvVar{
			{
				Name:  "LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_NAME",
				Value: c.OpConfig.LogicalBackup.LogicalBackupAzureStorageAccountName,
			},
			{
				Name:  "LOGICAL_BACKUP_AZURE_STORAGE_CONTAINER",
				Value: c.OpConfig.LogicalBackup.LogicalBackupAzureStorageContainer,
			}}...)

		if c.OpConfig.LogicalBackup.LogicalBackupAzureStorageAccountKey != "" {
			envVars = append(envVars, v1.EnvVar{Name: "LOGICAL_BACKUP_AZURE_STORAGE_ACCOUNT_KEY", Value: c.OpConfig.LogicalBackup.LogicalBackupAzureStorageAccountKey})
		}
	}

	return envVars
}

func (c *Cluster) getLogicalBackupRetentionTime() (retentionTime string) {
	if c.Spec.LogicalBackupRetention != "" {
		return c.Spec.LogicalBackupRetention
	}

	return c.OpConfig.LogicalBackup.LogicalBackupS3RetentionTime
}

// getLogicalBackupJobName returns the name; the job itself may not exists
func (c *Cluster) getLogicalBackupJobName() (jobName string) {
	return trimCronjobName(fmt.Sprintf("%s%s", c.OpConfig.LogicalBackupJobPrefix, c.clusterName().Name))
}

// Return an array of ownerReferences to make an arbitraty object dependent on
// the StatefulSet. Dependency is made on StatefulSet instead of PostgreSQL CRD
// while the former is represent the actual state, and only it's deletion means
// we delete the cluster (e.g. if CRD was deleted, StatefulSet somehow
// survived, we can't delete an object because it will affect the functioning
// cluster).
func (c *Cluster) ownerReferences() []metav1.OwnerReference {
	currentOwnerReferences := c.ObjectMeta.OwnerReferences
	if c.OpConfig.EnableOwnerReferences == nil || !*c.OpConfig.EnableOwnerReferences {
		return currentOwnerReferences
	}

	for _, ownerRef := range currentOwnerReferences {
		if ownerRef.UID == c.Postgresql.ObjectMeta.UID {
			return currentOwnerReferences
		}
	}

	controllerReference := metav1.OwnerReference{
		UID:        c.Postgresql.ObjectMeta.UID,
		APIVersion: acidv1.SchemeGroupVersion.Identifier(),
		Kind:       acidv1.PostgresCRDResourceKind,
		Name:       c.Postgresql.ObjectMeta.Name,
		Controller: util.True(),
	}

	return append(currentOwnerReferences, controllerReference)
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

func (c *Cluster) validateContainers(containers []v1.Container) error {
	for i, container := range containers {
		if container.Name == "" {
			return fmt.Errorf("container[%d]: name is required", i)
		}
		if container.Image == "" {
			return fmt.Errorf("container '%v': image is required", container.Name)
		}
	}
	return nil
}
