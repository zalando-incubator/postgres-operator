package cluster

import (
	"fmt"
	"sort"

	"encoding/json"
	"k8s.io/client-go/pkg/api/resource"
	"k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/pkg/apis/apps/v1beta1"
	"k8s.io/client-go/pkg/util/intstr"

	"github.com/zalando-incubator/postgres-operator/pkg/spec"
	"github.com/zalando-incubator/postgres-operator/pkg/util/constants"
)

const (
	pgBinariesLocationTemplate       = "/usr/lib/postgresql/%s/bin"
	patroniPGBinariesParameterName   = "bin_dir"
	patroniPGParametersParameterName = "parameters"
	localHost                        = "127.0.0.1/32"
)

type pgUser struct {
	Password string   `json:"password"`
	Options  []string `json:"options"`
}

type patroniDCS struct {
	TTL                  uint32  `json:"ttl,omitempty"`
	LoopWait             uint32  `json:"loop_wait,omitempty"`
	RetryTimeout         uint32  `json:"retry_timeout,omitempty"`
	MaximumLagOnFailover float32 `json:"maximum_lag_on_failover,omitempty"`
}

type pgBootstrap struct {
	Initdb []interface{}     `json:"initdb"`
	Users  map[string]pgUser `json:"users"`
	PgHBA  []string          `json:"pg_hba"`
	DCS    patroniDCS        `json:"dcs,omitempty"`
}

type spiloConfiguration struct {
	PgLocalConfiguration map[string]interface{} `json:"postgresql"`
	Bootstrap            pgBootstrap            `json:"bootstrap"`
}

func (c *Cluster) resourceRequirements(resources spec.Resources) (*v1.ResourceRequirements, error) {
	var err error

	specRequests := resources.ResourceRequest
	specLimits := resources.ResourceLimits

	config := c.OpConfig

	defaultRequests := spec.ResourceDescription{CPU: config.DefaultCPURequest, Memory: config.DefaultMemoryRequest}
	defaultLimits := spec.ResourceDescription{CPU: config.DefaultCPULimit, Memory: config.DefaultMemoryLimit}

	result := v1.ResourceRequirements{}

	result.Requests, err = fillResourceList(specRequests, defaultRequests)
	if err != nil {
		return nil, fmt.Errorf("could not fill resource requests: %v", err)
	}

	result.Limits, err = fillResourceList(specLimits, defaultLimits)
	if err != nil {
		return nil, fmt.Errorf("could not fill resource limits: %v", err)
	}

	return &result, nil
}

func fillResourceList(spec spec.ResourceDescription, defaults spec.ResourceDescription) (v1.ResourceList, error) {
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

func (c *Cluster) generateSpiloJSONConfiguration(pg *spec.PostgresqlParam, patroni *spec.Patroni) string {
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
PATRONI_INITDB_PARAMS:
	for _, k := range initdbOptionNames {
		v := patroni.InitDB[k]
		for i, defaultParam := range config.Bootstrap.Initdb {
			switch defaultParam.(type) {
			case map[string]string:
				{
					for k1 := range defaultParam.(map[string]string) {
						if k1 == k {
							(config.Bootstrap.Initdb[i]).(map[string]string)[k] = v
							continue PATRONI_INITDB_PARAMS
						}
					}
				}
			case string:
				{
					/* if the option already occurs in the list */
					if defaultParam.(string) == v {
						continue PATRONI_INITDB_PARAMS
					}
				}
			default:
				c.logger.Warnf("Unsupported type for initdb configuration item %s: %T", defaultParam)
				continue PATRONI_INITDB_PARAMS
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

	// pg_hba parameters in the manifest replace the default ones. We cannot
	// reasonably merge them automatically, because pg_hba parsing stops on
	// a first successfully matched rule.
	if len(patroni.PgHba) > 0 {
		config.Bootstrap.PgHBA = patroni.PgHba
	} else {
		config.Bootstrap.PgHBA = []string{
			"hostnossl all all all reject",
			fmt.Sprintf("hostssl   all +%s all pam", c.OpConfig.PamRoleName),
			"hostssl   all all all md5",
		}
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

	config.PgLocalConfiguration = make(map[string]interface{})
	config.PgLocalConfiguration[patroniPGBinariesParameterName] = fmt.Sprintf(pgBinariesLocationTemplate, pg.PgVersion)
	if len(pg.Parameters) > 0 {
		config.PgLocalConfiguration[patroniPGParametersParameterName] = pg.Parameters
	}
	config.Bootstrap.Users = map[string]pgUser{
		c.OpConfig.PamRoleName: {
			Password: "",
			Options:  []string{constants.RoleFlagCreateDB, constants.RoleFlagNoLogin},
		},
	}
	result, err := json.Marshal(config)
	if err != nil {
		c.logger.Errorf("Cannot convert spilo configuration into JSON: %s", err)
		return ""
	}
	return string(result)
}

func (c *Cluster) genPodTemplate(resourceRequirements *v1.ResourceRequirements, pgParameters *spec.PostgresqlParam, patroniParameters *spec.Patroni) *v1.PodTemplateSpec {
	spiloConfiguration := c.generateSpiloJSONConfiguration(pgParameters, patroniParameters)

	envVars := []v1.EnvVar{
		{
			Name:  "SCOPE",
			Value: c.Metadata.Name,
		},
		{
			Name:  "PGROOT",
			Value: constants.PostgresDataPath,
		},
		{
			Name:  "ETCD_HOST",
			Value: c.OpConfig.EtcdHost,
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
	}
	if spiloConfiguration != "" {
		envVars = append(envVars, v1.EnvVar{Name: "SPILO_CONFIGURATION", Value: spiloConfiguration})
	}
	if c.OpConfig.WALES3Bucket != "" {
		envVars = append(envVars, v1.EnvVar{Name: "WAL_S3_BUCKET", Value: c.OpConfig.WALES3Bucket})
	}
	privilegedMode := bool(true)
	container := v1.Container{
		Name:            c.Metadata.Name,
		Image:           c.OpConfig.DockerImage,
		ImagePullPolicy: v1.PullAlways,
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
		VolumeMounts: []v1.VolumeMount{
			{
				Name:      constants.DataVolumeName,
				MountPath: constants.PostgresDataMount, //TODO: fetch from manifesto
			},
		},
		Env: envVars,
		SecurityContext: &v1.SecurityContext{
			Privileged: &privilegedMode,
		},
	}
	terminateGracePeriodSeconds := int64(30)

	podSpec := v1.PodSpec{
		ServiceAccountName:            c.OpConfig.ServiceAccountName,
		TerminationGracePeriodSeconds: &terminateGracePeriodSeconds,
		Containers:                    []v1.Container{container},
	}

	template := v1.PodTemplateSpec{
		ObjectMeta: v1.ObjectMeta{
			Labels:    c.labelsSet(),
			Namespace: c.Metadata.Name,
		},
		Spec: podSpec,
	}
	if c.OpConfig.KubeIAMRole != "" {
		template.Annotations = map[string]string{constants.KubeIAmAnnotation: c.OpConfig.KubeIAMRole}
	}

	return &template
}

func (c *Cluster) genStatefulSet(spec spec.PostgresSpec) (*v1beta1.StatefulSet, error) {
	resourceRequirements, err := c.resourceRequirements(spec.Resources)
	if err != nil {
		return nil, err
	}

	podTemplate := c.genPodTemplate(resourceRequirements, &spec.PostgresqlParam, &spec.Patroni)
	volumeClaimTemplate, err := persistentVolumeClaimTemplate(spec.Volume.Size, spec.Volume.StorageClass)
	if err != nil {
		return nil, err
	}

	statefulSet := &v1beta1.StatefulSet{
		ObjectMeta: v1.ObjectMeta{
			Name:      c.Metadata.Name,
			Namespace: c.Metadata.Namespace,
			Labels:    c.labelsSet(),
		},
		Spec: v1beta1.StatefulSetSpec{
			Replicas:             &spec.NumberOfInstances,
			ServiceName:          c.Metadata.Name,
			Template:             *podTemplate,
			VolumeClaimTemplates: []v1.PersistentVolumeClaim{*volumeClaimTemplate},
		},
	}

	return statefulSet, nil
}

func persistentVolumeClaimTemplate(volumeSize, volumeStorageClass string) (*v1.PersistentVolumeClaim, error) {
	metadata := v1.ObjectMeta{
		Name: constants.DataVolumeName,
	}
	if volumeStorageClass != "" {
		// TODO: check if storage class exists
		metadata.Annotations = map[string]string{"volume.beta.kubernetes.io/storage-class": volumeStorageClass}
	} else {
		metadata.Annotations = map[string]string{"volume.alpha.kubernetes.io/storage-class": "default"}
	}

	quantity, err := resource.ParseQuantity(volumeSize)
	if err != nil {
		return nil, fmt.Errorf("could not parse volume size: %v", err)
	}

	volumeClaim := &v1.PersistentVolumeClaim{
		ObjectMeta: metadata,
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceStorage: quantity,
				},
			},
		},
	}

	return volumeClaim, nil
}

func (c *Cluster) genUserSecrets() (secrets map[string]*v1.Secret) {
	secrets = make(map[string]*v1.Secret, len(c.pgUsers))
	namespace := c.Metadata.Namespace
	for username, pgUser := range c.pgUsers {
		//Skip users with no password i.e. human users (they'll be authenticated using pam)
		secret := c.genSingleUserSecret(namespace, pgUser)
		if secret != nil {
			secrets[username] = secret
		}
	}
	/* special case for the system user */
	for _, systemUser := range c.systemUsers {
		secret := c.genSingleUserSecret(namespace, systemUser)
		if secret != nil {
			secrets[systemUser.Name] = secret
		}
	}

	return
}

func (c *Cluster) genSingleUserSecret(namespace string, pgUser spec.PgUser) *v1.Secret {
	//Skip users with no password i.e. human users (they'll be authenticated using pam)
	if pgUser.Password == "" {
		return nil
	}
	username := pgUser.Name
	secret := v1.Secret{
		ObjectMeta: v1.ObjectMeta{
			Name:      c.credentialSecretName(username),
			Namespace: namespace,
			Labels:    c.labelsSet(),
		},
		Type: v1.SecretTypeOpaque,
		Data: map[string][]byte{
			"username": []byte(pgUser.Name),
			"password": []byte(pgUser.Password),
		},
	}
	return &secret
}

func (c *Cluster) genService(role PostgresRole, allowedSourceRanges []string) *v1.Service {

	dnsNameFunction := c.masterDnsName
	name := c.Metadata.Name
	if role == Replica {
		dnsNameFunction = c.replicaDnsName
		name = name + "-repl"
	}

	serviceSpec := v1.ServiceSpec{}
	var annotations map[string]string

	if c.OpConfig.EnableLoadBalancer {
		// safe default value: lock load balancer to only local address unless overriden explicitely.
		sourceRanges := []string{localHost}
		if len(allowedSourceRanges) >= 0 {
			sourceRanges = allowedSourceRanges
		}

		serviceSpec = v1.ServiceSpec{
			Type:  v1.ServiceTypeLoadBalancer,
			Ports: []v1.ServicePort{{Name: "postgresql", Port: 5432, TargetPort: intstr.IntOrString{IntVal: 5432}}},
			LoadBalancerSourceRanges: sourceRanges,
		}

		annotations = map[string]string{
			constants.ZalandoDNSNameAnnotation: dnsNameFunction(),
			constants.ElbTimeoutAnnotationName: constants.ElbTimeoutAnnotationValue,
		}

		if role == Replica {
			serviceSpec.Selector = map[string]string{c.OpConfig.PodRoleLabel: string(Replica)}
		}
	}

	service := &v1.Service{
		ObjectMeta: v1.ObjectMeta{
			Name:      name,
			Namespace: c.Metadata.Namespace,
			Labels:    c.roleLabelsSet(role),
			Annotations: annotations,
		},
		Spec: serviceSpec,
	}

	return service
}

func (c *Cluster) genMasterEndpoints() *v1.Endpoints {
	endpoints := &v1.Endpoints{
		ObjectMeta: v1.ObjectMeta{
			Name:      c.Metadata.Name,
			Namespace: c.Metadata.Namespace,
			Labels:    c.roleLabelsSet(Master),
		},
	}

	return endpoints
}
