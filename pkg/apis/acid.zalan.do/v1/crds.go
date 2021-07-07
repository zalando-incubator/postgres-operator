package v1

import (
	acidzalando "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do"
	"github.com/zalando/postgres-operator/pkg/util"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CRDResource* define names necesssary for the k8s CRD API
const (
	PostgresCRDResourceKind   = "postgresql"
	PostgresCRDResourcePlural = "postgresqls"
	PostgresCRDResouceName    = PostgresCRDResourcePlural + "." + acidzalando.GroupName
	PostgresCRDResourceShort  = "pg"

	OperatorConfigCRDResouceKind    = "OperatorConfiguration"
	OperatorConfigCRDResourcePlural = "operatorconfigurations"
	OperatorConfigCRDResourceName   = OperatorConfigCRDResourcePlural + "." + acidzalando.GroupName
	OperatorConfigCRDResourceShort  = "opconfig"
)

// PostgresCRDResourceColumns definition of AdditionalPrinterColumns for postgresql CRD
var PostgresCRDResourceColumns = []apiextv1.CustomResourceColumnDefinition{
	{
		Name:        "Team",
		Type:        "string",
		Description: "Team responsible for Postgres cluster",
		JSONPath:    ".spec.teamId",
	},
	{
		Name:        "Version",
		Type:        "string",
		Description: "PostgreSQL version",
		JSONPath:    ".spec.postgresql.version",
	},
	{
		Name:        "Pods",
		Type:        "integer",
		Description: "Number of Pods per Postgres cluster",
		JSONPath:    ".spec.numberOfInstances",
	},
	{
		Name:        "Volume",
		Type:        "string",
		Description: "Size of the bound volume",
		JSONPath:    ".spec.volume.size",
	},
	{
		Name:        "CPU-Request",
		Type:        "string",
		Description: "Requested CPU for Postgres containers",
		JSONPath:    ".spec.resources.requests.cpu",
	},
	{
		Name:        "Memory-Request",
		Type:        "string",
		Description: "Requested memory for Postgres containers",
		JSONPath:    ".spec.resources.requests.memory",
	},
	{
		Name:     "Age",
		Type:     "date",
		JSONPath: ".metadata.creationTimestamp",
	},
	{
		Name:        "Status",
		Type:        "string",
		Description: "Current sync status of postgresql resource",
		JSONPath:    ".status.PostgresClusterStatus",
	},
}

// OperatorConfigCRDResourceColumns definition of AdditionalPrinterColumns for OperatorConfiguration CRD
var OperatorConfigCRDResourceColumns = []apiextv1.CustomResourceColumnDefinition{
	{
		Name:        "Image",
		Type:        "string",
		Description: "Spilo image to be used for Pods",
		JSONPath:    ".configuration.docker_image",
	},
	{
		Name:        "Cluster-Label",
		Type:        "string",
		Description: "Label for K8s resources created by operator",
		JSONPath:    ".configuration.kubernetes.cluster_name_label",
	},
	{
		Name:        "Service-Account",
		Type:        "string",
		Description: "Name of service account to be used",
		JSONPath:    ".configuration.kubernetes.pod_service_account_name",
	},
	{
		Name:        "Min-Instances",
		Type:        "integer",
		Description: "Minimum number of instances per Postgres cluster",
		JSONPath:    ".configuration.min_instances",
	},
	{
		Name:     "Age",
		Type:     "date",
		JSONPath: ".metadata.creationTimestamp",
	},
}

var min0 = 0.0
var min1 = 1.0
var minDisable = -1.0

// PostgresCRDResourceValidation to check applied manifest parameters
var PostgresCRDResourceValidation = apiextv1.CustomResourceValidation{
	OpenAPIV3Schema: &apiextv1.JSONSchemaProps{
		Type:     "object",
		Required: []string{"kind", "apiVersion", "spec"},
		Properties: map[string]apiextv1.JSONSchemaProps{
			"kind": {
				Type: "string",
				Enum: []apiextv1.JSON{
					{
						Raw: []byte(`"postgresql"`),
					},
				},
			},
			"apiVersion": {
				Type: "string",
				Enum: []apiextv1.JSON{
					{
						Raw: []byte(`"acid.zalan.do/v1"`),
					},
				},
			},
			"spec": {
				Type:     "object",
				Required: []string{"numberOfInstances", "teamId", "postgresql", "volume"},
				Properties: map[string]apiextv1.JSONSchemaProps{
					"additionalVolumes": {
						Type: "array",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:     "object",
								Required: []string{"name", "mountPath", "volumeSource"},
								Properties: map[string]apiextv1.JSONSchemaProps{
									"name": {
										Type: "string",
									},
									"mountPath": {
										Type: "string",
									},
									"targetContainers": {
										Type: "array",
										Items: &apiextv1.JSONSchemaPropsOrArray{
											Schema: &apiextv1.JSONSchemaProps{
												Type: "string",
											},
										},
									},
									"volumeSource": {
										Type:                   "object",
										XPreserveUnknownFields: util.True(),
									},
									"subPath": {
										Type: "string",
									},
								},
							},
						},
					},
					"allowedSourceRanges": {
						Type:     "array",
						Nullable: true,
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:    "string",
								Pattern: "^(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5])\\/(\\d|[1-2]\\d|3[0-2])$",
							},
						},
					},
					"clone": {
						Type:     "object",
						Required: []string{"cluster"},
						Properties: map[string]apiextv1.JSONSchemaProps{
							"cluster": {
								Type: "string",
							},
							"s3_endpoint": {
								Type: "string",
							},
							"s3_access_key_id": {
								Type: "string",
							},
							"s3_secret_access_key": {
								Type: "string",
							},
							"s3_force_path_style": {
								Type: "boolean",
							},
							"s3_wal_path": {
								Type: "string",
							},
							"timestamp": {
								Type:        "string",
								Description: "Date-time format that specifies a timezone as an offset relative to UTC e.g. 1996-12-19T16:39:57-08:00",
								Pattern:     "^([0-9]+)-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\\.[0-9]+)?(([+-]([01][0-9]|2[0-3]):[0-5][0-9]))$",
							},
							"uid": {
								Type:   "string",
								Format: "uuid",
							},
						},
					},
					"connectionPooler": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"dockerImage": {
								Type: "string",
							},
							"maxDBConnections": {
								Type: "integer",
							},
							"mode": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"session"`),
									},
									{
										Raw: []byte(`"transaction"`),
									},
								},
							},
							"numberOfInstances": {
								Type:    "integer",
								Minimum: &min1,
							},
							"resources": {
								Type:     "object",
								Required: []string{"requests", "limits"},
								Properties: map[string]apiextv1.JSONSchemaProps{
									"limits": {
										Type:     "object",
										Required: []string{"cpu", "memory"},
										Properties: map[string]apiextv1.JSONSchemaProps{
											"cpu": {
												Type:        "string",
												Description: "Decimal natural followed by m, or decimal natural followed by dot followed by up to three decimal digits (precision used by Kubernetes). Must be greater than 0",
												Pattern:     "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
											},
											"memory": {
												Type:        "string",
												Description: "Plain integer or fixed-point integer using one of these suffixes: E, P, T, G, M, k (with or without a tailing i). Must be greater than 0",
												Pattern:     "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
											},
										},
									},
									"requests": {
										Type:     "object",
										Required: []string{"cpu", "memory"},
										Properties: map[string]apiextv1.JSONSchemaProps{
											"cpu": {
												Type:        "string",
												Description: "Decimal natural followed by m, or decimal natural followed by dot followed by up to three decimal digits (precision used by Kubernetes). Must be greater than 0",
												Pattern:     "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
											},
											"memory": {
												Type:        "string",
												Description: "Plain integer or fixed-point integer using one of these suffixes: E, P, T, G, M, k (with or without a tailing i). Must be greater than 0",
												Pattern:     "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
											},
										},
									},
								},
							},
							"schema": {
								Type: "string",
							},
							"user": {
								Type: "string",
							},
						},
					},
					"databases": {
						Type: "object",
						AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
							Schema: &apiextv1.JSONSchemaProps{
								Type:        "string",
								Description: "User names specified here as database owners must be declared in the users key of the spec key",
							},
						},
					},
					"dockerImage": {
						Type: "string",
					},
					"enableConnectionPooler": {
						Type: "boolean",
					},
					"enableReplicaConnectionPooler": {
						Type: "boolean",
					},
					"enableLogicalBackup": {
						Type: "boolean",
					},
					"enableMasterLoadBalancer": {
						Type: "boolean",
					},
					"enableReplicaLoadBalancer": {
						Type: "boolean",
					},
					"enableShmVolume": {
						Type: "boolean",
					},
					"init_containers": {
						Type:        "array",
						Description: "Deprecated",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:                   "object",
								XPreserveUnknownFields: util.True(),
							},
						},
					},
					"initContainers": {
						Type: "array",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:                   "object",
								XPreserveUnknownFields: util.True(),
							},
						},
					},
					"logicalBackupSchedule": {
						Type:    "string",
						Pattern: "^(\\d+|\\*)(/\\d+)?(\\s+(\\d+|\\*)(/\\d+)?){4}$",
					},
					"maintenanceWindows": {
						Type: "array",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:    "string",
								Pattern: "^\\ *((Mon|Tue|Wed|Thu|Fri|Sat|Sun):(2[0-3]|[01]?\\d):([0-5]?\\d)|(2[0-3]|[01]?\\d):([0-5]?\\d))-((Mon|Tue|Wed|Thu|Fri|Sat|Sun):(2[0-3]|[01]?\\d):([0-5]?\\d)|(2[0-3]|[01]?\\d):([0-5]?\\d))\\ *$",
							},
						},
					},
					"numberOfInstances": {
						Type:    "integer",
						Minimum: &min0,
					},
					"patroni": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"initdb": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"loop_wait": {
								Type: "integer",
							},
							"maximum_lag_on_failover": {
								Type: "integer",
							},
							"pg_hba": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"retry_timeout": {
								Type: "integer",
							},
							"slots": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "object",
										AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
											Schema: &apiextv1.JSONSchemaProps{
												Type: "string",
											},
										},
									},
								},
							},
							"synchronous_mode": {
								Type: "boolean",
							},
							"synchronous_mode_strict": {
								Type: "boolean",
							},
							"ttl": {
								Type: "integer",
							},
						},
					},
					"podAnnotations": {
						Type: "object",
						AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
							Schema: &apiextv1.JSONSchemaProps{
								Type: "string",
							},
						},
					},
					"pod_priority_class_name": {
						Type:        "string",
						Description: "Deprecated",
					},
					"podPriorityClassName": {
						Type: "string",
					},
					"postgresql": {
						Type:     "object",
						Required: []string{"version"},
						Properties: map[string]apiextv1.JSONSchemaProps{
							"version": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"9.3"`),
									},
									{
										Raw: []byte(`"9.4"`),
									},
									{
										Raw: []byte(`"9.5"`),
									},
									{
										Raw: []byte(`"9.6"`),
									},
									{
										Raw: []byte(`"10"`),
									},
									{
										Raw: []byte(`"11"`),
									},
									{
										Raw: []byte(`"12"`),
									},
									{
										Raw: []byte(`"13"`),
									},
								},
							},
							"parameters": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
						},
					},
					"preparedDatabases": {
						Type: "object",
						AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
							Schema: &apiextv1.JSONSchemaProps{
								Type: "object",
								Properties: map[string]apiextv1.JSONSchemaProps{
									"defaultUsers": {
										Type: "boolean",
									},
									"extensions": {
										Type: "object",
										AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
											Schema: &apiextv1.JSONSchemaProps{
												Type: "string",
											},
										},
									},
									"schemas": {
										Type: "object",
										AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
											Schema: &apiextv1.JSONSchemaProps{
												Type: "object",
												Properties: map[string]apiextv1.JSONSchemaProps{
													"defaultUsers": {
														Type: "boolean",
													},
													"defaultRoles": {
														Type: "boolean",
													},
												},
											},
										},
									},
								},
							},
						},
					},
					"replicaLoadBalancer": {
						Type:        "boolean",
						Description: "Deprecated",
					},
					"resources": {
						Type:     "object",
						Required: []string{"requests", "limits"},
						Properties: map[string]apiextv1.JSONSchemaProps{
							"limits": {
								Type:     "object",
								Required: []string{"cpu", "memory"},
								Properties: map[string]apiextv1.JSONSchemaProps{
									"cpu": {
										Type:        "string",
										Description: "Decimal natural followed by m, or decimal natural followed by dot followed by up to three decimal digits (precision used by Kubernetes). Must be greater than 0",
										Pattern:     "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
									},
									"memory": {
										Type:        "string",
										Description: "Plain integer or fixed-point integer using one of these suffixes: E, P, T, G, M, k (with or without a tailing i). Must be greater than 0",
										Pattern:     "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
									},
								},
							},
							"requests": {
								Type:     "object",
								Required: []string{"cpu", "memory"},
								Properties: map[string]apiextv1.JSONSchemaProps{
									"cpu": {
										Type:        "string",
										Description: "Decimal natural followed by m, or decimal natural followed by dot followed by up to three decimal digits (precision used by Kubernetes). Must be greater than 0",
										Pattern:     "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
									},
									"memory": {
										Type:        "string",
										Description: "Plain integer or fixed-point integer using one of these suffixes: E, P, T, G, M, k (with or without a tailing i). Must be greater than 0",
										Pattern:     "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
									},
								},
							},
						},
					},
					"schedulerName": {
						Type: "string",
					},
					"serviceAnnotations": {
						Type: "object",
						AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
							Schema: &apiextv1.JSONSchemaProps{
								Type: "string",
							},
						},
					},
					"sidecars": {
						Type: "array",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:                   "object",
								XPreserveUnknownFields: util.True(),
							},
						},
					},
					"spiloRunAsUser": {
						Type: "integer",
					},
					"spiloRunAsGroup": {
						Type: "integer",
					},
					"spiloFSGroup": {
						Type: "integer",
					},
					"standby": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"cluster": {
								Type: "string",
							},
							"uid": {
								Type:   "string",
								Format: "uuid",
							},
							"version": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"9.3"`),
									},
									{
										Raw: []byte(`"9.4"`),
									},
									{
										Raw: []byte(`"9.5"`),
									},
									{
										Raw: []byte(`"9.6"`),
									},
									{
										Raw: []byte(`"10"`),
									},
									{
										Raw: []byte(`"11"`),
									},
									{
										Raw: []byte(`"12"`),
									},
									{
										Raw: []byte(`"13"`),
									},
								},
							},
							"s3_wal_path": {
								Type: "string",
							},
							"gs_wal_path": {
								Type: "string",
							},
						},
					},
					"teamId": {
						Type: "string",
					},
					"tls": {
						Type:     "object",
						Required: []string{"secretName"},
						Properties: map[string]apiextv1.JSONSchemaProps{
							"secretName": {
								Type: "string",
							},
							"certificateFile": {
								Type: "string",
							},
							"privateKeyFile": {
								Type: "string",
							},
							"caFile": {
								Type: "string",
							},
							"caSecretName": {
								Type: "string",
							},
						},
					},
					"nodeAffinity": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"preferredDuringSchedulingIgnoredDuringExecution": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type:     "object",
										Required: []string{"preference", "weight"},
										Properties: map[string]apiextv1.JSONSchemaProps{
											"preference": {
												Type: "object",
												Properties: map[string]apiextv1.JSONSchemaProps{
													"matchExpressions": {
														Type: "array",
														Items: &apiextv1.JSONSchemaPropsOrArray{
															Schema: &apiextv1.JSONSchemaProps{
																Type: "object",
																AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
																	Allows: true,
																},
															},
														},
													},
													"matchFields": {
														Type: "array",
														Items: &apiextv1.JSONSchemaPropsOrArray{
															Schema: &apiextv1.JSONSchemaProps{
																Type: "object",
																AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
																	Allows: true,
																},
															},
														},
													},
												},
											},
											"weight": {
												Type:   "integer",
												Format: "int32",
											},
										},
									},
								},
							},
							"requiredDuringSchedulingIgnoredDuringExecution": {
								Type:     "object",
								Required: []string{"nodeSelectorTerms"},
								Properties: map[string]apiextv1.JSONSchemaProps{
									"nodeSelectorTerms": {
										Type: "array",
										Items: &apiextv1.JSONSchemaPropsOrArray{
											Schema: &apiextv1.JSONSchemaProps{
												Type: "object",
												Properties: map[string]apiextv1.JSONSchemaProps{
													"matchExpressions": {
														Type: "array",
														Items: &apiextv1.JSONSchemaPropsOrArray{
															Schema: &apiextv1.JSONSchemaProps{
																Type: "object",
																AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
																	Allows: true,
																},
															},
														},
													},
													"matchFields": {
														Type: "array",
														Items: &apiextv1.JSONSchemaPropsOrArray{
															Schema: &apiextv1.JSONSchemaProps{
																Type: "object",
																AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
																	Allows: true,
																},
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
					"tolerations": {
						Type: "array",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:     "object",
								Required: []string{"key", "operator", "effect"},
								Properties: map[string]apiextv1.JSONSchemaProps{
									"key": {
										Type: "string",
									},
									"operator": {
										Type: "string",
										Enum: []apiextv1.JSON{
											{
												Raw: []byte(`"Equal"`),
											},
											{
												Raw: []byte(`"Exists"`),
											},
										},
									},
									"value": {
										Type: "string",
									},
									"effect": {
										Type: "string",
										Enum: []apiextv1.JSON{
											{
												Raw: []byte(`"NoExecute"`),
											},
											{
												Raw: []byte(`"NoSchedule"`),
											},
											{
												Raw: []byte(`"PreferNoSchedule"`),
											},
										},
									},
									"tolerationSeconds": {
										Type: "integer",
									},
								},
							},
						},
					},
					"useLoadBalancer": {
						Type:        "boolean",
						Description: "Deprecated",
					},
					"users": {
						Type: "object",
						AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
							Schema: &apiextv1.JSONSchemaProps{
								Type:        "array",
								Description: "Role flags specified here must not contradict each other",
								Nullable:    true,
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
										Enum: []apiextv1.JSON{
											{
												Raw: []byte(`"bypassrls"`),
											},
											{
												Raw: []byte(`"BYPASSRLS"`),
											},
											{
												Raw: []byte(`"nobypassrls"`),
											},
											{
												Raw: []byte(`"NOBYPASSRLS"`),
											},
											{
												Raw: []byte(`"createdb"`),
											},
											{
												Raw: []byte(`"CREATEDB"`),
											},
											{
												Raw: []byte(`"nocreatedb"`),
											},
											{
												Raw: []byte(`"NOCREATEDB"`),
											},
											{
												Raw: []byte(`"createrole"`),
											},
											{
												Raw: []byte(`"CREATEROLE"`),
											},
											{
												Raw: []byte(`"nocreaterole"`),
											},
											{
												Raw: []byte(`"NOCREATEROLE"`),
											},
											{
												Raw: []byte(`"inherit"`),
											},
											{
												Raw: []byte(`"INHERIT"`),
											},
											{
												Raw: []byte(`"noinherit"`),
											},
											{
												Raw: []byte(`"NOINHERIT"`),
											},
											{
												Raw: []byte(`"login"`),
											},
											{
												Raw: []byte(`"LOGIN"`),
											},
											{
												Raw: []byte(`"nologin"`),
											},
											{
												Raw: []byte(`"NOLOGIN"`),
											},
											{
												Raw: []byte(`"replication"`),
											},
											{
												Raw: []byte(`"REPLICATION"`),
											},
											{
												Raw: []byte(`"noreplication"`),
											},
											{
												Raw: []byte(`"NOREPLICATION"`),
											},
											{
												Raw: []byte(`"superuser"`),
											},
											{
												Raw: []byte(`"SUPERUSER"`),
											},
											{
												Raw: []byte(`"nosuperuser"`),
											},
											{
												Raw: []byte(`"NOSUPERUSER"`),
											},
										},
									},
								},
							},
						},
					},
					"volume": {
						Type:     "object",
						Required: []string{"size"},
						Properties: map[string]apiextv1.JSONSchemaProps{
							"iops": {
								Type: "integer",
							},
							"size": {
								Type:        "string",
								Description: "Value must not be zero",
								Pattern:     "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"storageClass": {
								Type: "string",
							},
							"subPath": {
								Type: "string",
							},
							"throughput": {
								Type: "integer",
							},
						},
					},
				},
			},
			"status": {
				Type: "object",
				AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
					Schema: &apiextv1.JSONSchemaProps{
						Type: "string",
					},
				},
			},
		},
	},
}

// OperatorConfigCRDResourceValidation to check applied manifest parameters
var OperatorConfigCRDResourceValidation = apiextv1.CustomResourceValidation{
	OpenAPIV3Schema: &apiextv1.JSONSchemaProps{
		Type:     "object",
		Required: []string{"kind", "apiVersion", "configuration"},
		Properties: map[string]apiextv1.JSONSchemaProps{
			"kind": {
				Type: "string",
				Enum: []apiextv1.JSON{
					{
						Raw: []byte(`"OperatorConfiguration"`),
					},
				},
			},
			"apiVersion": {
				Type: "string",
				Enum: []apiextv1.JSON{
					{
						Raw: []byte(`"acid.zalan.do/v1"`),
					},
				},
			},
			"configuration": {
				Type: "object",
				Properties: map[string]apiextv1.JSONSchemaProps{
					"docker_image": {
						Type: "string",
					},
					"enable_crd_validation": {
						Type: "boolean",
					},
					"enable_lazy_spilo_upgrade": {
						Type: "boolean",
					},
					"enable_shm_volume": {
						Type: "boolean",
					},
					"enable_spilo_wal_path_compat": {
						Type: "boolean",
					},
					"etcd_host": {
						Type: "string",
					},
					"kubernetes_use_configmaps": {
						Type: "boolean",
					},
					"max_instances": {
						Type:        "integer",
						Description: "-1 = disabled",
						Minimum:     &minDisable,
					},
					"min_instances": {
						Type:        "integer",
						Description: "-1 = disabled",
						Minimum:     &minDisable,
					},
					"resync_period": {
						Type: "string",
					},
					"repair_period": {
						Type: "string",
					},
					"set_memory_request_to_limit": {
						Type: "boolean",
					},
					"sidecar_docker_images": {
						Type: "object",
						AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
							Schema: &apiextv1.JSONSchemaProps{
								Type: "string",
							},
						},
					},
					"sidecars": {
						Type: "array",
						Items: &apiextv1.JSONSchemaPropsOrArray{
							Schema: &apiextv1.JSONSchemaProps{
								Type:                   "object",
								XPreserveUnknownFields: util.True(),
							},
						},
					},
					"workers": {
						Type:    "integer",
						Minimum: &min1,
					},
					"users": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"replication_username": {
								Type: "string",
							},
							"super_username": {
								Type: "string",
							},
						},
					},
					"major_version_upgrade": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"major_version_upgrade_mode": {
								Type: "string",
							},
							"minimal_major_version": {
								Type: "string",
							},
							"target_major_version": {
								Type: "string",
							},
						},
					},
					"kubernetes": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"additional_pod_capabilities": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"cluster_domain": {
								Type: "string",
							},
							"cluster_labels": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"cluster_name_label": {
								Type: "string",
							},
							"custom_pod_annotations": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"delete_annotation_date_key": {
								Type: "string",
							},
							"delete_annotation_name_key": {
								Type: "string",
							},
							"downscaler_annotations": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"enable_cross_namespace_secret": {
								Type: "boolean",
							},
							"enable_init_containers": {
								Type: "boolean",
							},
							"enable_pod_antiaffinity": {
								Type: "boolean",
							},
							"enable_pod_disruption_budget": {
								Type: "boolean",
							},
							"enable_sidecars": {
								Type: "boolean",
							},
							"infrastructure_roles_secret_name": {
								Type: "string",
							},
							"infrastructure_roles_secrets": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type:     "object",
										Required: []string{"secretname", "userkey", "passwordkey"},
										Properties: map[string]apiextv1.JSONSchemaProps{
											"secretname": {
												Type: "string",
											},
											"userkey": {
												Type: "string",
											},
											"passwordkey": {
												Type: "string",
											},
											"rolekey": {
												Type: "string",
											},
											"defaultuservalue": {
												Type: "string",
											},
											"defaultrolevalue": {
												Type: "string",
											},
											"details": {
												Type: "string",
											},
											"template": {
												Type: "boolean",
											},
										},
									},
								},
							},
							"inherited_annotations": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"inherited_labels": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"master_pod_move_timeout": {
								Type: "string",
							},
							"node_readiness_label": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"oauth_token_secret_name": {
								Type: "string",
							},
							"pdb_name_format": {
								Type: "string",
							},
							"pod_antiaffinity_topology_key": {
								Type: "string",
							},
							"pod_environment_configmap": {
								Type: "string",
							},
							"pod_environment_secret": {
								Type: "string",
							},
							"pod_management_policy": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"ordered_ready"`),
									},
									{
										Raw: []byte(`"parallel"`),
									},
								},
							},
							"pod_priority_class_name": {
								Type: "string",
							},
							"pod_role_label": {
								Type: "string",
							},
							"pod_service_account_definition": {
								Type: "string",
							},
							"pod_service_account_name": {
								Type: "string",
							},
							"pod_service_account_role_binding_definition": {
								Type: "string",
							},
							"pod_terminate_grace_period": {
								Type: "string",
							},
							"secret_name_template": {
								Type: "string",
							},
							"spilo_runasuser": {
								Type: "integer",
							},
							"spilo_runasgroup": {
								Type: "integer",
							},
							"spilo_fsgroup": {
								Type: "integer",
							},
							"spilo_privileged": {
								Type: "boolean",
							},
							"spilo_allow_privilege_escalation": {
								Type: "boolean",
							},
							"storage_resize_mode": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"ebs"`),
									},
									{
										Raw: []byte(`"pvc"`),
									},
									{
										Raw: []byte(`"off"`),
									},
								},
							},
							"toleration": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"watched_namespace": {
								Type: "string",
							},
						},
					},
					"postgres_pod_resources": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"default_cpu_limit": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"default_cpu_request": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"default_memory_limit": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"default_memory_request": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"min_cpu_limit": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"min_memory_limit": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
						},
					},
					"timeouts": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"pod_label_wait_timeout": {
								Type: "string",
							},
							"pod_deletion_wait_timeout": {
								Type: "string",
							},
							"ready_wait_interval": {
								Type: "string",
							},
							"ready_wait_timeout": {
								Type: "string",
							},
							"resource_check_interval": {
								Type: "string",
							},
							"resource_check_timeout": {
								Type: "string",
							},
						},
					},
					"load_balancer": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"custom_service_annotations": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"db_hosted_zone": {
								Type: "string",
							},
							"enable_master_load_balancer": {
								Type: "boolean",
							},
							"enable_replica_load_balancer": {
								Type: "boolean",
							},
							"external_traffic_policy": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"Cluster"`),
									},
									{
										Raw: []byte(`"Local"`),
									},
								},
							},
							"master_dns_name_format": {
								Type: "string",
							},
							"replica_dns_name_format": {
								Type: "string",
							},
						},
					},
					"aws_or_gcp": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"additional_secret_mount": {
								Type: "string",
							},
							"additional_secret_mount_path": {
								Type: "string",
							},
							"aws_region": {
								Type: "string",
							},
							"enable_ebs_gp3_migration": {
								Type: "boolean",
							},
							"enable_ebs_gp3_migration_max_size": {
								Type: "integer",
							},
							"gcp_credentials": {
								Type: "string",
							},
							"kube_iam_role": {
								Type: "string",
							},
							"log_s3_bucket": {
								Type: "string",
							},
							"wal_s3_bucket": {
								Type: "string",
							},
						},
					},
					"logical_backup": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"logical_backup_docker_image": {
								Type: "string",
							},
							"logical_backup_google_application_credentials": {
								Type: "string",
							},
							"logical_backup_job_prefix": {
								Type: "string",
							},
							"logical_backup_provider": {
								Type: "string",
							},
							"logical_backup_s3_access_key_id": {
								Type: "string",
							},
							"logical_backup_s3_bucket": {
								Type: "string",
							},
							"logical_backup_s3_endpoint": {
								Type: "string",
							},
							"logical_backup_s3_region": {
								Type: "string",
							},
							"logical_backup_s3_secret_access_key": {
								Type: "string",
							},
							"logical_backup_s3_sse": {
								Type: "string",
							},
							"logical_backup_schedule": {
								Type:    "string",
								Pattern: "^(\\d+|\\*)(/\\d+)?(\\s+(\\d+|\\*)(/\\d+)?){4}$",
							},
						},
					},
					"debug": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"debug_logging": {
								Type: "boolean",
							},
							"enable_database_access": {
								Type: "boolean",
							},
						},
					},
					"teams_api": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"enable_admin_role_for_users": {
								Type: "boolean",
							},
							"enable_postgres_team_crd": {
								Type: "boolean",
							},
							"enable_postgres_team_crd_superusers": {
								Type: "boolean",
							},
							"enable_team_member_deprecation": {
								Type: "boolean",
							},
							"enable_team_superuser": {
								Type: "boolean",
							},
							"enable_teams_api": {
								Type: "boolean",
							},
							"pam_configuration": {
								Type: "string",
							},
							"pam_role_name": {
								Type: "string",
							},
							"postgres_superuser_teams": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"protected_role_names": {
								Type: "array",
								Items: &apiextv1.JSONSchemaPropsOrArray{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"role_deletion_suffix": {
								Type: "string",
							},
							"team_admin_role": {
								Type: "string",
							},
							"team_api_role_configuration": {
								Type: "object",
								AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
									Schema: &apiextv1.JSONSchemaProps{
										Type: "string",
									},
								},
							},
							"teams_api_url": {
								Type: "string",
							},
						},
					},
					"logging_rest_api": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"api_port": {
								Type: "integer",
							},
							"cluster_history_entries": {
								Type: "integer",
							},
							"ring_log_lines": {
								Type: "integer",
							},
						},
					},
					"scalyr": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"scalyr_api_key": {
								Type: "string",
							},
							"scalyr_cpu_limit": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"scalyr_cpu_request": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"scalyr_image": {
								Type: "string",
							},
							"scalyr_memory_limit": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"scalyr_memory_request": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"scalyr_server_url": {
								Type: "string",
							},
						},
					},
					"connection_pooler": {
						Type: "object",
						Properties: map[string]apiextv1.JSONSchemaProps{
							"connection_pooler_default_cpu_limit": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"connection_pooler_default_cpu_request": {
								Type:    "string",
								Pattern: "^(\\d+m|\\d+(\\.\\d{1,3})?)$",
							},
							"connection_pooler_default_memory_limit": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"connection_pooler_default_memory_request": {
								Type:    "string",
								Pattern: "^(\\d+(e\\d+)?|\\d+(\\.\\d+)?(e\\d+)?[EPTGMK]i?)$",
							},
							"connection_pooler_image": {
								Type: "string",
							},
							"connection_pooler_max_db_connections": {
								Type: "integer",
							},
							"connection_pooler_mode": {
								Type: "string",
								Enum: []apiextv1.JSON{
									{
										Raw: []byte(`"session"`),
									},
									{
										Raw: []byte(`"transaction"`),
									},
								},
							},
							"connection_pooler_number_of_instances": {
								Type:    "integer",
								Minimum: &min1,
							},
							"connection_pooler_schema": {
								Type: "string",
							},
							"connection_pooler_user": {
								Type: "string",
							},
						},
					},
				},
			},
			"status": {
				Type: "object",
				AdditionalProperties: &apiextv1.JSONSchemaPropsOrBool{
					Schema: &apiextv1.JSONSchemaProps{
						Type: "string",
					},
				},
			},
		},
	},
}

func buildCRD(name, kind, plural, short string, columns []apiextv1.CustomResourceColumnDefinition, validation apiextv1.CustomResourceValidation) *apiextv1.CustomResourceDefinition {
	return &apiextv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: apiextv1.CustomResourceDefinitionSpec{
			Group: SchemeGroupVersion.Group,
			Names: apiextv1.CustomResourceDefinitionNames{
				Plural:     plural,
				ShortNames: []string{short},
				Kind:       kind,
				Categories: []string{"all"},
			},
			Scope: apiextv1.NamespaceScoped,
			Versions: []apiextv1.CustomResourceDefinitionVersion{
				{
					Name:    SchemeGroupVersion.Version,
					Served:  true,
					Storage: true,
					Subresources: &apiextv1.CustomResourceSubresources{
						Status: &apiextv1.CustomResourceSubresourceStatus{},
					},
					AdditionalPrinterColumns: columns,
					Schema:                   &validation,
				},
			},
		},
	}
}

// PostgresCRD returns CustomResourceDefinition built from PostgresCRDResource
func PostgresCRD(enableValidation *bool) *apiextv1.CustomResourceDefinition {
	postgresCRDvalidation := apiextv1.CustomResourceValidation{}

	if enableValidation != nil && *enableValidation {
		postgresCRDvalidation = PostgresCRDResourceValidation
	}

	return buildCRD(PostgresCRDResouceName,
		PostgresCRDResourceKind,
		PostgresCRDResourcePlural,
		PostgresCRDResourceShort,
		PostgresCRDResourceColumns,
		postgresCRDvalidation)
}

// ConfigurationCRD returns CustomResourceDefinition built from OperatorConfigCRDResource
func ConfigurationCRD(enableValidation *bool) *apiextv1.CustomResourceDefinition {
	opconfigCRDvalidation := apiextv1.CustomResourceValidation{}

	if enableValidation != nil && *enableValidation {
		opconfigCRDvalidation = OperatorConfigCRDResourceValidation
	}

	return buildCRD(OperatorConfigCRDResourceName,
		OperatorConfigCRDResouceKind,
		OperatorConfigCRDResourcePlural,
		OperatorConfigCRDResourceShort,
		OperatorConfigCRDResourceColumns,
		opconfigCRDvalidation)
}
