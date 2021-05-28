package controller

import (
	"context"
	"fmt"

	"time"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/constants"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Controller) readOperatorConfigurationFromCRD(configObjectNamespace, configObjectName string) (*acidv1.OperatorConfiguration, error) {

	config, err := c.KubeClient.OperatorConfigurationsGetter.OperatorConfigurations(configObjectNamespace).Get(
		context.TODO(), configObjectName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not get operator configuration object %q: %v", configObjectName, err)
	}

	return config, nil
}

func int32ToPointer(value int32) *int32 {
	return &value
}

// importConfigurationFromCRD is a transitional function that converts CRD configuration to the one based on the configmap
func (c *Controller) importConfigurationFromCRD(fromCRD *acidv1.OperatorConfigurationData) *config.Config {
	result := &config.Config{}

	// general config
	result.EnableCRDValidation = util.CoalesceBool(fromCRD.EnableCRDValidation, util.True())
	result.EnableLazySpiloUpgrade = fromCRD.EnableLazySpiloUpgrade
	result.EnablePgVersionEnvVar = fromCRD.EnablePgVersionEnvVar
	result.EnableSpiloWalPathCompat = fromCRD.EnableSpiloWalPathCompat
	result.EtcdHost = fromCRD.EtcdHost
	result.KubernetesUseConfigMaps = fromCRD.KubernetesUseConfigMaps
	result.DockerImage = util.Coalesce(fromCRD.DockerImage, "registry.opensource.zalan.do/acid/spilo-13:2.0-p7")
	result.Workers = util.CoalesceUInt32(fromCRD.Workers, 8)
	result.MinInstances = fromCRD.MinInstances
	result.MaxInstances = fromCRD.MaxInstances
	result.ResyncPeriod = util.CoalesceDuration(time.Duration(fromCRD.ResyncPeriod), "30m")
	result.RepairPeriod = util.CoalesceDuration(time.Duration(fromCRD.RepairPeriod), "5m")
	result.SetMemoryRequestToLimit = fromCRD.SetMemoryRequestToLimit
	result.ShmVolume = util.CoalesceBool(fromCRD.ShmVolume, util.True())
	result.SidecarImages = fromCRD.SidecarImages
	result.SidecarContainers = fromCRD.SidecarContainers

	// user config
	result.SuperUsername = util.Coalesce(fromCRD.PostgresUsersConfiguration.SuperUsername, "postgres")
	result.ReplicationUsername = util.Coalesce(fromCRD.PostgresUsersConfiguration.ReplicationUsername, "standby")

	// major version upgrade config
	result.MajorVersionUpgradeMode = util.Coalesce(fromCRD.MajorVersionUpgrade.MajorVersionUpgradeMode, "off")
	result.MinimalMajorVersion = util.Coalesce(fromCRD.MajorVersionUpgrade.MinimalMajorVersion, "9.5")
	result.TargetMajorVersion = util.Coalesce(fromCRD.MajorVersionUpgrade.TargetMajorVersion, "13")

	// kubernetes config
	result.CustomPodAnnotations = fromCRD.Kubernetes.CustomPodAnnotations
	result.PodServiceAccountName = util.Coalesce(fromCRD.Kubernetes.PodServiceAccountName, "postgres-pod")
	result.PodServiceAccountDefinition = fromCRD.Kubernetes.PodServiceAccountDefinition
	result.PodServiceAccountRoleBindingDefinition = fromCRD.Kubernetes.PodServiceAccountRoleBindingDefinition
	result.PodEnvironmentConfigMap = fromCRD.Kubernetes.PodEnvironmentConfigMap
	result.PodEnvironmentSecret = fromCRD.Kubernetes.PodEnvironmentSecret
	result.PodTerminateGracePeriod = util.CoalesceDuration(time.Duration(fromCRD.Kubernetes.PodTerminateGracePeriod), "5m")
	result.SpiloPrivileged = fromCRD.Kubernetes.SpiloPrivileged
	result.SpiloAllowPrivilegeEscalation = util.CoalesceBool(fromCRD.Kubernetes.SpiloAllowPrivilegeEscalation, util.True())
	result.SpiloRunAsUser = fromCRD.Kubernetes.SpiloRunAsUser
	result.SpiloRunAsGroup = fromCRD.Kubernetes.SpiloRunAsGroup
	result.SpiloFSGroup = fromCRD.Kubernetes.SpiloFSGroup
	result.AdditionalPodCapabilities = fromCRD.Kubernetes.AdditionalPodCapabilities
	result.ClusterDomain = util.Coalesce(fromCRD.Kubernetes.ClusterDomain, "cluster.local")
	result.WatchedNamespace = fromCRD.Kubernetes.WatchedNamespace
	result.PDBNameFormat = fromCRD.Kubernetes.PDBNameFormat
	result.EnablePodDisruptionBudget = util.CoalesceBool(fromCRD.Kubernetes.EnablePodDisruptionBudget, util.True())
	result.StorageResizeMode = util.Coalesce(fromCRD.Kubernetes.StorageResizeMode, "pvc")
	result.EnableInitContainers = util.CoalesceBool(fromCRD.Kubernetes.EnableInitContainers, util.True())
	result.EnableSidecars = util.CoalesceBool(fromCRD.Kubernetes.EnableSidecars, util.True())
	result.SecretNameTemplate = fromCRD.Kubernetes.SecretNameTemplate
	result.OAuthTokenSecretName = fromCRD.Kubernetes.OAuthTokenSecretName

	result.InfrastructureRolesSecretName = fromCRD.Kubernetes.InfrastructureRolesSecretName
	if fromCRD.Kubernetes.InfrastructureRolesDefs != nil {
		result.InfrastructureRoles = []*config.InfrastructureRole{}
		for _, secret := range fromCRD.Kubernetes.InfrastructureRolesDefs {
			result.InfrastructureRoles = append(
				result.InfrastructureRoles,
				&config.InfrastructureRole{
					SecretName:  secret.SecretName,
					UserKey:     secret.UserKey,
					RoleKey:     secret.RoleKey,
					PasswordKey: secret.PasswordKey,
				})
		}
	}

	result.PodRoleLabel = util.Coalesce(fromCRD.Kubernetes.PodRoleLabel, "spilo-role")
	result.ClusterLabels = util.CoalesceStrMap(fromCRD.Kubernetes.ClusterLabels, map[string]string{"application": "spilo"})
	result.InheritedLabels = fromCRD.Kubernetes.InheritedLabels
	result.InheritedAnnotations = fromCRD.Kubernetes.InheritedAnnotations
	result.DownscalerAnnotations = fromCRD.Kubernetes.DownscalerAnnotations
	result.ClusterNameLabel = util.Coalesce(fromCRD.Kubernetes.ClusterNameLabel, "cluster-name")
	result.DeleteAnnotationDateKey = fromCRD.Kubernetes.DeleteAnnotationDateKey
	result.DeleteAnnotationNameKey = fromCRD.Kubernetes.DeleteAnnotationNameKey
	result.NodeReadinessLabel = fromCRD.Kubernetes.NodeReadinessLabel
	result.PodPriorityClassName = fromCRD.Kubernetes.PodPriorityClassName
	result.PodManagementPolicy = util.Coalesce(fromCRD.Kubernetes.PodManagementPolicy, "ordered_ready")
	result.MasterPodMoveTimeout = util.CoalesceDuration(time.Duration(fromCRD.Kubernetes.MasterPodMoveTimeout), "10m")
	result.EnablePodAntiAffinity = fromCRD.Kubernetes.EnablePodAntiAffinity
	result.PodAntiAffinityTopologyKey = util.Coalesce(fromCRD.Kubernetes.PodAntiAffinityTopologyKey, "kubernetes.io/hostname")

	// Postgres Pod resources
	result.DefaultCPURequest = util.Coalesce(fromCRD.PostgresPodResources.DefaultCPURequest, "100m")
	result.DefaultMemoryRequest = util.Coalesce(fromCRD.PostgresPodResources.DefaultMemoryRequest, "100Mi")
	result.DefaultCPULimit = util.Coalesce(fromCRD.PostgresPodResources.DefaultCPULimit, "1")
	result.DefaultMemoryLimit = util.Coalesce(fromCRD.PostgresPodResources.DefaultMemoryLimit, "500Mi")
	result.MinCPULimit = util.Coalesce(fromCRD.PostgresPodResources.MinCPULimit, "250m")
	result.MinMemoryLimit = util.Coalesce(fromCRD.PostgresPodResources.MinMemoryLimit, "250Mi")

	// timeout config
	result.ResourceCheckInterval = util.CoalesceDuration(time.Duration(fromCRD.Timeouts.ResourceCheckInterval), "3s")
	result.ResourceCheckTimeout = util.CoalesceDuration(time.Duration(fromCRD.Timeouts.ResourceCheckTimeout), "10m")
	result.PodLabelWaitTimeout = util.CoalesceDuration(time.Duration(fromCRD.Timeouts.PodLabelWaitTimeout), "10m")
	result.PodDeletionWaitTimeout = util.CoalesceDuration(time.Duration(fromCRD.Timeouts.PodDeletionWaitTimeout), "10m")
	result.ReadyWaitInterval = util.CoalesceDuration(time.Duration(fromCRD.Timeouts.ReadyWaitInterval), "4s")
	result.ReadyWaitTimeout = util.CoalesceDuration(time.Duration(fromCRD.Timeouts.ReadyWaitTimeout), "30s")

	// load balancer config
	result.DbHostedZone = util.Coalesce(fromCRD.LoadBalancer.DbHostedZone, "db.example.com")
	result.EnableMasterLoadBalancer = fromCRD.LoadBalancer.EnableMasterLoadBalancer
	result.EnableReplicaLoadBalancer = fromCRD.LoadBalancer.EnableReplicaLoadBalancer
	result.CustomServiceAnnotations = fromCRD.LoadBalancer.CustomServiceAnnotations
	result.MasterDNSNameFormat = fromCRD.LoadBalancer.MasterDNSNameFormat
	result.ReplicaDNSNameFormat = fromCRD.LoadBalancer.ReplicaDNSNameFormat
	result.ExternalTrafficPolicy = util.Coalesce(fromCRD.LoadBalancer.ExternalTrafficPolicy, "Cluster")

	// AWS or GCP config
	result.WALES3Bucket = fromCRD.AWSGCP.WALES3Bucket
	result.AWSRegion = fromCRD.AWSGCP.AWSRegion
	result.LogS3Bucket = fromCRD.AWSGCP.LogS3Bucket
	result.KubeIAMRole = fromCRD.AWSGCP.KubeIAMRole
	result.WALGSBucket = fromCRD.AWSGCP.WALGSBucket
	result.GCPCredentials = fromCRD.AWSGCP.GCPCredentials
	result.AdditionalSecretMount = fromCRD.AWSGCP.AdditionalSecretMount
	result.AdditionalSecretMountPath = util.Coalesce(fromCRD.AWSGCP.AdditionalSecretMountPath, "/meta/credentials")
	result.EnableEBSGp3Migration = fromCRD.AWSGCP.EnableEBSGp3Migration
	result.EnableEBSGp3MigrationMaxSize = util.CoalesceInt64(fromCRD.AWSGCP.EnableEBSGp3MigrationMaxSize, 1000)

	// logical backup config
	result.LogicalBackupSchedule = util.Coalesce(fromCRD.LogicalBackup.Schedule, "30 00 * * *")
	result.LogicalBackupDockerImage = util.Coalesce(fromCRD.LogicalBackup.DockerImage, "registry.opensource.zalan.do/acid/logical-backup:v1.6.3")
	result.LogicalBackupProvider = util.Coalesce(fromCRD.LogicalBackup.BackupProvider, "s3")
	result.LogicalBackupS3Bucket = fromCRD.LogicalBackup.S3Bucket
	result.LogicalBackupS3Region = fromCRD.LogicalBackup.S3Region
	result.LogicalBackupS3Endpoint = fromCRD.LogicalBackup.S3Endpoint
	result.LogicalBackupS3AccessKeyID = fromCRD.LogicalBackup.S3AccessKeyID
	result.LogicalBackupS3SecretAccessKey = fromCRD.LogicalBackup.S3SecretAccessKey
	result.LogicalBackupS3SSE = fromCRD.LogicalBackup.S3SSE
	result.LogicalBackupS3RetentionTime = fromCRD.LogicalBackup.RetentionTime
	result.LogicalBackupGoogleApplicationCredentials = fromCRD.LogicalBackup.GoogleApplicationCredentials
	result.LogicalBackupJobPrefix = util.Coalesce(fromCRD.LogicalBackup.JobPrefix, "logical-backup-")

	// debug config
	result.DebugLogging = fromCRD.OperatorDebug.DebugLogging
	result.EnableDBAccess = fromCRD.OperatorDebug.EnableDBAccess

	// Teams API config
	result.EnableTeamsAPI = fromCRD.TeamsAPI.EnableTeamsAPI
	result.TeamsAPIUrl = util.Coalesce(fromCRD.TeamsAPI.TeamsAPIUrl, "https://teams.example.com/api/")
	result.TeamAPIRoleConfiguration = util.CoalesceStrMap(fromCRD.TeamsAPI.TeamAPIRoleConfiguration, map[string]string{"log_statement": "all"})
	result.EnableTeamSuperuser = fromCRD.TeamsAPI.EnableTeamSuperuser
	result.EnableAdminRoleForUsers = fromCRD.TeamsAPI.EnableAdminRoleForUsers
	result.TeamAdminRole = fromCRD.TeamsAPI.TeamAdminRole
	result.PamRoleName = util.Coalesce(fromCRD.TeamsAPI.PamRoleName, "zalandos")
	result.PamConfiguration = util.Coalesce(fromCRD.TeamsAPI.PamConfiguration, "https://info.example.com/oauth2/tokeninfo?access_token= uid realm=/employees")
	result.ProtectedRoles = util.CoalesceStrArr(fromCRD.TeamsAPI.ProtectedRoles, []string{"admin"})
	result.PostgresSuperuserTeams = fromCRD.TeamsAPI.PostgresSuperuserTeams
	result.EnablePostgresTeamCRD = fromCRD.TeamsAPI.EnablePostgresTeamCRD
	result.EnablePostgresTeamCRDSuperusers = fromCRD.TeamsAPI.EnablePostgresTeamCRDSuperusers
	result.EnableTeamMemberDeprecation = fromCRD.TeamsAPI.EnableTeamMemberDeprecation
	result.RoleDeletionSuffix = util.Coalesce(fromCRD.TeamsAPI.RoleDeletionSuffix, "_deleted")

	// logging REST API config
	result.APIPort = util.CoalesceInt(fromCRD.LoggingRESTAPI.APIPort, 8080)
	result.RingLogLines = util.CoalesceInt(fromCRD.LoggingRESTAPI.RingLogLines, 100)
	result.ClusterHistoryEntries = util.CoalesceInt(fromCRD.LoggingRESTAPI.ClusterHistoryEntries, 1000)

	// Scalyr config
	result.ScalyrAPIKey = fromCRD.Scalyr.ScalyrAPIKey
	result.ScalyrImage = fromCRD.Scalyr.ScalyrImage
	result.ScalyrServerURL = fromCRD.Scalyr.ScalyrServerURL
	result.ScalyrCPURequest = fromCRD.Scalyr.ScalyrCPURequest
	result.ScalyrMemoryRequest = fromCRD.Scalyr.ScalyrMemoryRequest
	result.ScalyrCPULimit = fromCRD.Scalyr.ScalyrCPULimit
	result.ScalyrMemoryLimit = fromCRD.Scalyr.ScalyrMemoryLimit

	// Connection pooler. Looks like we can't use defaulting in CRD before 1.17,
	// so ensure default values here.
	result.ConnectionPooler.NumberOfInstances = util.CoalesceInt32(
		fromCRD.ConnectionPooler.NumberOfInstances,
		int32ToPointer(2))

	result.ConnectionPooler.NumberOfInstances = util.MaxInt32(
		result.ConnectionPooler.NumberOfInstances,
		int32ToPointer(2))

	result.ConnectionPooler.Schema = util.Coalesce(
		fromCRD.ConnectionPooler.Schema,
		constants.ConnectionPoolerSchemaName)

	result.ConnectionPooler.User = util.Coalesce(
		fromCRD.ConnectionPooler.User,
		constants.ConnectionPoolerUserName)

	if result.ConnectionPooler.User == result.SuperUsername {
		msg := "Connection pool user is not allowed to be the same as super user, username: %s"
		panic(fmt.Errorf(msg, result.ConnectionPooler.User))
	}

	result.ConnectionPooler.Image = util.Coalesce(
		fromCRD.ConnectionPooler.Image,
		"registry.opensource.zalan.do/acid/pgbouncer")

	result.ConnectionPooler.Mode = util.Coalesce(
		fromCRD.ConnectionPooler.Mode,
		constants.ConnectionPoolerDefaultMode)

	result.ConnectionPooler.ConnectionPoolerDefaultCPURequest = util.Coalesce(
		fromCRD.ConnectionPooler.DefaultCPURequest,
		constants.ConnectionPoolerDefaultCpuRequest)

	result.ConnectionPooler.ConnectionPoolerDefaultMemoryRequest = util.Coalesce(
		fromCRD.ConnectionPooler.DefaultMemoryRequest,
		constants.ConnectionPoolerDefaultMemoryRequest)

	result.ConnectionPooler.ConnectionPoolerDefaultCPULimit = util.Coalesce(
		fromCRD.ConnectionPooler.DefaultCPULimit,
		constants.ConnectionPoolerDefaultCpuLimit)

	result.ConnectionPooler.ConnectionPoolerDefaultMemoryLimit = util.Coalesce(
		fromCRD.ConnectionPooler.DefaultMemoryLimit,
		constants.ConnectionPoolerDefaultMemoryLimit)

	result.ConnectionPooler.MaxDBConnections = util.CoalesceInt32(
		fromCRD.ConnectionPooler.MaxDBConnections,
		int32ToPointer(constants.ConnectionPoolerMaxDBConnections))

	return result
}
