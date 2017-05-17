package constants

import "time"

const (
	TPRName                     = "postgresql"
	TPRVendor                   = "acid.zalan.do"
	TPRDescription              = "Managed PostgreSQL clusters"
	TPRApiVersion               = "v1"
	ListClustersURITemplate     = "/apis/" + TPRVendor + "/" + TPRApiVersion + "/namespaces/%s/" + ResourceName       // Namespace
	WatchClustersURITemplate    = "/apis/" + TPRVendor + "/" + TPRApiVersion + "/watch/namespaces/%s/" + ResourceName // Namespace
	K8sVersion                  = "v1"
	K8sAPIPath                  = "/api"
	DataVolumeName              = "pgdata"
	PasswordLength              = 64
	UserSecretTemplate          = "%s.%s.credentials." + TPRName + "." + TPRVendor // Username, ClusterName
	ZalandoDNSNameAnnotation    = "external-dns.alpha.kubernetes.io/hostname"
	ElbTimeoutAnnotationName    = "service.beta.kubernetes.io/aws-load-balancer-connection-idle-timeout"
	ElbTimeoutAnnotationValue   = "3600"
	KubeIAmAnnotation           = "iam.amazonaws.com/role"
	ResourceName                = TPRName + "s"
	PodRoleMaster               = "master"
	PodRoleReplica              = "replica"
	SuperuserKeyName            = "superuser"
	ReplicationUserKeyName      = "replication"
	StatefulsetDeletionInterval = 1 * time.Second
	StatefulsetDeletionTimeout  = 30 * time.Second

	RoleFlagSuperuser  = "SUPERUSER"
	RoleFlagInherit    = "INHERIT"
	RoleFlagLogin      = "LOGIN"
	RoleFlagNoLogin    = "NOLOGIN"
	RoleFlagCreateRole = "CREATEROLE"
	RoleFlagCreateDB   = "CREATEDB"
)
