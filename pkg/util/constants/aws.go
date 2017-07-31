package constants

import "time"

// AWS specific constants used by other modules
const (
	// default region for AWS. TODO: move it to the operator configuration
	AWSRegion = "eu-central-1"
	// EBS related constants
	EBSVolumeIDStart = "/vol-"
	EBSProvisioner   = "kubernetes.io/aws-ebs"
	//https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_VolumeModification.html
	EBSVolumeStateModifying     = "modifying"
	EBSVolumeStateOptimizing    = "optimizing"
	EBSVolumeStateFailed        = "failed"
	EBSVolumeStateCompleted     = "completed"
	EBSVolumeResizeWaitInterval = 2 * time.Second
	EBSVolumeResizeWaitTimeout  = 30 * time.Second
)
