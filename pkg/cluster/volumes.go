package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/filesystems"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/volumes"
)

func (c *Cluster) syncVolumes() error {
	c.logger.Debugf("syncing volumes using %q storage resize mode", c.OpConfig.StorageResizeMode)
	var err error

	// check quantity string once, and do not bother with it anymore anywhere else
	_, err = resource.ParseQuantity(c.Spec.Volume.Size)
	if err != nil {
		return fmt.Errorf("could not parse volume size from the manifest: %v", err)
	}

	if c.OpConfig.StorageResizeMode == "mixed" {
		// mixed op uses AWS API to adjust size, throughput, iops, and calls pvc change for file system resize
		// in case of errors we proceed to let K8s do its work, favoring disk space increase of other adjustments

		err = c.populateVolumeMetaData()
		if err != nil {
			c.logger.Errorf("populating EBS meta data failed, skipping potential adjustments: %v", err)
		} else {
			err = c.syncUnderlyingEBSVolume()
			if err != nil {
				c.logger.Errorf("errors occurred during EBS volume adjustments: %v", err)
			}
		}
	}

	if err = c.syncVolumeClaims(); err != nil {
		err = fmt.Errorf("could not sync persistent volume claims: %v", err)
		return err
	}

	if c.OpConfig.StorageResizeMode == "ebs" {
		// potentially enlarge volumes before changing the statefulset. By doing that
		// in this order we make sure the operator is not stuck waiting for a pod that
		// cannot start because it ran out of disk space.
		// TODO: handle the case of the cluster that is downsized and enlarged again
		// (there will be a volume from the old pod for which we can't act before the
		//  the statefulset modification is concluded)
		if err = c.syncEbsVolumes(); err != nil {
			err = fmt.Errorf("could not sync persistent volumes: %v", err)
			return err
		}
	}

	return nil
}

func (c *Cluster) syncUnderlyingEBSVolume() error {
	c.logger.Debug("starting to sync EBS volumes: type, iops, throughput, and size")

	var (
		err     error
		newSize resource.Quantity
	)

	targetValue := c.Spec.Volume
	if newSize, err = resource.ParseQuantity(targetValue.Size); err != nil {
		return fmt.Errorf("could not parse volume size: %v", err)
	}
	targetSize := quantityToGigabyte(newSize)

	errors := make([]string, 0)

	// with a volume attributes class the CSI driver owns iops, throughput and volume type
	useVolumeAttributesClass := targetValue.VolumeAttributesClass != ""
	if useVolumeAttributesClass && (targetValue.Iops != nil || targetValue.Throughput != nil || targetValue.VolumeType != "") {
		c.logger.Warningf("volume attributes class %q is set, ignoring iops, throughput and type of the volume in the manifest", targetValue.VolumeAttributesClass)
	}

	for _, volume := range c.EBSVolumes {
		var modifyIops *int32
		var modifyThroughput *int32
		var modifySize *int32
		var modifyType *string

		if targetValue.Iops != nil && *targetValue.Iops >= int32(3000) {
			if volume.Iops != int32(*targetValue.Iops) {
				modifyIops = targetValue.Iops
			}
		}

		if targetValue.Throughput != nil && *targetValue.Throughput >= int32(125) {
			if volume.Throughput != int32(*targetValue.Throughput) {
				modifyThroughput = targetValue.Throughput
			}
		}

		if targetSize > volume.Size {
			modifySize = &targetSize
		}

		if targetValue.VolumeType != "" && targetValue.VolumeType != volume.VolumeType {
			modifyType = &targetValue.VolumeType
		}

		if useVolumeAttributesClass {
			modifyIops, modifyThroughput, modifyType = nil, nil, nil
		}

		if modifyIops != nil || modifyThroughput != nil || modifySize != nil || modifyType != nil {
			err = c.VolumeResizer.ModifyVolume(volume.VolumeID, modifyType, modifySize, modifyIops, modifyThroughput)
			if err != nil {
				errors = append(errors, fmt.Sprintf("modify failed: %v, showing current EBS values: volume-id=%s size=%d iops=%d throughput=%d", err, volume.VolumeID, volume.Size, volume.Iops, volume.Throughput))
			}
		}
	}

	if len(errors) > 0 {
		for _, s := range errors {
			c.logger.Warningf("%s", s)
		}
	}
	return nil
}

func (c *Cluster) populateVolumeMetaData() error {
	c.logger.Debug("starting reading ebs meta data")

	pvs, err := c.listPersistentVolumes()
	if err != nil {
		return fmt.Errorf("could not list persistent volumes: %v", err)
	}
	if len(pvs) == 0 {
		c.EBSVolumes = make(map[string]volumes.VolumeProperties)
		return fmt.Errorf("no persistent volumes found")
	}
	c.logger.Debugf("found %d persistent volumes, size of known volumes %d", len(pvs), len(c.EBSVolumes))

	volumeIds := []string{}
	var volumeID string
	for _, pv := range pvs {
		volumeID, err = c.VolumeResizer.GetProviderVolumeID(pv)
		if err != nil {
			continue
		}

		volumeIds = append(volumeIds, volumeID)
	}

	currentVolumes, err := c.VolumeResizer.DescribeVolumes(volumeIds)
	if nil != err {
		return err
	}

	if len(currentVolumes) != len(c.EBSVolumes) && len(c.EBSVolumes) > 0 {
		c.logger.Infof("number of ebs volumes (%d) discovered differs from already known volumes (%d)", len(currentVolumes), len(c.EBSVolumes))
	}

	// reset map, operator is not responsible for dangling ebs volumes
	c.EBSVolumes = make(map[string]volumes.VolumeProperties)
	for _, volume := range currentVolumes {
		c.EBSVolumes[volume.VolumeID] = volume
	}

	return nil
}

// syncVolumeClaims reads all persistent volume claims and checks that their size matches the one declared in the statefulset.
func (c *Cluster) syncVolumeClaims() error {
	c.setProcessName("syncing volume claims")

	ignoreResize := false

	if c.OpConfig.StorageResizeMode == "off" || c.OpConfig.StorageResizeMode == "ebs" {
		ignoreResize = true
		c.logger.Debugf("Storage resize mode is set to %q. Skipping volume size sync of persistent volume claims.", c.OpConfig.StorageResizeMode)
	}

	newSize, err := resource.ParseQuantity(c.Spec.Volume.Size)
	if err != nil {
		return fmt.Errorf("could not parse volume size from the manifest: %v", err)
	}
	manifestSize := quantityToGigabyte(newSize)

	pvcs, err := c.listPersistentVolumeClaims()
	if err != nil {
		return fmt.Errorf("could not list persistent volume claims: %v", err)
	}
	for _, pvc := range pvcs {
		c.VolumeClaims[pvc.UID] = &pvc
		needsUpdate := false
		currentSize := quantityToGigabyte(pvc.Spec.Resources.Requests[v1.ResourceStorage])
		if !ignoreResize && currentSize != manifestSize {
			if currentSize < manifestSize {
				pvc.Spec.Resources.Requests[v1.ResourceStorage] = newSize
				needsUpdate = true
				c.logger.Infof("persistent volume claim for volume %q needs to be resized", pvc.Name)
			} else {
				c.logger.Warningf("cannot shrink persistent volume")
			}
		}

		if c.volumeAttributesClassNeedsUpdate(&pvc) {
			volumeAttributesClass := c.Spec.Volume.VolumeAttributesClass
			pvc.Spec.VolumeAttributesClassName = &volumeAttributesClass
			needsUpdate = true
			c.logger.Infof("persistent volume claim for volume %q needs volume attributes class %q", pvc.Name, volumeAttributesClass)
		}

		if status := pvc.Status.ModifyVolumeStatus; status != nil && status.Status == v1.PersistentVolumeClaimModifyVolumeInfeasible {
			c.logger.Warningf("modifying persistent volume claim %q to volume attributes class %q is infeasible", pvc.Name, status.TargetVolumeAttributesClassName)
		}

		if needsUpdate {
			c.logger.Infof("updating persistent volume claim definition for volume %q", pvc.Name)
			updatedPvc, err := c.KubeClient.PersistentVolumeClaims(pvc.Namespace).Update(context.TODO(), &pvc, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("could not update persistent volume claim: %q", err)
			}
			c.VolumeClaims[pvc.UID] = updatedPvc
			c.logger.Infof("successfully updated persistent volume claim %q", pvc.Name)
		} else {
			c.logger.Debugf("volume claim for volume %q do not require updates", pvc.Name)
		}

		newAnnotations := c.annotationsSet(nil)
		if changed, _ := c.compareAnnotations(pvc.Annotations, newAnnotations, nil); changed {
			patchData, err := metaAnnotationsPatch(newAnnotations)
			if err != nil {
				return fmt.Errorf("could not form patch for the persistent volume claim for volume %q: %v", pvc.Name, err)
			}
			patchedPvc, err := c.KubeClient.PersistentVolumeClaims(pvc.Namespace).Patch(context.TODO(), pvc.Name, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("could not patch annotations of the persistent volume claim for volume %q: %v", pvc.Name, err)
			}
			c.VolumeClaims[pvc.UID] = patchedPvc
		}
	}

	c.logger.Debug("volume claims have been synced successfully")

	return nil
}

// volumeAttributesClassNeedsUpdate reports whether the volume claim has to be switched to the volume attributes class
// of the manifest. Removing the class from the manifest leaves existing volume claims unchanged.
func (c *Cluster) volumeAttributesClassNeedsUpdate(pvc *v1.PersistentVolumeClaim) bool {
	wanted := c.Spec.Volume.VolumeAttributesClass
	if wanted == "" {
		if pvc.Spec.VolumeAttributesClassName != nil {
			c.logger.Debugf("no volume attributes class in the manifest, keeping %q on persistent volume claim %q", *pvc.Spec.VolumeAttributesClassName, pvc.Name)
		}
		return false
	}
	return pvc.Spec.VolumeAttributesClassName == nil || *pvc.Spec.VolumeAttributesClassName != wanted
}

// syncVolumes reads all persistent volumes and checks that their size matches the one declared in the statefulset.
func (c *Cluster) syncEbsVolumes() error {
	c.setProcessName("syncing EBS volumes")

	act, err := c.volumesNeedResizing()
	if err != nil {
		return fmt.Errorf("could not compare size of the volumes: %v", err)
	}
	if !act {
		return nil
	}

	if err := c.resizeVolumes(); err != nil {
		return fmt.Errorf("could not sync volumes: %v", err)
	}

	c.logger.Debug("volumes have been synced successfully")

	return nil
}

func (c *Cluster) listPersistentVolumeClaims() ([]v1.PersistentVolumeClaim, error) {
	ns := c.Namespace
	listOptions := metav1.ListOptions{
		LabelSelector: c.labelsSet(false).String(),
	}

	pvcs, err := c.KubeClient.PersistentVolumeClaims(ns).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("could not list of persistent volume claims: %v", err)
	}
	return pvcs.Items, nil
}

func (c *Cluster) deletePersistentVolumeClaims() error {
	c.setProcessName("deleting persistent volume claims")
	errors := make([]string, 0)
	for uid := range c.VolumeClaims {
		err := c.deletePersistentVolumeClaim(uid)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%v", err))
		}
	}

	if len(errors) > 0 {
		c.logger.Warningf("could not delete all persistent volume claims: %v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) deletePersistentVolumeClaim(uid types.UID) error {
	c.setProcessName("deleting persistent volume claim")
	pvc := c.VolumeClaims[uid]
	c.logger.Debugf("deleting persistent volume claim %q", pvc.Name)
	err := c.KubeClient.PersistentVolumeClaims(pvc.Namespace).Delete(context.TODO(), pvc.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("persistent volume claim %q has already been deleted", pvc.Name)
	} else if err != nil {
		return fmt.Errorf("could not delete persistent volume claim %q: %v", pvc.Name, err)
	}
	c.logger.Infof("persistent volume claim %q has been deleted", pvc.Name)
	delete(c.VolumeClaims, uid)

	return nil
}

func (c *Cluster) listPersistentVolumes() ([]*v1.PersistentVolume, error) {
	result := make([]*v1.PersistentVolume, 0)

	pvcs, err := c.listPersistentVolumeClaims()
	if err != nil {
		return nil, fmt.Errorf("could not list cluster's persistent volume claims: %v", err)
	}

	pods, err := c.listPods()
	if err != nil {
		return nil, fmt.Errorf("could not get list of running pods for resizing persistent volumes: %v", err)
	}

	lastPodIndex := len(pods) - 1

	for _, pvc := range pvcs {
		lastDash := strings.LastIndex(pvc.Name, "-")
		if lastDash > 0 && lastDash < len(pvc.Name)-1 {
			pvcNumber, err := strconv.Atoi(pvc.Name[lastDash+1:])
			if err != nil {
				return nil, fmt.Errorf("could not convert last part of the persistent volume claim name %q to a number", pvc.Name)
			}
			if pvcNumber > lastPodIndex {
				c.logger.Debugf("skipping persistent volume %q corresponding to a non-running pods", pvc.Name)
				continue
			}
		}
		pv, err := c.KubeClient.PersistentVolumes().Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not get PersistentVolume: %v", err)
		}
		result = append(result, pv)
	}

	return result, nil
}

// resizeVolumes resize persistent volumes compatible with the given resizer interface
func (c *Cluster) resizeVolumes() error {
	if c.VolumeResizer == nil {
		return fmt.Errorf("no volume resizer set for EBS volume handling")
	}

	c.setProcessName("resizing EBS volumes")

	newQuantity, err := resource.ParseQuantity(c.Spec.Volume.Size)
	if err != nil {
		return fmt.Errorf("could not parse volume size: %v", err)
	}

	newSize := quantityToGigabyte(newQuantity)
	resizer := c.VolumeResizer
	var totalIncompatible int

	pvs, err := c.listPersistentVolumes()
	if err != nil {
		return fmt.Errorf("could not list persistent volumes: %v", err)
	}

	for _, pv := range pvs {
		volumeSize := quantityToGigabyte(pv.Spec.Capacity[v1.ResourceStorage])
		if volumeSize >= newSize {
			if volumeSize > newSize {
				c.logger.Warningf("cannot shrink persistent volume")
			}
			continue
		}
		compatible := false

		if !resizer.VolumeBelongsToProvider(pv) {
			continue
		}
		compatible = true
		if !resizer.IsConnectedToProvider() {
			err := resizer.ConnectToProvider()
			if err != nil {
				return fmt.Errorf("could not connect to the volume provider: %v", err)
			}
			defer func() {
				if err := resizer.DisconnectFromProvider(); err != nil {
					c.logger.Errorf("%v", err)
				}
			}()
		}
		awsVolumeID, err := resizer.GetProviderVolumeID(pv)
		if err != nil {
			return err
		}
		c.logger.Infof("updating persistent volume %q to %d", pv.Name, newSize)
		if err := resizer.ResizeVolume(awsVolumeID, newSize); err != nil {
			return fmt.Errorf("could not resize EBS volume %q: %v", awsVolumeID, err)
		}
		c.logger.Infof("resizing the filesystem on the volume %q", pv.Name)
		podName := getPodNameFromPersistentVolume(pv)
		if err := c.resizePostgresFilesystem(podName, []filesystems.FilesystemResizer{&filesystems.Ext234Resize{}}); err != nil {
			return fmt.Errorf("could not resize the filesystem on pod %q: %v", podName, err)
		}
		c.logger.Infof("filesystem resize successful on volume %q", pv.Name)
		pv.Spec.Capacity[v1.ResourceStorage] = newQuantity
		c.logger.Infof("updating persistent volume definition for volume %q", pv.Name)
		if _, err := c.KubeClient.PersistentVolumes().Update(context.TODO(), pv, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("could not update persistent volume: %q", err)
		}
		c.logger.Infof("successfully updated persistent volume %q", pv.Name)

		if !compatible {
			c.logger.Warningf("volume %q is incompatible with all available resizing providers, consider switching storage_resize_mode to pvc or off", pv.Name)
			totalIncompatible++
		}
	}
	if totalIncompatible > 0 {
		return fmt.Errorf("could not resize EBS volumes: some persistent volumes are not compatible with existing resizing providers")
	}
	return nil
}

func (c *Cluster) volumesNeedResizing() (bool, error) {
	newQuantity, _ := resource.ParseQuantity(c.Spec.Volume.Size)
	newSize := quantityToGigabyte(newQuantity)

	vols, err := c.listPersistentVolumes()
	if err != nil {
		return false, err
	}
	for _, pv := range vols {
		currentSize := quantityToGigabyte(pv.Spec.Capacity[v1.ResourceStorage])
		if currentSize != newSize {
			return true, nil
		}
	}
	return false, nil
}

// getPodNameFromPersistentVolume returns a pod name that it extracts from the volume claim ref.
func getPodNameFromPersistentVolume(pv *v1.PersistentVolume) *spec.NamespacedName {
	namespace := pv.Spec.ClaimRef.Namespace
	name := pv.Spec.ClaimRef.Name[len(constants.DataVolumeName)+1:]
	return &spec.NamespacedName{Namespace: namespace, Name: name}
}

func quantityToGigabyte(q resource.Quantity) int32 {
	return int32(q.ScaledValue(0) / (1 * constants.Gigabyte))
}
