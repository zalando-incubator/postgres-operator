package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/retryutil"
)

const (
	rollingUpdatePodAnnotationKey = "zalando-postgres-operator-rolling-update-required"
)

func (c *Cluster) listResources() error {
	if c.PrimaryPodDisruptionBudget != nil {
		c.logger.Infof("found primary pod disruption budget: %q (uid: %q)", util.NameFromMeta(c.PrimaryPodDisruptionBudget.ObjectMeta), c.PrimaryPodDisruptionBudget.UID)
	}

	if c.CriticalOpPodDisruptionBudget != nil {
		c.logger.Infof("found pod disruption budget for critical operations: %q (uid: %q)", util.NameFromMeta(c.CriticalOpPodDisruptionBudget.ObjectMeta), c.CriticalOpPodDisruptionBudget.UID)

	}

	if c.Statefulset != nil {
		c.logger.Infof("found statefulset: %q (uid: %q)", util.NameFromMeta(c.Statefulset.ObjectMeta), c.Statefulset.UID)
	}

	for appId, stream := range c.Streams {
		c.logger.Infof("found stream: %q with application id %q (uid: %q)", util.NameFromMeta(stream.ObjectMeta), appId, stream.UID)
	}

	if c.LogicalBackupJob != nil {
		c.logger.Infof("found logical backup job: %q (uid: %q)", util.NameFromMeta(c.LogicalBackupJob.ObjectMeta), c.LogicalBackupJob.UID)
	}

	for uid, secret := range c.Secrets {
		c.logger.Infof("found secret: %q (uid: %q) namespace: %s", util.NameFromMeta(secret.ObjectMeta), uid, secret.ObjectMeta.Namespace)
	}

	for role, service := range c.Services {
		c.logger.Infof("found %s service: %q (uid: %q)", role, util.NameFromMeta(service.ObjectMeta), service.UID)
	}

	for role, endpoint := range c.Endpoints {
		c.logger.Infof("found %s endpoint: %q (uid: %q)", role, util.NameFromMeta(endpoint.ObjectMeta), endpoint.UID)
	}

	if c.patroniKubernetesUseConfigMaps() {
		for suffix, configmap := range c.PatroniConfigMaps {
			c.logger.Infof("found %s Patroni config map: %q (uid: %q)", suffix, util.NameFromMeta(configmap.ObjectMeta), configmap.UID)
		}
	} else {
		for suffix, endpoint := range c.PatroniEndpoints {
			c.logger.Infof("found %s Patroni endpoint: %q (uid: %q)", suffix, util.NameFromMeta(endpoint.ObjectMeta), endpoint.UID)
		}
	}

	pods, err := c.listPods()
	if err != nil {
		return fmt.Errorf("could not get the list of pods: %v", err)
	}

	for _, obj := range pods {
		c.logger.Infof("found pod: %q (uid: %q)", util.NameFromMeta(obj.ObjectMeta), obj.UID)
	}

	for uid, pvc := range c.VolumeClaims {
		c.logger.Infof("found persistent volume claim: %q (uid: %q)", util.NameFromMeta(pvc.ObjectMeta), uid)
	}

	for role, poolerObjs := range c.ConnectionPooler {
		if poolerObjs.Deployment != nil {
			c.logger.Infof("found %s pooler deployment: %q (uid: %q) ", role, util.NameFromMeta(poolerObjs.Deployment.ObjectMeta), poolerObjs.Deployment.UID)
		}
		if poolerObjs.Service != nil {
			c.logger.Infof("found %s pooler service: %q (uid: %q) ", role, util.NameFromMeta(poolerObjs.Service.ObjectMeta), poolerObjs.Service.UID)
		}
	}

	return nil
}

func (c *Cluster) createStatefulSet() (*appsv1.StatefulSet, error) {
	c.setProcessName("creating statefulset")
	// check if it's allowed that spec contains initContainers
	if len(c.Spec.InitContainers) > 0 &&
		c.OpConfig.EnableInitContainers != nil && !(*c.OpConfig.EnableInitContainers) {
		return nil, fmt.Errorf("initContainers specified but disabled in configuration")
	}
	// check if it's allowed that spec contains sidecars
	if len(c.Spec.Sidecars) > 0 &&
		c.OpConfig.EnableSidecars != nil && !(*c.OpConfig.EnableSidecars) {
		return nil, fmt.Errorf("sidecar containers specified but disabled in configuration")
	}

	statefulSetSpec, err := c.generateStatefulSet(&c.Spec)
	if err != nil {
		return nil, fmt.Errorf("could not generate statefulset: %v", err)
	}
	statefulSet, err := c.KubeClient.StatefulSets(statefulSetSpec.Namespace).Create(
		context.TODO(),
		statefulSetSpec,
		metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	c.Statefulset = statefulSet
	c.logger.Debugf("created new statefulset %q, uid: %q", util.NameFromMeta(statefulSet.ObjectMeta), statefulSet.UID)

	return statefulSet, nil
}

func getPodIndex(podName string) (int32, error) {
	parts := strings.Split(podName, "-")
	if len(parts) == 0 {
		return 0, fmt.Errorf("pod has no index part")
	}

	postfix := parts[len(parts)-1]
	res, err := strconv.ParseInt(postfix, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("could not parse pod index: %v", err)
	}

	return int32(res), nil
}

func (c *Cluster) preScaleDown(newStatefulSet *appsv1.StatefulSet) error {
	masterPod, err := c.getRolePods(Master)
	if err != nil {
		return fmt.Errorf("could not get master pod: %v", err)
	}
	if len(masterPod) == 0 {
		return fmt.Errorf("no master pod is running in the cluster")
	}

	podNum, err := getPodIndex(masterPod[0].Name)
	if err != nil {
		return fmt.Errorf("could not get pod number: %v", err)
	}

	//Check if scale down affects current master pod
	if *newStatefulSet.Spec.Replicas >= podNum+1 {
		return nil
	}

	podName := fmt.Sprintf("%s-0", c.Statefulset.Name)
	masterCandidatePod, err := c.KubeClient.Pods(c.clusterNamespace()).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not get master candidate pod: %v", err)
	}

	// some sanity check
	if !util.MapContains(masterCandidatePod.Labels, c.OpConfig.ClusterLabels) ||
		!util.MapContains(masterCandidatePod.Labels, map[string]string{c.OpConfig.ClusterNameLabel: c.Name}) {
		return fmt.Errorf("pod %q does not belong to cluster", podName)
	}

	if err := c.patroni.Switchover(&masterPod[0], masterCandidatePod.Name, ""); err != nil {
		return fmt.Errorf("could not switchover: %v", err)
	}

	return nil
}

func (c *Cluster) updateStatefulSet(newStatefulSet *appsv1.StatefulSet) error {
	c.setProcessName("updating statefulset")
	if c.Statefulset == nil {
		return fmt.Errorf("there is no statefulset in the cluster")
	}
	statefulSetName := util.NameFromMeta(c.Statefulset.ObjectMeta)

	//scale down
	if *c.Statefulset.Spec.Replicas > *newStatefulSet.Spec.Replicas {
		if err := c.preScaleDown(newStatefulSet); err != nil {
			c.logger.Warningf("could not scale down: %v", err)
		}
	}
	c.logger.Debug("updating statefulset")

	patchData, err := specPatch(newStatefulSet.Spec)
	if err != nil {
		return fmt.Errorf("could not form patch for the statefulset %q: %v", statefulSetName, err)
	}

	statefulSet, err := c.KubeClient.StatefulSets(c.Statefulset.Namespace).Patch(
		context.TODO(),
		c.Statefulset.Name,
		types.MergePatchType,
		patchData,
		metav1.PatchOptions{},
		"")
	if err != nil {
		return fmt.Errorf("could not patch statefulset spec %q: %v", statefulSetName, err)
	}

	c.Statefulset = statefulSet

	return nil
}

// replaceStatefulSet deletes an old StatefulSet and creates the new using spec in the PostgreSQL CRD.
func (c *Cluster) replaceStatefulSet(newStatefulSet *appsv1.StatefulSet) error {
	c.setProcessName("replacing statefulset")
	if c.Statefulset == nil {
		return fmt.Errorf("there is no statefulset in the cluster")
	}

	statefulSetName := util.NameFromMeta(c.Statefulset.ObjectMeta)
	c.logger.Debug("replacing statefulset")

	// Delete the current statefulset without deleting the pods
	deletePropagationPolicy := metav1.DeletePropagationOrphan
	oldStatefulset := c.Statefulset

	options := metav1.DeleteOptions{PropagationPolicy: &deletePropagationPolicy}
	err := c.KubeClient.StatefulSets(oldStatefulset.Namespace).Delete(context.TODO(), oldStatefulset.Name, options)
	if err != nil {
		return fmt.Errorf("could not delete statefulset %q: %v", statefulSetName, err)
	}
	// make sure we clear the stored statefulset status if the subsequent create fails.
	c.Statefulset = nil
	// wait until the statefulset is truly deleted
	c.logger.Debug("waiting for the statefulset to be deleted")

	err = retryutil.Retry(c.OpConfig.ResourceCheckInterval.Duration, c.OpConfig.ResourceCheckTimeout.Duration,
		func() (bool, error) {
			_, err2 := c.KubeClient.StatefulSets(oldStatefulset.Namespace).Get(context.TODO(), oldStatefulset.Name, metav1.GetOptions{})
			if err2 == nil {
				return false, nil
			}
			if k8sutil.ResourceNotFound(err2) {
				return true, nil
			}
			return false, err2
		})
	if err != nil {
		return fmt.Errorf("could not delete statefulset: %v", err)
	}

	// create the new statefulset with the desired spec. It would take over the remaining pods.
	createdStatefulset, err := c.KubeClient.StatefulSets(newStatefulSet.Namespace).Create(context.TODO(), newStatefulSet, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create statefulset %q: %v", statefulSetName, err)
	}
	// check that all the previous replicas were picked up.
	if newStatefulSet.Spec.Replicas == oldStatefulset.Spec.Replicas &&
		createdStatefulset.Status.Replicas != oldStatefulset.Status.Replicas {
		c.logger.Warningf("number of pods for the old and updated Statefulsets is not identical")
	}

	c.Statefulset = createdStatefulset
	return nil
}

func (c *Cluster) deleteStatefulSet() error {
	c.setProcessName("deleting statefulset")
	c.logger.Debug("deleting statefulset")
	if c.Statefulset == nil {
		c.logger.Debug("there is no statefulset in the cluster")
		return nil
	}

	err := c.KubeClient.StatefulSets(c.Statefulset.Namespace).Delete(context.TODO(), c.Statefulset.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("statefulset %q has already been deleted", util.NameFromMeta(c.Statefulset.ObjectMeta))
	} else if err != nil {
		return err
	}

	c.logger.Infof("statefulset %q has been deleted", util.NameFromMeta(c.Statefulset.ObjectMeta))
	c.Statefulset = nil

	if err := c.deletePods(); err != nil {
		return fmt.Errorf("could not delete pods: %v", err)
	}

	if c.OpConfig.EnablePersistentVolumeClaimDeletion != nil && *c.OpConfig.EnablePersistentVolumeClaimDeletion {
		if err := c.deletePersistentVolumeClaims(); err != nil {
			return fmt.Errorf("could not delete persistent volume claims: %v", err)
		}
	} else {
		c.logger.Info("not deleting persistent volume claims because disabled in configuration")
	}

	return nil
}

func (c *Cluster) createService(role PostgresRole) (*v1.Service, error) {
	c.setProcessName("creating %v service", role)

	serviceSpec := c.generateService(role, &c.Spec)
	service, err := c.KubeClient.Services(serviceSpec.Namespace).Create(context.TODO(), serviceSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	c.Services[role] = service
	return service, nil
}

func (c *Cluster) updateService(role PostgresRole, oldService *v1.Service, newService *v1.Service) (*v1.Service, error) {
	var err error
	svc := oldService

	serviceName := util.NameFromMeta(oldService.ObjectMeta)
	match, reason := c.compareServices(oldService, newService)
	if !match {
		c.logServiceChanges(role, oldService, newService, false, reason)
		c.setProcessName("updating %v service", role)

		// now, patch the service spec, but when disabling LoadBalancers do update instead
		// patch does not work because of LoadBalancerSourceRanges field (even if set to nil)
		oldServiceType := oldService.Spec.Type
		newServiceType := newService.Spec.Type
		if newServiceType != oldServiceType && oldServiceType == v1.ServiceTypeLoadBalancer {
			// Kubernetes rejects updates that change type away from LoadBalancer while
			// loadBalancerSourceRanges is still set; clear it before updating
			newService.Spec.LoadBalancerSourceRanges = nil
			newService.ResourceVersion = oldService.ResourceVersion
			if newServiceType == v1.ServiceTypeClusterIP {
				newService.Spec.ClusterIP = oldService.Spec.ClusterIP
			}
		}
		svc, err = c.KubeClient.Services(serviceName.Namespace).Update(context.TODO(), newService, metav1.UpdateOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not update service %q: %v", serviceName, err)
		}
	}

	if changed, _ := c.compareAnnotations(oldService.Annotations, newService.Annotations, nil); changed {
		patchData, err := metaAnnotationsPatch(newService.Annotations)
		if err != nil {
			return nil, fmt.Errorf("could not form patch for service %q annotations: %v", oldService.Name, err)
		}
		svc, err = c.KubeClient.Services(serviceName.Namespace).Patch(context.TODO(), newService.Name, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not patch annotations for service %q: %v", oldService.Name, err)
		}
	}

	return svc, nil
}

func (c *Cluster) deleteService(role PostgresRole) error {
	c.setProcessName("deleting service")
	c.logger.Debugf("deleting %s service", role)

	if c.Services[role] == nil {
		c.logger.Debugf("No service for %s role was found, nothing to delete", role)
		return nil
	}

	if err := c.KubeClient.Services(c.Services[role].Namespace).Delete(context.TODO(), c.Services[role].Name, c.deleteOptions); err != nil {
		if !k8sutil.ResourceNotFound(err) {
			return fmt.Errorf("could not delete %s service: %v", role, err)
		}
		c.logger.Debugf("%s service has already been deleted", role)
	}

	c.logger.Infof("%s service %q has been deleted", role, util.NameFromMeta(c.Services[role].ObjectMeta))
	delete(c.Services, role)

	return nil
}

func (c *Cluster) createEndpoint(role PostgresRole) (*v1.Endpoints, error) {
	var (
		subsets []v1.EndpointSubset
	)
	c.setProcessName("creating endpoint")
	if !c.isNewCluster() {
		subsets = c.generateEndpointSubsets(role)
	} else {
		// Patroni will populate the master endpoint for the new cluster
		// The replica endpoint will be filled-in by the service selector.
		subsets = make([]v1.EndpointSubset, 0)
	}
	endpointsSpec := c.generateEndpoint(role, subsets)

	endpoints, err := c.KubeClient.Endpoints(endpointsSpec.Namespace).Create(context.TODO(), endpointsSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("could not create %s endpoint: %v", role, err)
	}

	c.Endpoints[role] = endpoints

	return endpoints, nil
}

func (c *Cluster) generateEndpointSubsets(role PostgresRole) []v1.EndpointSubset {
	result := make([]v1.EndpointSubset, 0)
	pods, err := c.getRolePods(role)
	if err != nil {
		if role == Master {
			c.logger.Warningf("could not obtain the address for %s pod: %v", role, err)
		} else {
			c.logger.Warningf("could not obtain the addresses for %s pods: %v", role, err)
		}
		return result
	}

	endPointAddresses := make([]v1.EndpointAddress, 0)
	for _, pod := range pods {
		endPointAddresses = append(endPointAddresses, v1.EndpointAddress{IP: pod.Status.PodIP})
	}
	if len(endPointAddresses) > 0 {
		result = append(result, v1.EndpointSubset{
			Addresses: endPointAddresses,
			Ports:     []v1.EndpointPort{{Name: "postgresql", Port: 5432, Protocol: "TCP"}},
		})
	} else if role == Master {
		c.logger.Warningf("master is not running, generated master endpoint does not contain any addresses")
	}

	return result
}

func (c *Cluster) createPrimaryPodDisruptionBudget() error {
	c.logger.Debug("creating primary pod disruption budget")
	if c.PrimaryPodDisruptionBudget != nil {
		c.logger.Warning("primary pod disruption budget already exists in the cluster")
		return nil
	}

	podDisruptionBudgetSpec := c.generatePrimaryPodDisruptionBudget()
	podDisruptionBudget, err := c.KubeClient.
		PodDisruptionBudgets(podDisruptionBudgetSpec.Namespace).
		Create(context.TODO(), podDisruptionBudgetSpec, metav1.CreateOptions{})

	if err != nil {
		return err
	}
	c.logger.Infof("primary pod disruption budget %q has been successfully created", util.NameFromMeta(podDisruptionBudget.ObjectMeta))
	c.PrimaryPodDisruptionBudget = podDisruptionBudget

	return nil
}

func (c *Cluster) createCriticalOpPodDisruptionBudget() error {
	c.logger.Debug("creating pod disruption budget for critical operations")
	if c.CriticalOpPodDisruptionBudget != nil {
		c.logger.Warning("pod disruption budget for critical operations already exists in the cluster")
		return nil
	}

	podDisruptionBudgetSpec := c.generateCriticalOpPodDisruptionBudget()
	podDisruptionBudget, err := c.KubeClient.
		PodDisruptionBudgets(podDisruptionBudgetSpec.Namespace).
		Create(context.TODO(), podDisruptionBudgetSpec, metav1.CreateOptions{})

	if err != nil {
		return err
	}
	c.logger.Infof("pod disruption budget for critical operations %q has been successfully created", util.NameFromMeta(podDisruptionBudget.ObjectMeta))
	c.CriticalOpPodDisruptionBudget = podDisruptionBudget

	return nil
}

func (c *Cluster) createPodDisruptionBudgets() error {
	errors := make([]string, 0)

	err := c.createPrimaryPodDisruptionBudget()
	if err != nil {
		errors = append(errors, fmt.Sprintf("could not create primary pod disruption budget: %v", err))
	}

	err = c.createCriticalOpPodDisruptionBudget()
	if err != nil {
		errors = append(errors, fmt.Sprintf("could not create pod disruption budget for critical operations: %v", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}
	return nil
}

func (c *Cluster) updatePrimaryPodDisruptionBudget(pdb *policyv1.PodDisruptionBudget) error {
	c.logger.Debug("updating primary pod disruption budget")
	if c.PrimaryPodDisruptionBudget == nil {
		return fmt.Errorf("there is no primary pod disruption budget in the cluster")
	}

	if err := c.deletePrimaryPodDisruptionBudget(); err != nil {
		return fmt.Errorf("could not delete primary pod disruption budget: %v", err)
	}

	newPdb, err := c.KubeClient.
		PodDisruptionBudgets(pdb.Namespace).
		Create(context.TODO(), pdb, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create primary pod disruption budget: %v", err)
	}
	c.PrimaryPodDisruptionBudget = newPdb

	return nil
}

func (c *Cluster) updateCriticalOpPodDisruptionBudget(pdb *policyv1.PodDisruptionBudget) error {
	c.logger.Debug("updating pod disruption budget for critical operations")
	if c.CriticalOpPodDisruptionBudget == nil {
		return fmt.Errorf("there is no pod disruption budget for critical operations in the cluster")
	}

	if err := c.deleteCriticalOpPodDisruptionBudget(); err != nil {
		return fmt.Errorf("could not delete pod disruption budget for critical operations: %v", err)
	}

	newPdb, err := c.KubeClient.
		PodDisruptionBudgets(pdb.Namespace).
		Create(context.TODO(), pdb, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create pod disruption budget for critical operations: %v", err)
	}
	c.CriticalOpPodDisruptionBudget = newPdb

	return nil
}

func (c *Cluster) deletePrimaryPodDisruptionBudget() error {
	c.logger.Debug("deleting primary pod disruption budget")
	if c.PrimaryPodDisruptionBudget == nil {
		c.logger.Debug("there is no primary pod disruption budget in the cluster")
		return nil
	}

	pdbName := util.NameFromMeta(c.PrimaryPodDisruptionBudget.ObjectMeta)
	err := c.KubeClient.
		PodDisruptionBudgets(c.PrimaryPodDisruptionBudget.Namespace).
		Delete(context.TODO(), c.PrimaryPodDisruptionBudget.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("PodDisruptionBudget %q has already been deleted", util.NameFromMeta(c.PrimaryPodDisruptionBudget.ObjectMeta))
	} else if err != nil {
		return fmt.Errorf("could not delete primary pod disruption budget: %v", err)
	}

	c.logger.Infof("pod disruption budget %q has been deleted", util.NameFromMeta(c.PrimaryPodDisruptionBudget.ObjectMeta))
	c.PrimaryPodDisruptionBudget = nil

	err = retryutil.Retry(c.OpConfig.ResourceCheckInterval.Duration, c.OpConfig.ResourceCheckTimeout.Duration,
		func() (bool, error) {
			_, err2 := c.KubeClient.PodDisruptionBudgets(pdbName.Namespace).Get(context.TODO(), pdbName.Name, metav1.GetOptions{})
			if err2 == nil {
				return false, nil
			}
			if k8sutil.ResourceNotFound(err2) {
				return true, nil
			}
			return false, err2
		})
	if err != nil {
		return fmt.Errorf("could not delete primary pod disruption budget: %v", err)
	}

	return nil
}

func (c *Cluster) deleteCriticalOpPodDisruptionBudget() error {
	c.logger.Debug("deleting pod disruption budget for critical operations")
	if c.CriticalOpPodDisruptionBudget == nil {
		c.logger.Debug("there is no pod disruption budget for critical operations in the cluster")
		return nil
	}

	pdbName := util.NameFromMeta(c.CriticalOpPodDisruptionBudget.ObjectMeta)
	err := c.KubeClient.
		PodDisruptionBudgets(c.CriticalOpPodDisruptionBudget.Namespace).
		Delete(context.TODO(), c.CriticalOpPodDisruptionBudget.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("PodDisruptionBudget %q has already been deleted", util.NameFromMeta(c.CriticalOpPodDisruptionBudget.ObjectMeta))
	} else if err != nil {
		return fmt.Errorf("could not delete pod disruption budget for critical operations: %v", err)
	}

	c.logger.Infof("pod disruption budget %q has been deleted", util.NameFromMeta(c.CriticalOpPodDisruptionBudget.ObjectMeta))
	c.CriticalOpPodDisruptionBudget = nil

	err = retryutil.Retry(c.OpConfig.ResourceCheckInterval.Duration, c.OpConfig.ResourceCheckTimeout.Duration,
		func() (bool, error) {
			_, err2 := c.KubeClient.PodDisruptionBudgets(pdbName.Namespace).Get(context.TODO(), pdbName.Name, metav1.GetOptions{})
			if err2 == nil {
				return false, nil
			}
			if k8sutil.ResourceNotFound(err2) {
				return true, nil
			}
			return false, err2
		})
	if err != nil {
		return fmt.Errorf("could not delete pod disruption budget for critical operations: %v", err)
	}

	return nil
}

func (c *Cluster) deletePodDisruptionBudgets() error {
	errors := make([]string, 0)

	if err := c.deletePrimaryPodDisruptionBudget(); err != nil {
		errors = append(errors, fmt.Sprintf("%v", err))
	}

	if err := c.deleteCriticalOpPodDisruptionBudget(); err != nil {
		errors = append(errors, fmt.Sprintf("%v", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}
	return nil
}

func (c *Cluster) deleteEndpoint(role PostgresRole) error {
	c.setProcessName("deleting endpoint")
	c.logger.Debugf("deleting %s endpoint", role)
	if c.Endpoints[role] == nil {
		c.logger.Debugf("there is no %s endpoint in the cluster", role)
		return nil
	}

	if err := c.KubeClient.Endpoints(c.Endpoints[role].Namespace).Delete(context.TODO(), c.Endpoints[role].Name, c.deleteOptions); err != nil {
		if !k8sutil.ResourceNotFound(err) {
			return fmt.Errorf("could not delete %s endpoint: %v", role, err)
		}
		c.logger.Debugf("%s endpoint has already been deleted", role)
	}

	c.logger.Infof("%s endpoint %q has been deleted", role, util.NameFromMeta(c.Endpoints[role].ObjectMeta))
	delete(c.Endpoints, role)

	return nil
}

func (c *Cluster) deletePatroniResources() error {
	c.setProcessName("deleting Patroni resources")
	errors := make([]string, 0)

	if err := c.deleteService(Patroni); err != nil {
		errors = append(errors, fmt.Sprintf("%v", err))
	}

	for _, suffix := range patroniObjectSuffixes {
		if c.patroniKubernetesUseConfigMaps() {
			if err := c.deletePatroniConfigMap(suffix); err != nil {
				errors = append(errors, fmt.Sprintf("%v", err))
			}
		} else {
			if err := c.deletePatroniEndpoint(suffix); err != nil {
				errors = append(errors, fmt.Sprintf("%v", err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) deletePatroniConfigMap(suffix string) error {
	c.setProcessName("deleting Patroni config map")
	c.logger.Debugf("deleting %s Patroni config map", suffix)
	cm := c.PatroniConfigMaps[suffix]
	if cm == nil {
		c.logger.Debugf("there is no %s Patroni config map in the cluster", suffix)
		return nil
	}

	if err := c.KubeClient.ConfigMaps(cm.Namespace).Delete(context.TODO(), cm.Name, c.deleteOptions); err != nil {
		if !k8sutil.ResourceNotFound(err) {
			return fmt.Errorf("could not delete %s Patroni config map %q: %v", suffix, cm.Name, err)
		}
		c.logger.Debugf("%s Patroni config map has already been deleted", suffix)
	}

	c.logger.Infof("%s Patroni config map %q has been deleted", suffix, util.NameFromMeta(cm.ObjectMeta))
	delete(c.PatroniConfigMaps, suffix)

	return nil
}

func (c *Cluster) deletePatroniEndpoint(suffix string) error {
	c.setProcessName("deleting Patroni endpoint")
	c.logger.Debugf("deleting %s Patroni endpoint", suffix)
	ep := c.PatroniEndpoints[suffix]
	if ep == nil {
		c.logger.Debugf("there is no %s Patroni endpoint in the cluster", suffix)
		return nil
	}

	if err := c.KubeClient.Endpoints(ep.Namespace).Delete(context.TODO(), ep.Name, c.deleteOptions); err != nil {
		if !k8sutil.ResourceNotFound(err) {
			return fmt.Errorf("could not delete %s Patroni endpoint %q: %v", suffix, ep.Name, err)
		}
		c.logger.Debugf("%s Patroni endpoint has already been deleted", suffix)
	}

	c.logger.Infof("%s Patroni endpoint %q has been deleted", suffix, util.NameFromMeta(ep.ObjectMeta))
	delete(c.PatroniEndpoints, suffix)

	return nil
}

func (c *Cluster) deleteSecrets() error {
	c.setProcessName("deleting secrets")
	errors := make([]string, 0)

	for uid := range c.Secrets {
		err := c.deleteSecret(uid)
		if err != nil {
			errors = append(errors, fmt.Sprintf("%v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("could not delete all secrets: %v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) deleteSecret(uid types.UID) error {
	c.setProcessName("deleting secret")
	secret := c.Secrets[uid]
	secretName := util.NameFromMeta(secret.ObjectMeta)
	c.logger.Debugf("deleting secret %q", secretName)
	err := c.KubeClient.Secrets(secret.Namespace).Delete(context.TODO(), secret.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("secret %q has already been deleted", secretName)
	} else if err != nil {
		return fmt.Errorf("could not delete secret %q: %v", secretName, err)
	}
	c.logger.Infof("secret %q has been deleted", secretName)
	delete(c.Secrets, uid)

	return nil
}

func (c *Cluster) createRoles() (err error) {
	// TODO: figure out what to do with duplicate names (humans and robots) among pgUsers
	return c.syncRoles()
}

func (c *Cluster) createLogicalBackupJob() (err error) {

	c.setProcessName("creating a k8s cron job for logical backups")

	logicalBackupJobSpec, err := c.generateLogicalBackupJob()
	if err != nil {
		return fmt.Errorf("could not generate k8s cron job spec: %v", err)
	}

	cronJob, err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Create(context.TODO(), logicalBackupJobSpec, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create k8s cron job: %v", err)
	}
	c.LogicalBackupJob = cronJob

	return nil
}

func (c *Cluster) patchLogicalBackupJob(newJob *batchv1.CronJob) error {
	c.setProcessName("patching logical backup job")

	patchData, err := specPatch(newJob.Spec)
	if err != nil {
		return fmt.Errorf("could not form patch for the logical backup job: %v", err)
	}

	// update the backup job spec
	cronJob, err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Patch(
		context.TODO(),
		c.getLogicalBackupJobName(),
		types.MergePatchType,
		patchData,
		metav1.PatchOptions{},
		"")
	if err != nil {
		return fmt.Errorf("could not patch logical backup job: %v", err)
	}
	c.LogicalBackupJob = cronJob

	return nil
}

func (c *Cluster) deleteLogicalBackupJob() error {
	if c.LogicalBackupJob == nil {
		return nil
	}
	c.logger.Info("removing the logical backup job")

	err := c.KubeClient.CronJobsGetter.CronJobs(c.LogicalBackupJob.Namespace).Delete(context.TODO(), c.getLogicalBackupJobName(), c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("logical backup cron job %q has already been deleted", c.getLogicalBackupJobName())
	} else if err != nil {
		return err
	}
	c.LogicalBackupJob = nil

	return nil
}

// GetServiceMaster returns cluster's kubernetes master Service
func (c *Cluster) GetServiceMaster() *v1.Service {
	return c.Services[Master]
}

// GetServiceReplica returns cluster's kubernetes replica Service
func (c *Cluster) GetServiceReplica() *v1.Service {
	return c.Services[Replica]
}

// GetEndpointMaster returns cluster's kubernetes master Endpoint
func (c *Cluster) GetEndpointMaster() *v1.Endpoints {
	return c.Endpoints[Master]
}

// GetEndpointReplica returns cluster's kubernetes replica Endpoint
func (c *Cluster) GetEndpointReplica() *v1.Endpoints {
	return c.Endpoints[Replica]
}

// GetStatefulSet returns cluster's kubernetes StatefulSet
func (c *Cluster) GetStatefulSet() *appsv1.StatefulSet {
	return c.Statefulset
}

// GetPrimaryPodDisruptionBudget returns cluster's primary kubernetes PodDisruptionBudget
func (c *Cluster) GetPrimaryPodDisruptionBudget() *policyv1.PodDisruptionBudget {
	return c.PrimaryPodDisruptionBudget
}

// GetCriticalOpPodDisruptionBudget returns cluster's kubernetes PodDisruptionBudget for critical operations
func (c *Cluster) GetCriticalOpPodDisruptionBudget() *policyv1.PodDisruptionBudget {
	return c.CriticalOpPodDisruptionBudget
}
