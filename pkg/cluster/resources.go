package cluster

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	v1 "k8s.io/api/core/v1"
	policybeta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/pkg/util/retryutil"
)

const (
	rollingUpdateStatefulsetAnnotationKey = "zalando-postgres-operator-rolling-update-required"
)

func (c *Cluster) listResources() error {
	if c.PodDisruptionBudget != nil {
		c.logger.Infof("found pod disruption budget: %q (uid: %q)", util.NameFromMeta(c.PodDisruptionBudget.ObjectMeta), c.PodDisruptionBudget.UID)
	}

	if c.Statefulset != nil {
		c.logger.Infof("found statefulset: %q (uid: %q)", util.NameFromMeta(c.Statefulset.ObjectMeta), c.Statefulset.UID)
	}

	for _, obj := range c.Secrets {
		c.logger.Infof("found secret: %q (uid: %q)", util.NameFromMeta(obj.ObjectMeta), obj.UID)
	}

	for role, endpoint := range c.Endpoints {
		c.logger.Infof("found %s endpoint: %q (uid: %q)", role, util.NameFromMeta(endpoint.ObjectMeta), endpoint.UID)
	}

	for role, service := range c.Services {
		c.logger.Infof("found %s service: %q (uid: %q)", role, util.NameFromMeta(service.ObjectMeta), service.UID)
	}

	pods, err := c.listPods()
	if err != nil {
		return fmt.Errorf("could not get the list of pods: %v", err)
	}

	for _, obj := range pods {
		c.logger.Infof("found pod: %q (uid: %q)", util.NameFromMeta(obj.ObjectMeta), obj.UID)
	}

	pvcs, err := c.listPersistentVolumeClaims()
	if err != nil {
		return fmt.Errorf("could not get the list of PVCs: %v", err)
	}

	for _, obj := range pvcs {
		c.logger.Infof("found PVC: %q (uid: %q)", util.NameFromMeta(obj.ObjectMeta), obj.UID)
	}

	return nil
}

func (c *Cluster) createStatefulSet() (*appsv1.StatefulSet, error) {
	c.setProcessName("creating statefulset")
	// check if it's allowed that spec contains initContainers
	if c.Spec.InitContainers != nil && len(c.Spec.InitContainers) > 0 &&
		c.OpConfig.EnableInitContainers != nil && !(*c.OpConfig.EnableInitContainers) {
		return nil, fmt.Errorf("initContainers specified but disabled in configuration")
	}
	// check if it's allowed that spec contains sidecars
	if c.Spec.Sidecars != nil && len(c.Spec.Sidecars) > 0 &&
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

// Prepare the database for connection pooler to be used, i.e. install lookup
// function (do it first, because it should be fast and if it didn't succeed,
// it doesn't makes sense to create more K8S objects. At this moment we assume
// that necessary connection pooler user exists.
//
// After that create all the objects for connection pooler, namely a deployment
// with a chosen pooler and a service to expose it.
func (c *Cluster) createConnectionPooler(lookup InstallFunction) (*ConnectionPoolerObjects, error) {
	var msg string
	c.setProcessName("creating connection pooler")

	if c.ConnectionPooler == nil {
		c.ConnectionPooler = &ConnectionPoolerObjects{}
	}

	schema := c.Spec.ConnectionPooler.Schema

	if schema == "" {
		schema = c.OpConfig.ConnectionPooler.Schema
	}

	user := c.Spec.ConnectionPooler.User
	if user == "" {
		user = c.OpConfig.ConnectionPooler.User
	}

	err := lookup(schema, user)

	if err != nil {
		msg = "could not prepare database for connection pooler: %v"
		return nil, fmt.Errorf(msg, err)
	}

	deploymentSpec, err := c.generateConnectionPoolerDeployment(&c.Spec)
	if err != nil {
		msg = "could not generate deployment for connection pooler: %v"
		return nil, fmt.Errorf(msg, err)
	}

	// client-go does retry 10 times (with NoBackoff by default) when the API
	// believe a request can be retried and returns Retry-After header. This
	// should be good enough to not think about it here.
	deployment, err := c.KubeClient.
		Deployments(deploymentSpec.Namespace).
		Create(context.TODO(), deploymentSpec, metav1.CreateOptions{})

	if err != nil {
		return nil, err
	}

	serviceSpec := c.generateConnectionPoolerService(&c.Spec)
	service, err := c.KubeClient.
		Services(serviceSpec.Namespace).
		Create(context.TODO(), serviceSpec, metav1.CreateOptions{})

	if err != nil {
		return nil, err
	}

	c.ConnectionPooler = &ConnectionPoolerObjects{
		Deployment: deployment,
		Service:    service,
	}
	c.logger.Debugf("created new connection pooler %q, uid: %q",
		util.NameFromMeta(deployment.ObjectMeta), deployment.UID)

	return c.ConnectionPooler, nil
}

func (c *Cluster) deleteConnectionPooler() (err error) {
	c.setProcessName("deleting connection pooler")
	c.logger.Debugln("deleting connection pooler")

	// Lack of connection pooler objects is not a fatal error, just log it if
	// it was present before in the manifest
	if c.ConnectionPooler == nil {
		c.logger.Infof("No connection pooler to delete")
		return nil
	}

	// Clean up the deployment object. If deployment resource we've remembered
	// is somehow empty, try to delete based on what would we generate
	deploymentName := c.connectionPoolerName()
	deployment := c.ConnectionPooler.Deployment

	if deployment != nil {
		deploymentName = deployment.Name
	}

	// set delete propagation policy to foreground, so that replica set will be
	// also deleted.
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{PropagationPolicy: &policy}
	err = c.KubeClient.
		Deployments(c.Namespace).
		Delete(context.TODO(), deploymentName, options)

	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("Connection pooler deployment was already deleted")
	} else if err != nil {
		return fmt.Errorf("could not delete deployment: %v", err)
	}

	c.logger.Infof("Connection pooler deployment %q has been deleted", deploymentName)

	// Repeat the same for the service object
	service := c.ConnectionPooler.Service
	serviceName := c.connectionPoolerName()

	if service != nil {
		serviceName = service.Name
	}

	// set delete propagation policy to foreground, so that all the dependant
	// will be deleted.
	err = c.KubeClient.
		Services(c.Namespace).
		Delete(context.TODO(), serviceName, options)

	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("Connection pooler service was already deleted")
	} else if err != nil {
		return fmt.Errorf("could not delete service: %v", err)
	}

	c.logger.Infof("Connection pooler service %q has been deleted", serviceName)

	c.ConnectionPooler = nil
	return nil
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

	if err := c.patroni.Switchover(&masterPod[0], masterCandidatePod.Name); err != nil {
		return fmt.Errorf("could not failover: %v", err)
	}

	return nil
}

// setRollingUpdateFlagForStatefulSet sets the indicator or the rolling update requirement
// in the StatefulSet annotation.
func (c *Cluster) setRollingUpdateFlagForStatefulSet(sset *appsv1.StatefulSet, val bool) {
	anno := sset.GetAnnotations()
	if anno == nil {
		anno = make(map[string]string)
	}

	anno[rollingUpdateStatefulsetAnnotationKey] = strconv.FormatBool(val)
	sset.SetAnnotations(anno)
	c.logger.Debugf("statefulset's rolling update annotation has been set to %t", val)
}

// applyRollingUpdateFlagforStatefulSet sets the rolling update flag for the cluster's StatefulSet
// and applies that setting to the actual running cluster.
func (c *Cluster) applyRollingUpdateFlagforStatefulSet(val bool) error {
	c.setRollingUpdateFlagForStatefulSet(c.Statefulset, val)
	sset, err := c.updateStatefulSetAnnotations(c.Statefulset.GetAnnotations())
	if err != nil {
		return err
	}
	c.Statefulset = sset
	return nil
}

// getRollingUpdateFlagFromStatefulSet returns the value of the rollingUpdate flag from the passed
// StatefulSet, reverting to the default value in case of errors
func (c *Cluster) getRollingUpdateFlagFromStatefulSet(sset *appsv1.StatefulSet, defaultValue bool) (flag bool) {
	anno := sset.GetAnnotations()
	flag = defaultValue

	stringFlag, exists := anno[rollingUpdateStatefulsetAnnotationKey]
	if exists {
		var err error
		if flag, err = strconv.ParseBool(stringFlag); err != nil {
			c.logger.Warnf("error when parsing %q annotation for the statefulset %q: expected boolean value, got %q\n",
				rollingUpdateStatefulsetAnnotationKey,
				types.NamespacedName{Namespace: sset.Namespace, Name: sset.Name},
				stringFlag)
			flag = defaultValue
		}
	}
	return flag
}

// mergeRollingUpdateFlagUsingCache returns the value of the rollingUpdate flag from the passed
// statefulset, however, the value can be cleared if there is a cached flag in the cluster that
// is set to false (the discrepancy could be a result of a failed StatefulSet update)
func (c *Cluster) mergeRollingUpdateFlagUsingCache(runningStatefulSet *appsv1.StatefulSet) bool {
	var (
		cachedStatefulsetExists, clearRollingUpdateFromCache, podsRollingUpdateRequired bool
	)

	if c.Statefulset != nil {
		// if we reset the rolling update flag in the statefulset structure in memory but didn't manage to update
		// the actual object in Kubernetes for some reason we want to avoid doing an unnecessary update by relying
		// on the 'cached' in-memory flag.
		cachedStatefulsetExists = true
		clearRollingUpdateFromCache = !c.getRollingUpdateFlagFromStatefulSet(c.Statefulset, true)
		c.logger.Debugf("cached StatefulSet value exists, rollingUpdate flag is %t", clearRollingUpdateFromCache)
	}

	if podsRollingUpdateRequired = c.getRollingUpdateFlagFromStatefulSet(runningStatefulSet, false); podsRollingUpdateRequired {
		if cachedStatefulsetExists && clearRollingUpdateFromCache {
			c.logger.Infof("clearing the rolling update flag based on the cached information")
			podsRollingUpdateRequired = false
		} else {
			c.logger.Infof("found a statefulset with an unfinished rolling update of the pods")

		}
	}
	return podsRollingUpdateRequired
}

func (c *Cluster) updateStatefulSetAnnotations(annotations map[string]string) (*appsv1.StatefulSet, error) {
	c.logger.Debugf("updating statefulset annotations")
	patchData, err := metaAnnotationsPatch(annotations)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the statefulset metadata: %v", err)
	}
	result, err := c.KubeClient.StatefulSets(c.Statefulset.Namespace).Patch(
		context.TODO(),
		c.Statefulset.Name,
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch statefulset annotations %q: %v", patchData, err)
	}
	return result, nil

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
	c.logger.Debugf("updating statefulset")

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

	if newStatefulSet.Annotations != nil {
		statefulSet, err = c.updateStatefulSetAnnotations(newStatefulSet.Annotations)
		if err != nil {
			return err
		}
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
	c.logger.Debugf("replacing statefulset")

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
	c.logger.Debugf("waiting for the statefulset to be deleted")

	err = retryutil.Retry(c.OpConfig.ResourceCheckInterval, c.OpConfig.ResourceCheckTimeout,
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
	c.logger.Debugln("deleting statefulset")
	if c.Statefulset == nil {
		c.logger.Debugf("there is no statefulset in the cluster")
		return nil
	}

	err := c.KubeClient.StatefulSets(c.Statefulset.Namespace).Delete(context.TODO(), c.Statefulset.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("StatefulSet was already deleted")
	} else if err != nil {
		return err
	}

	c.logger.Infof("statefulset %q has been deleted", util.NameFromMeta(c.Statefulset.ObjectMeta))
	c.Statefulset = nil

	if err := c.deletePods(); err != nil {
		return fmt.Errorf("could not delete pods: %v", err)
	}

	if err := c.deletePersistentVolumeClaims(); err != nil {
		return fmt.Errorf("could not delete PersistentVolumeClaims: %v", err)
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

func (c *Cluster) updateService(role PostgresRole, newService *v1.Service) error {
	var (
		svc *v1.Service
		err error
	)

	c.setProcessName("updating %v service", role)

	if c.Services[role] == nil {
		return fmt.Errorf("there is no service in the cluster")
	}

	serviceName := util.NameFromMeta(c.Services[role].ObjectMeta)

	// update the service annotation in order to propagate ELB notation.
	if len(newService.ObjectMeta.Annotations) > 0 {
		if annotationsPatchData, err := metaAnnotationsPatch(newService.ObjectMeta.Annotations); err == nil {
			_, err = c.KubeClient.Services(serviceName.Namespace).Patch(
				context.TODO(),
				serviceName.Name,
				types.MergePatchType,
				[]byte(annotationsPatchData),
				metav1.PatchOptions{},
				"")

			if err != nil {
				return fmt.Errorf("could not replace annotations for the service %q: %v", serviceName, err)
			}
		} else {
			return fmt.Errorf("could not form patch for the service metadata: %v", err)
		}
	}

	// now, patch the service spec, but when disabling LoadBalancers do update instead
	// patch does not work because of LoadBalancerSourceRanges field (even if set to nil)
	oldServiceType := c.Services[role].Spec.Type
	newServiceType := newService.Spec.Type
	if newServiceType == "ClusterIP" && newServiceType != oldServiceType {
		newService.ResourceVersion = c.Services[role].ResourceVersion
		newService.Spec.ClusterIP = c.Services[role].Spec.ClusterIP
		svc, err = c.KubeClient.Services(serviceName.Namespace).Update(context.TODO(), newService, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("could not update service %q: %v", serviceName, err)
		}
	} else {
		patchData, err := specPatch(newService.Spec)
		if err != nil {
			return fmt.Errorf("could not form patch for the service %q: %v", serviceName, err)
		}

		svc, err = c.KubeClient.Services(serviceName.Namespace).Patch(
			context.TODO(), serviceName.Name, types.MergePatchType, patchData, metav1.PatchOptions{}, "")
		if err != nil {
			return fmt.Errorf("could not patch service %q: %v", serviceName, err)
		}
	}
	c.Services[role] = svc

	return nil
}

func (c *Cluster) deleteService(role PostgresRole) error {
	c.logger.Debugf("deleting service %s", role)

	if c.Services[role] == nil {
		c.logger.Debugf("No service for %s role was found, nothing to delete", role)
		return nil
	}

	if err := c.KubeClient.Services(c.Services[role].Namespace).Delete(context.TODO(), c.Services[role].Name, c.deleteOptions); err != nil {
		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("Service was already deleted")
		} else if err != nil {
			return err
		}
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

func (c *Cluster) createPodDisruptionBudget() (*policybeta1.PodDisruptionBudget, error) {
	podDisruptionBudgetSpec := c.generatePodDisruptionBudget()
	podDisruptionBudget, err := c.KubeClient.
		PodDisruptionBudgets(podDisruptionBudgetSpec.Namespace).
		Create(context.TODO(), podDisruptionBudgetSpec, metav1.CreateOptions{})

	if err != nil {
		return nil, err
	}
	c.PodDisruptionBudget = podDisruptionBudget

	return podDisruptionBudget, nil
}

func (c *Cluster) updatePodDisruptionBudget(pdb *policybeta1.PodDisruptionBudget) error {
	if c.PodDisruptionBudget == nil {
		return fmt.Errorf("there is no pod disruption budget in the cluster")
	}

	if err := c.deletePodDisruptionBudget(); err != nil {
		return fmt.Errorf("could not delete pod disruption budget: %v", err)
	}

	newPdb, err := c.KubeClient.
		PodDisruptionBudgets(pdb.Namespace).
		Create(context.TODO(), pdb, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create pod disruption budget: %v", err)
	}
	c.PodDisruptionBudget = newPdb

	return nil
}

func (c *Cluster) deletePodDisruptionBudget() error {
	c.logger.Debug("deleting pod disruption budget")
	if c.PodDisruptionBudget == nil {
		c.logger.Debugf("there is no pod disruption budget in the cluster")
		return nil
	}

	pdbName := util.NameFromMeta(c.PodDisruptionBudget.ObjectMeta)
	err := c.KubeClient.
		PodDisruptionBudgets(c.PodDisruptionBudget.Namespace).
		Delete(context.TODO(), c.PodDisruptionBudget.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("PodDisruptionBudget was already deleted")
	} else if err != nil {
		return fmt.Errorf("could not delete PodDisruptionBudget: %v", err)
	}

	c.logger.Infof("pod disruption budget %q has been deleted", util.NameFromMeta(c.PodDisruptionBudget.ObjectMeta))
	c.PodDisruptionBudget = nil

	err = retryutil.Retry(c.OpConfig.ResourceCheckInterval, c.OpConfig.ResourceCheckTimeout,
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
		return fmt.Errorf("could not delete pod disruption budget: %v", err)
	}

	return nil
}

func (c *Cluster) deleteEndpoint(role PostgresRole) error {
	c.setProcessName("deleting endpoint")
	c.logger.Debugln("deleting endpoint")
	if c.Endpoints[role] == nil {
		c.logger.Debugf("there is no %s endpoint in the cluster", role)
		return nil
	}

	if err := c.KubeClient.Endpoints(c.Endpoints[role].Namespace).Delete(context.TODO(), c.Endpoints[role].Name, c.deleteOptions); err != nil {
		if k8sutil.ResourceNotFound(err) {
			c.logger.Debugf("Endpoint was already deleted")
		} else if err != nil {
			return fmt.Errorf("could not delete endpoint: %v", err)
		}
	}

	c.logger.Infof("endpoint %q has been deleted", util.NameFromMeta(c.Endpoints[role].ObjectMeta))
	delete(c.Endpoints, role)

	return nil
}

func (c *Cluster) deleteSecret(secret *v1.Secret) error {
	c.setProcessName("deleting secret %q", util.NameFromMeta(secret.ObjectMeta))
	c.logger.Debugf("deleting secret %q", util.NameFromMeta(secret.ObjectMeta))
	err := c.KubeClient.Secrets(secret.Namespace).Delete(context.TODO(), secret.Name, c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("Secret was already deleted")
	} else if err != nil {
		return err
	}
	c.logger.Infof("secret %q has been deleted", util.NameFromMeta(secret.ObjectMeta))
	delete(c.Secrets, secret.UID)

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
	c.logger.Debugf("Generated cronJobSpec: %v", logicalBackupJobSpec)

	_, err = c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Create(context.TODO(), logicalBackupJobSpec, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("could not create k8s cron job: %v", err)
	}

	return nil
}

func (c *Cluster) patchLogicalBackupJob(newJob *batchv1beta1.CronJob) error {
	c.setProcessName("patching logical backup job")

	patchData, err := specPatch(newJob.Spec)
	if err != nil {
		return fmt.Errorf("could not form patch for the logical backup job: %v", err)
	}

	// update the backup job spec
	_, err = c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Patch(
		context.TODO(),
		c.getLogicalBackupJobName(),
		types.MergePatchType,
		patchData,
		metav1.PatchOptions{},
		"")
	if err != nil {
		return fmt.Errorf("could not patch logical backup job: %v", err)
	}

	return nil
}

func (c *Cluster) deleteLogicalBackupJob() error {

	c.logger.Info("removing the logical backup job")

	err := c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Delete(context.TODO(), c.getLogicalBackupJobName(), c.deleteOptions)
	if k8sutil.ResourceNotFound(err) {
		c.logger.Debugf("LogicalBackup CronJob was already deleted")
	} else if err != nil {
		return err
	}

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

// GetEndpointReplica returns cluster's kubernetes master Endpoint
func (c *Cluster) GetEndpointReplica() *v1.Endpoints {
	return c.Endpoints[Replica]
}

// GetStatefulSet returns cluster's kubernetes StatefulSet
func (c *Cluster) GetStatefulSet() *appsv1.StatefulSet {
	return c.Statefulset
}

// GetPodDisruptionBudget returns cluster's kubernetes PodDisruptionBudget
func (c *Cluster) GetPodDisruptionBudget() *policybeta1.PodDisruptionBudget {
	return c.PodDisruptionBudget
}

// Perform actual patching of a connection pooler deployment, assuming that all
// the check were already done before.
func (c *Cluster) updateConnectionPoolerDeployment(oldDeploymentSpec, newDeployment *appsv1.Deployment) (*appsv1.Deployment, error) {
	c.setProcessName("updating connection pooler")
	if c.ConnectionPooler == nil || c.ConnectionPooler.Deployment == nil {
		return nil, fmt.Errorf("there is no connection pooler in the cluster")
	}

	patchData, err := specPatch(newDeployment.Spec)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the deployment: %v", err)
	}

	// An update probably requires RetryOnConflict, but since only one operator
	// worker at one time will try to update it chances of conflicts are
	// minimal.
	deployment, err := c.KubeClient.
		Deployments(c.ConnectionPooler.Deployment.Namespace).Patch(
		context.TODO(),
		c.ConnectionPooler.Deployment.Name,
		types.MergePatchType,
		patchData,
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch deployment: %v", err)
	}

	c.ConnectionPooler.Deployment = deployment

	return deployment, nil
}

//updateConnectionPoolerAnnotations updates the annotations of connection pooler deployment
func (c *Cluster) updateConnectionPoolerAnnotations(annotations map[string]string) (*appsv1.Deployment, error) {
	c.logger.Debugf("updating connection pooler annotations")
	patchData, err := metaAnnotationsPatch(annotations)
	if err != nil {
		return nil, fmt.Errorf("could not form patch for the deployment metadata: %v", err)
	}
	result, err := c.KubeClient.Deployments(c.ConnectionPooler.Deployment.Namespace).Patch(
		context.TODO(),
		c.ConnectionPooler.Deployment.Name,
		types.MergePatchType,
		[]byte(patchData),
		metav1.PatchOptions{},
		"")
	if err != nil {
		return nil, fmt.Errorf("could not patch connection pooler annotations %q: %v", patchData, err)
	}
	return result, nil

}
