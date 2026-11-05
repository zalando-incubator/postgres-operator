package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/constants"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var requirePrimaryRestartWhenDecreased = []string{
	"max_connections",
	"max_prepared_transactions",
	"max_locks_per_transaction",
	"max_worker_processes",
	"max_wal_senders",
}

// Sync syncs the cluster, making sure the actual Kubernetes objects correspond to what is defined in the manifest.
// Unlike the update, sync does not error out if some objects do not exist and takes care of creating them.
func (c *Cluster) Sync(newSpec *acidv1.Postgresql) error {
	var err error
	c.mu.Lock()
	defer c.mu.Unlock()

	oldSpec := c.Postgresql

	if !c.isInMaintenanceWindow(newSpec.Spec.MaintenanceWindows) {
		// do not apply any major version related changes yet
		newSpec.Spec.PostgresqlParam.PgVersion = oldSpec.Spec.PostgresqlParam.PgVersion
	}

	c.setSpec(newSpec)

	defer func() {
		if err != nil {
			c.logger.Warningf("error while syncing cluster state: %v", err)
			newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusSyncFailed
		} else if !c.Status.Running() {
			newSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusRunning
		}

		if !equality.Semantic.DeepEqual(oldSpec.Status, newSpec.Status) {
			pgUpdatedStatus, err := c.KubeClient.SetPostgresCRDStatus(c.clusterName(), newSpec)
			if err != nil {
				c.logger.Warningf("could not set cluster status: %v", err)
				return
			}
			c.setSpec(pgUpdatedStatus)
		}
	}()

	if !c.patroniKubernetesUseConfigMaps() {
		c.logger.Warning("K8s endpoints are deprecated. Please, enable kubernetes_use_configmaps. Requires scale-in to a single primary, see v1 -> v2 migration docs!")
	}

	if err = c.syncFinalizer(); err != nil {
		c.logger.Debugf("could not sync finalizers: %v", err)
	}

	if err = c.initUsers(); err != nil {
		err = fmt.Errorf("could not init users: %v", err)
		return err
	}

	//TODO: mind the secrets of the deleted/new users
	if err = c.syncSecrets(); err != nil {
		err = fmt.Errorf("could not sync secrets: %v", err)
		return err
	}

	if err = c.syncServices(); err != nil {
		err = fmt.Errorf("could not sync services: %v", err)
		return err
	}

	if err = c.syncPatroniResources(); err != nil {
		c.logger.Errorf("could not sync Patroni resources: %v", err)
	}

	if err = c.syncVolumes(); err != nil {
		return err
	}

	if err = c.syncPodServiceAccount(); err != nil {
		c.logger.Errorf("could not sync pod service account: %v", err)
	}

	if err = c.syncStatefulSet(); err != nil {
		if !k8sutil.ResourceAlreadyExists(err) {
			err = fmt.Errorf("could not sync statefulsets: %v", err)
			return err
		}
	}

	// add or remove standby_cluster section from Patroni config depending on changes in standby section
	if !reflect.DeepEqual(oldSpec.Spec.StandbyCluster, newSpec.Spec.StandbyCluster) {
		if err := c.syncStandbyClusterConfiguration(); err != nil {
			return fmt.Errorf("could not sync StandbyCluster configuration: %v", err)
		}
	}

	c.logger.Debug("syncing pod disruption budgets")
	if err = c.syncPodDisruptionBudgets(false); err != nil {
		err = fmt.Errorf("could not sync pod disruption budgets: %v", err)
		return err
	}

	// create a logical backup job unless we are running without pods or disable that feature explicitly
	if c.Spec.EnableLogicalBackup && c.getNumberOfInstances(&c.Spec) > 0 {

		c.logger.Debug("syncing logical backup job")
		if err = c.syncLogicalBackupJob(); err != nil {
			err = fmt.Errorf("could not sync the logical backup job: %v", err)
			return err
		}
	}

	// create database objects unless we are running without pods or disabled that feature explicitly
	if !(c.databaseAccessDisabled() || c.getNumberOfInstances(&newSpec.Spec) <= 0 || c.Spec.StandbyCluster != nil) {
		c.logger.Debug("syncing roles")
		if err = c.syncRoles(); err != nil {
			c.logger.Errorf("could not sync roles: %v", err)
		}
		c.logger.Debug("syncing databases")
		if err = c.syncDatabases(); err != nil {
			c.logger.Errorf("could not sync databases: %v", err)
		}
		c.logger.Debug("syncing prepared databases with schemas")
		if err = c.syncPreparedDatabases(); err != nil {
			c.logger.Errorf("could not sync prepared database: %v", err)
		}
	}

	// sync connection pooler
	if _, err = c.syncConnectionPooler(&oldSpec, newSpec, c.installLookupFunction); err != nil {
		return fmt.Errorf("could not sync connection pooler: %v", err)
	}

	// sync if manifest stream count is different from stream CR count
	// it can be that they are always different due to grouping of manifest streams
	// but we would catch missed removals on update
	if len(c.Spec.Streams) != len(c.Streams) {
		c.logger.Debug("syncing streams")
		if err = c.syncStreams(); err != nil {
			err = fmt.Errorf("could not sync streams: %v", err)
			return err
		}
	}

	// Major version upgrade must only run after success of all earlier operations, must remain last item in sync
	if err := c.majorVersionUpgrade(); err != nil {
		c.logger.Errorf("major version upgrade failed: %v", err)
	}

	return err
}

func (c *Cluster) syncFinalizer() error {
	var err error
	if c.OpConfig.EnableFinalizers != nil && *c.OpConfig.EnableFinalizers {
		err = c.addFinalizer()
	} else {
		err = c.removeFinalizer()
	}
	if err != nil {
		return fmt.Errorf("could not sync finalizer: %v", err)
	}

	return nil
}

func (c *Cluster) syncPatroniResources() error {
	errors := make([]string, 0)

	if err := c.syncPatroniService(); err != nil {
		errors = append(errors, fmt.Sprintf("could not sync %s service: %v", Patroni, err))
	}

	for _, suffix := range patroniObjectSuffixes {
		if c.patroniKubernetesUseConfigMaps() {
			if err := c.syncPatroniConfigMap(suffix); err != nil {
				errors = append(errors, fmt.Sprintf("could not sync %s Patroni config map: %v", suffix, err))
			}
		} else {
			if err := c.syncPatroniEndpoint(suffix); err != nil {
				errors = append(errors, fmt.Sprintf("could not sync %s Patroni endpoint: %v", suffix, err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) syncPatroniConfigMap(suffix string) error {
	var (
		cm  *v1.ConfigMap
		err error
	)
	configMapName := fmt.Sprintf("%s-%s", c.Name, suffix)
	c.logger.Debugf("syncing %s config map", configMapName)
	c.setProcessName("syncing %s config map", configMapName)

	if cm, err = c.KubeClient.ConfigMaps(c.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{}); err == nil {
		c.PatroniConfigMaps[suffix] = cm
		desiredOwnerRefs := c.ownerReferences()
		if !reflect.DeepEqual(cm.ObjectMeta.OwnerReferences, desiredOwnerRefs) {
			c.logger.Infof("new %s config map's owner references do not match the current ones", configMapName)
			cm.ObjectMeta.OwnerReferences = desiredOwnerRefs
			c.setProcessName("updating %s config map", configMapName)
			cm, err = c.KubeClient.ConfigMaps(c.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("could not update %s config map: %v", configMapName, err)
			}
			c.PatroniConfigMaps[suffix] = cm
		}
		annotations := make(map[string]string)
		maps.Copy(annotations, cm.Annotations)
		// Patroni can add extra annotations so incl. current annotations in desired annotations
		desiredAnnotations := c.annotationsSet(cm.Annotations)
		if changed, _ := c.compareAnnotations(annotations, desiredAnnotations, nil); changed {
			patchData, err := metaAnnotationsPatch(desiredAnnotations)
			if err != nil {
				return fmt.Errorf("could not form patch for %s config map: %v", configMapName, err)
			}
			cm, err = c.KubeClient.ConfigMaps(c.Namespace).Patch(context.TODO(), configMapName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("could not patch annotations of %s config map: %v", configMapName, err)
			}
			c.PatroniConfigMaps[suffix] = cm
		}
	} else if !k8sutil.ResourceNotFound(err) {
		// if config map does not exist yet, Patroni should create it
		return fmt.Errorf("could not get %s config map: %v", configMapName, err)
	}

	return nil
}

func (c *Cluster) syncPatroniEndpoint(suffix string) error {
	var (
		ep  *v1.Endpoints
		err error
	)
	endpointName := fmt.Sprintf("%s-%s", c.Name, suffix)
	c.logger.Debugf("syncing %s endpoint", endpointName)
	c.setProcessName("syncing %s endpoint", endpointName)

	if ep, err = c.KubeClient.Endpoints(c.Namespace).Get(context.TODO(), endpointName, metav1.GetOptions{}); err == nil {
		c.PatroniEndpoints[suffix] = ep
		desiredOwnerRefs := c.ownerReferences()
		if !reflect.DeepEqual(ep.ObjectMeta.OwnerReferences, desiredOwnerRefs) {
			c.logger.Infof("new %s endpoints's owner references do not match the current ones", endpointName)
			ep.ObjectMeta.OwnerReferences = desiredOwnerRefs
			c.setProcessName("updating %s endpoint", endpointName)
			ep, err = c.KubeClient.Endpoints(c.Namespace).Update(context.TODO(), ep, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("could not update %s endpoint: %v", endpointName, err)
			}
			c.PatroniEndpoints[suffix] = ep
		}
		annotations := make(map[string]string)
		maps.Copy(annotations, ep.Annotations)
		// Patroni can add extra annotations so incl. current annotations in desired annotations
		desiredAnnotations := c.annotationsSet(ep.Annotations)
		if changed, _ := c.compareAnnotations(annotations, desiredAnnotations, nil); changed {
			patchData, err := metaAnnotationsPatch(desiredAnnotations)
			if err != nil {
				return fmt.Errorf("could not form patch for %s endpoint: %v", endpointName, err)
			}
			ep, err = c.KubeClient.Endpoints(c.Namespace).Patch(context.TODO(), endpointName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("could not patch annotations of %s endpoint: %v", endpointName, err)
			}
			c.PatroniEndpoints[suffix] = ep
		}
	} else if !k8sutil.ResourceNotFound(err) {
		// if endpoint does not exist yet, Patroni should create it
		return fmt.Errorf("could not get %s endpoint: %v", endpointName, err)
	}

	return nil
}

func (c *Cluster) syncPatroniService() error {
	var (
		svc *v1.Service
		err error
	)
	serviceName := fmt.Sprintf("%s-%s", c.Name, Patroni)
	c.logger.Debugf("syncing %s service", serviceName)
	c.setProcessName("syncing %s service", serviceName)

	if svc, err = c.KubeClient.Services(c.Namespace).Get(context.TODO(), serviceName, metav1.GetOptions{}); err == nil {
		c.Services[Patroni] = svc
		desiredOwnerRefs := c.ownerReferences()
		if !reflect.DeepEqual(svc.ObjectMeta.OwnerReferences, desiredOwnerRefs) {
			c.logger.Infof("new %s service's owner references do not match the current ones", serviceName)
			svc.ObjectMeta.OwnerReferences = desiredOwnerRefs
			c.setProcessName("updating %v service", serviceName)
			svc, err = c.KubeClient.Services(c.Namespace).Update(context.TODO(), svc, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("could not update %s service: %v", serviceName, err)
			}
			c.Services[Patroni] = svc
		}
		annotations := make(map[string]string)
		maps.Copy(annotations, svc.Annotations)
		// Patroni can add extra annotations so incl. current annotations in desired annotations
		desiredAnnotations := c.annotationsSet(svc.Annotations)
		if changed, _ := c.compareAnnotations(annotations, desiredAnnotations, nil); changed {
			patchData, err := metaAnnotationsPatch(desiredAnnotations)
			if err != nil {
				return fmt.Errorf("could not form patch for %s service: %v", serviceName, err)
			}
			svc, err = c.KubeClient.Services(c.Namespace).Patch(context.TODO(), serviceName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("could not patch annotations of %s service: %v", serviceName, err)
			}
			c.Services[Patroni] = svc
		}
	} else if !k8sutil.ResourceNotFound(err) {
		// if config service does not exist yet, Patroni should create it
		return fmt.Errorf("could not get %s service: %v", serviceName, err)
	}

	return nil
}

func (c *Cluster) syncServices() error {
	for _, role := range []PostgresRole{Master, Replica} {
		c.logger.Debugf("syncing %s service", role)

		if !c.patroniKubernetesUseConfigMaps() {
			if err := c.syncEndpoint(role); err != nil {
				return fmt.Errorf("could not sync %s endpoint: %v", role, err)
			}
		}
		if err := c.syncService(role); err != nil {
			return fmt.Errorf("could not sync %s service: %v", role, err)
		}
	}

	return nil
}

func (c *Cluster) syncService(role PostgresRole) error {
	var (
		svc *v1.Service
		err error
	)
	c.setProcessName("syncing %s service", role)

	if svc, err = c.KubeClient.Services(c.Namespace).Get(context.TODO(), c.serviceName(role), metav1.GetOptions{}); err == nil {
		c.Services[role] = svc
		desiredSvc := c.generateService(role, &c.Spec)
		updatedSvc, err := c.updateService(role, svc, desiredSvc)
		if err != nil {
			return fmt.Errorf("could not update %s service to match desired state: %v", role, err)
		}
		c.Services[role] = updatedSvc
		return nil
	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get %s service: %v", role, err)
	}
	// no existing service, create new one
	c.logger.Infof("could not find the cluster's %s service", role)

	if svc, err = c.createService(role); err == nil {
		c.logger.Infof("created missing %s service %q", role, util.NameFromMeta(svc.ObjectMeta))
	} else {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create missing %s service: %v", role, err)
		}
		c.logger.Infof("%s service %q already exists", role, util.NameFromMeta(svc.ObjectMeta))
		if svc, err = c.KubeClient.Services(c.Namespace).Get(context.TODO(), c.serviceName(role), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %s service: %v", role, err)
		}
	}
	c.Services[role] = svc
	return nil
}

func (c *Cluster) syncEndpoint(role PostgresRole) error {
	var (
		ep  *v1.Endpoints
		err error
	)
	c.setProcessName("syncing %s endpoint", role)

	if ep, err = c.KubeClient.Endpoints(c.Namespace).Get(context.TODO(), c.serviceName(role), metav1.GetOptions{}); err == nil {
		desiredEp := c.generateEndpoint(role, ep.Subsets)
		// if owner references differ we update which would also change annotations
		if !reflect.DeepEqual(ep.ObjectMeta.OwnerReferences, desiredEp.ObjectMeta.OwnerReferences) {
			c.logger.Infof("new %s endpoints's owner references do not match the current ones", role)
			c.setProcessName("updating %v endpoint", role)
			ep, err = c.KubeClient.Endpoints(c.Namespace).Update(context.TODO(), desiredEp, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("could not update %s endpoint: %v", role, err)
			}
		} else {
			if changed, _ := c.compareAnnotations(ep.Annotations, desiredEp.Annotations, nil); changed {
				patchData, err := metaAnnotationsPatch(desiredEp.Annotations)
				if err != nil {
					return fmt.Errorf("could not form patch for %s endpoint: %v", role, err)
				}
				ep, err = c.KubeClient.Endpoints(c.Namespace).Patch(context.TODO(), c.serviceName(role), types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
				if err != nil {
					return fmt.Errorf("could not patch annotations of %s endpoint: %v", role, err)
				}
			}
		}
		c.Endpoints[role] = ep
		return nil
	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get %s endpoint: %v", role, err)
	}
	// no existing endpoint, create new one
	c.logger.Infof("could not find the cluster's %s endpoint", role)

	if ep, err = c.createEndpoint(role); err == nil {
		c.logger.Infof("created missing %s endpoint %q", role, util.NameFromMeta(ep.ObjectMeta))
	} else {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create missing %s endpoint: %v", role, err)
		}
		c.logger.Infof("%s endpoint %q already exists", role, util.NameFromMeta(ep.ObjectMeta))
		if ep, err = c.KubeClient.Endpoints(c.Namespace).Get(context.TODO(), c.serviceName(role), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %s endpoint: %v", role, err)
		}
	}
	c.Endpoints[role] = ep
	return nil
}

func (c *Cluster) syncPrimaryPodDisruptionBudget(isUpdate bool) error {
	var (
		pdb *policyv1.PodDisruptionBudget
		err error
	)
	if pdb, err = c.KubeClient.PodDisruptionBudgets(c.Namespace).Get(context.TODO(), c.PrimaryPodDisruptionBudgetName(), metav1.GetOptions{}); err == nil {
		c.PrimaryPodDisruptionBudget = pdb
		newPDB := c.generatePrimaryPodDisruptionBudget()
		match, reason := c.comparePodDisruptionBudget(pdb, newPDB)
		if !match {
			c.logPDBChanges(pdb, newPDB, isUpdate, reason)
			if err = c.updatePrimaryPodDisruptionBudget(newPDB); err != nil {
				return err
			}
		} else {
			c.PrimaryPodDisruptionBudget = pdb
		}
		return nil

	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get pod disruption budget: %v", err)
	}
	// no existing pod disruption budget, create new one
	c.logger.Infof("could not find the primary pod disruption budget")

	if err = c.createPrimaryPodDisruptionBudget(); err != nil {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create primary pod disruption budget: %v", err)
		}
		c.logger.Infof("pod disruption budget %q already exists", util.NameFromMeta(pdb.ObjectMeta))
		if pdb, err = c.KubeClient.PodDisruptionBudgets(c.Namespace).Get(context.TODO(), c.PrimaryPodDisruptionBudgetName(), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %q pod disruption budget", util.NameFromMeta(pdb.ObjectMeta))
		}
	}

	return nil
}

func (c *Cluster) syncCriticalOpPodDisruptionBudget(isUpdate bool) error {
	var (
		pdb *policyv1.PodDisruptionBudget
		err error
	)
	if pdb, err = c.KubeClient.PodDisruptionBudgets(c.Namespace).Get(context.TODO(), c.criticalOpPodDisruptionBudgetName(), metav1.GetOptions{}); err == nil {
		c.CriticalOpPodDisruptionBudget = pdb
		newPDB := c.generateCriticalOpPodDisruptionBudget()
		match, reason := c.comparePodDisruptionBudget(pdb, newPDB)
		if !match {
			c.logPDBChanges(pdb, newPDB, isUpdate, reason)
			if err = c.updateCriticalOpPodDisruptionBudget(newPDB); err != nil {
				return err
			}
		} else {
			c.CriticalOpPodDisruptionBudget = pdb
		}
		return nil

	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get pod disruption budget: %v", err)
	}
	// no existing pod disruption budget, create new one
	c.logger.Infof("could not find pod disruption budget for critical operations")

	if err = c.createCriticalOpPodDisruptionBudget(); err != nil {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create pod disruption budget for critical operations: %v", err)
		}
		c.logger.Infof("pod disruption budget %q already exists", util.NameFromMeta(pdb.ObjectMeta))
		if pdb, err = c.KubeClient.PodDisruptionBudgets(c.Namespace).Get(context.TODO(), c.criticalOpPodDisruptionBudgetName(), metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing %q pod disruption budget", util.NameFromMeta(pdb.ObjectMeta))
		}
	}

	return nil
}

func (c *Cluster) syncPodDisruptionBudgets(isUpdate bool) error {
	errors := make([]string, 0)

	if err := c.syncPrimaryPodDisruptionBudget(isUpdate); err != nil {
		errors = append(errors, fmt.Sprintf("%v", err))
	}

	if err := c.syncCriticalOpPodDisruptionBudget(isUpdate); err != nil {
		errors = append(errors, fmt.Sprintf("%v", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}
	return nil
}

func (c *Cluster) syncStatefulSet() error {
	var (
		restartWait         uint32
		configPatched       bool
		restartPrimaryFirst bool
	)
	podsToRecreate := make([]v1.Pod, 0)
	isSafeToRecreatePods := true
	postponeReasons := make([]string, 0)
	switchoverCandidates := make([]spec.NamespacedName, 0)

	pods, err := c.listPods()
	if err != nil {
		c.logger.Warnf("could not list pods of the statefulset: %v", err)
	}

	// NB: Be careful to consider the codepath that acts on podsRollingUpdateRequired before returning early.
	sset, err := c.KubeClient.StatefulSets(c.Namespace).Get(context.TODO(), c.statefulSetName(), metav1.GetOptions{})
	if err != nil && !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("error during reading of statefulset: %v", err)
	}

	if err != nil {
		// statefulset does not exist, try to re-create it
		c.logger.Infof("cluster's statefulset does not exist")

		sset, err = c.createStatefulSet()
		if err != nil {
			return fmt.Errorf("could not create missing statefulset: %v", err)
		}

		if err = c.waitStatefulsetPodsReady(); err != nil {
			return fmt.Errorf("cluster is not ready: %v", err)
		}

		if len(pods) > 0 {
			for _, pod := range pods {
				if err = c.markRollingUpdateForPod(&pod, "pod from previous statefulset"); err != nil {
					c.logger.Warnf("marking old pod for rolling update failed: %v", err)
				}
				podsToRecreate = append(podsToRecreate, pod)
			}
		}
		c.logger.Infof("created missing statefulset %q", util.NameFromMeta(sset.ObjectMeta))

	} else {
		desiredSts, err := c.generateStatefulSet(&c.Spec)
		if err != nil {
			return fmt.Errorf("could not generate statefulset: %v", err)
		}
		c.logger.Debug("syncing statefulsets")
		// check if there are still pods with a rolling update annotation
		for _, pod := range pods {
			if rollingUpdateRequired := c.getRollingUpdateAnnotationFromPod(&pod); rollingUpdateRequired != "" {
				c.logger.Debugf("found rolling update annotation %q on pod %q", rollingUpdateRequired, pod.Name)
				podsToRecreate = append(podsToRecreate, pod)
			} else {
				role := PostgresRole(pod.Labels[c.OpConfig.PodRoleLabel])
				if role == Master {
					continue
				}
				switchoverCandidates = append(switchoverCandidates, util.NameFromMeta(pod.ObjectMeta))
			}
		}

		if len(podsToRecreate) > 0 {
			c.logger.Infof("%d / %d pod(s) still need to be rotated", len(podsToRecreate), len(pods))
		}

		// statefulset is already there, make sure we use its definition in order to compare with the spec.
		c.Statefulset = sset

		cmp := c.compareStatefulSetWith(desiredSts)
		if !cmp.rollingUpdate {
			updatedPodAnnotations := map[string]*string{}
			for _, anno := range cmp.deletedPodAnnotations {
				// during IRSA migration let kube2iam annotation drain naturally via pod rotation
				if c.OpConfig.IRSARoleARN != "" && anno == constants.KubeIAmAnnotation {
					continue
				}
				updatedPodAnnotations[anno] = nil
			}
			for anno, val := range desiredSts.Spec.Template.Annotations {
				updatedPodAnnotations[anno] = &val
			}
			metadataReq := map[string]map[string]map[string]*string{"metadata": {"annotations": updatedPodAnnotations}}
			patch, err := json.Marshal(metadataReq)
			if err != nil {
				return fmt.Errorf("could not form patch for pod annotations: %v", err)
			}

			for _, pod := range pods {
				if changed, _ := c.compareAnnotations(pod.Annotations, desiredSts.Spec.Template.Annotations, nil); changed {
					_, err = c.KubeClient.Pods(c.Namespace).Patch(context.TODO(), pod.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
					if err != nil {
						return fmt.Errorf("could not patch annotations for pod %q: %v", pod.Name, err)
					}
				}
			}
		}

		if !cmp.match {
			isImageDiffOnly := len(cmp.reasons) == 1 && strings.Contains(cmp.reasons[0], "image does not match the current one")
			lazyUpdate := isImageDiffOnly && c.OpConfig.EnableLazySpiloUpgrade
			shouldDeferUpdate := isImageDiffOnly && !c.isInMaintenanceWindow(c.Spec.MaintenanceWindows)

			if lazyUpdate {
				c.logger.Debug("lazy Spilo update: new statefulset's pod image does not match the current one")
				cmp.replace = true
			}

			if cmp.rollingUpdate && !lazyUpdate {
				// mark pods for rolling update if we are in a maintenance window
				if !shouldDeferUpdate {
					podsToRecreate = make([]v1.Pod, 0)
					switchoverCandidates = make([]spec.NamespacedName, 0)
					for _, pod := range pods {
						if err = c.markRollingUpdateForPod(&pod, "pod changes"); err != nil {
							return fmt.Errorf("updating rolling update annotation for pod %q failed: %v", pod.Name, err)
						}
						podsToRecreate = append(podsToRecreate, pod)
					}
				} else {
					c.logger.Debug("deferring rolling update of pods until next maintenance window")
				}
			}

			if !shouldDeferUpdate || lazyUpdate {
				c.logStatefulSetChanges(c.Statefulset, desiredSts, false, cmp.reasons)

				if !cmp.replace {
					if err := c.updateStatefulSet(desiredSts); err != nil {
						return fmt.Errorf("could not update statefulset: %v", err)
					}
				} else {
					if err := c.replaceStatefulSet(desiredSts); err != nil {
						return fmt.Errorf("could not replace statefulset: %v", err)
					}
				}
			} else {
				c.logger.Debug("deferring statefulset update until next maintenance window")
			}
		}

		// even if the desired and the running statefulsets match
		// there still may be not up-to-date pods on condition
		//  (a) the lazy update was just disabled
		// and
		//  (b) some of the pods were not restarted when the lazy update was still in place
		// catching up on those pods is a rolling update, hence it waits for the maintenance window
		if len(podsToRecreate) == 0 && !c.OpConfig.EnableLazySpiloUpgrade && c.isInMaintenanceWindow(c.Spec.MaintenanceWindows) {
			for _, pod := range pods {
				effectivePodImage := getPostgresContainer(&pod.Spec).Image
				stsImage := getPostgresContainer(&desiredSts.Spec.Template.Spec).Image

				if stsImage != effectivePodImage {
					if err = c.markRollingUpdateForPod(&pod, "pod not yet restarted due to lazy update"); err != nil {
						c.logger.Warnf("updating rolling update flag failed for pod %q: %v", pod.Name, err)
					}
					podsToRecreate = append(podsToRecreate, pod)
				} else {
					role := PostgresRole(pod.Labels[c.OpConfig.PodRoleLabel])
					if role == Master {
						continue
					}
					switchoverCandidates = append(switchoverCandidates, util.NameFromMeta(pod.ObjectMeta))
				}
			}
		}
	}

	// apply PostgreSQL parameters that can only be set via the Patroni API.
	// it is important to do it after the statefulset pods are there, but before the rolling update
	// since those parameters require PostgreSQL restart.
	pods, err = c.listPods()
	if err != nil {
		c.logger.Warnf("could not get list of pods to apply PostgreSQL parameters only to be set via Patroni API: %v", err)
	}

	requiredPgParameters := make(map[string]string)
	for k, v := range c.Spec.Parameters {
		requiredPgParameters[k] = v
	}
	// if streams are defined wal_level must be switched to logical
	if len(c.Spec.Streams) > 0 {
		requiredPgParameters["wal_level"] = "logical"
	}

	// sync Patroni config
	c.logger.Debug("syncing Patroni config")
	if configPatched, restartPrimaryFirst, restartWait, err = c.syncPatroniConfig(pods, c.Spec.Patroni, requiredPgParameters); err != nil {
		c.logger.Warningf("Patroni config updated? %v - errors during config sync: %v", configPatched, err)
		postponeReasons = append(postponeReasons, "errors during Patroni config sync")
		// Only mark unsafe if all pods are running. If some pods are not running,
		// Patroni API errors are expected and should not block pod recreation,
		// which is the only way to fix non-running pods.
		if c.allPodsRunning(pods) {
			isSafeToRecreatePods = false
		} else {
			c.logger.Warningf("ignoring Patroni config sync errors because some pods are not running")
		}
	}

	// restart Postgres where it is still pending
	if err = c.restartInstances(pods, restartWait, restartPrimaryFirst); err != nil {
		c.logger.Errorf("errors while restarting Postgres in pods via Patroni API: %v", err)
		postponeReasons = append(postponeReasons, "errors while restarting Postgres via Patroni API")
		// Same logic: don't let unreachable non-running pods block recreation.
		if c.allPodsRunning(pods) {
			isSafeToRecreatePods = false
		} else {
			c.logger.Warningf("ignoring Patroni restart errors because some pods are not running")
		}
	}

	// if we get here, we need to re-create the pods from the outdated statefulset
	// or leftovers from an old statefulset
	if len(podsToRecreate) > 0 {
		if isSafeToRecreatePods && len(podsToRecreate) > 0 {
			c.logger.Info("performing rolling update")
			c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Update", "Performing rolling update")
			if err := c.recreatePods(podsToRecreate, switchoverCandidates); err != nil {
				return fmt.Errorf("could not recreate pods: %v", err)
			}
			c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Update", "Rolling update done - pods have been recreated")
		} else {
			c.logger.Warningf("postpone pod recreation until next sync - reason: %s", strings.Join(postponeReasons, `', '`))
		}
	}

	return nil
}

func (c *Cluster) syncPatroniConfig(pods []v1.Pod, requiredPatroniConfig acidv1.Patroni, requiredPgParameters map[string]string) (bool, bool, uint32, error) {
	var (
		effectivePatroniConfig acidv1.Patroni
		effectivePgParameters  map[string]string
		loopWait               uint32
		configPatched          bool
		restartPrimaryFirst    bool
		err                    error
	)

	errors := make([]string, 0)

	// get Postgres config, compare with manifest and update via Patroni PATCH endpoint if it differs
	for i, pod := range pods {
		podName := util.NameFromMeta(pods[i].ObjectMeta)
		effectivePatroniConfig, effectivePgParameters, err = c.getPatroniConfig(&pod)
		if err != nil {
			errors = append(errors, fmt.Sprintf("could not get Postgres config from pod %s: %v", podName, err))
			continue
		}
		loopWait = effectivePatroniConfig.LoopWait

		// empty config probably means cluster is not fully initialized yet, e.g. restoring from backup
		if reflect.DeepEqual(effectivePatroniConfig, acidv1.Patroni{}) || len(effectivePgParameters) == 0 {
			errors = append(errors, fmt.Sprintf("empty Patroni config on pod %s - skipping config patch", podName))
		} else {
			configPatched, restartPrimaryFirst, err = c.checkAndSetGlobalPostgreSQLConfiguration(&pod, effectivePatroniConfig, requiredPatroniConfig, effectivePgParameters, requiredPgParameters)
			if err != nil {
				errors = append(errors, fmt.Sprintf("could not set PostgreSQL configuration options for pod %s: %v", podName, err))
				continue
			}

			// it could take up to LoopWait to apply the config
			if configPatched {
				time.Sleep(time.Duration(loopWait)*time.Second + time.Second*2)
				// Patroni's config endpoint is just a "proxy" to DCS.
				// It is enough to patch it only once and it doesn't matter which pod is used
				break
			}
		}
	}

	if len(errors) > 0 {
		err = fmt.Errorf("%v", strings.Join(errors, `', '`))
	}

	return configPatched, restartPrimaryFirst, loopWait, err
}

func (c *Cluster) restartInstances(pods []v1.Pod, restartWait uint32, restartPrimaryFirst bool) (err error) {
	errors := make([]string, 0)
	remainingPods := make([]*v1.Pod, 0)

	skipRole := Master
	if restartPrimaryFirst {
		skipRole = Replica
	}

	for i, pod := range pods {
		role := PostgresRole(pod.Labels[c.OpConfig.PodRoleLabel])
		if role == skipRole {
			remainingPods = append(remainingPods, &pods[i])
			continue
		}
		if err = c.restartInstance(&pod, restartWait); err != nil {
			errors = append(errors, fmt.Sprintf("%v", err))
		}
	}

	// in most cases only the master should be left to restart
	if len(remainingPods) > 0 {
		for _, remainingPod := range remainingPods {
			if err = c.restartInstance(remainingPod, restartWait); err != nil {
				errors = append(errors, fmt.Sprintf("%v", err))
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) restartInstance(pod *v1.Pod, restartWait uint32) error {
	// if the config update requires a restart, call Patroni restart
	podName := util.NameFromMeta(pod.ObjectMeta)
	role := PostgresRole(pod.Labels[c.OpConfig.PodRoleLabel])
	memberData, err := c.getPatroniMemberData(pod)
	if err != nil {
		return fmt.Errorf("could not restart Postgres in %s pod %s: %v", role, podName, err)
	}

	// do restart only when it is pending
	if memberData.PendingRestart {
		c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Update", fmt.Sprintf("restarting Postgres server within %s pod %s", role, podName))
		if err := c.patroni.Restart(pod); err != nil {
			return err
		}
		time.Sleep(time.Duration(restartWait) * time.Second)
		c.eventRecorder.Event(c.GetReference(), v1.EventTypeNormal, "Update", fmt.Sprintf("Postgres server restart done for %s pod %s", role, podName))
	}

	return nil
}

// AnnotationsToPropagate get the annotations to update if required
// based on the annotations in postgres CRD
func (c *Cluster) AnnotationsToPropagate(annotations map[string]string) map[string]string {

	if annotations == nil {
		annotations = make(map[string]string)
	}

	pgCRDAnnotations := c.ObjectMeta.Annotations

	if pgCRDAnnotations != nil {
		for _, anno := range c.OpConfig.DownscalerAnnotations {
			for k, v := range pgCRDAnnotations {
				matched, err := regexp.MatchString(anno, k)
				if err != nil {
					c.logger.Errorf("annotations matching issue: %v", err)
					return nil
				}
				if matched {
					annotations[k] = v
				}
			}
		}
	}

	if len(annotations) > 0 {
		return annotations
	}

	return nil
}

// checkAndSetGlobalPostgreSQLConfiguration checks whether cluster-wide API parameters
// (like max_connections) have changed and if necessary sets it via the Patroni API
func (c *Cluster) checkAndSetGlobalPostgreSQLConfiguration(pod *v1.Pod, effectivePatroniConfig, desiredPatroniConfig acidv1.Patroni, effectivePgParameters, desiredPgParameters map[string]string) (bool, bool, error) {
	configToSet := make(map[string]interface{})
	parametersToSet := make(map[string]string)
	restartPrimary := make([]bool, 0)
	configPatched := false
	requiresMasterRestart := false

	// compare effective and desired Patroni config options
	if desiredPatroniConfig.LoopWait > 0 && desiredPatroniConfig.LoopWait != effectivePatroniConfig.LoopWait {
		configToSet["loop_wait"] = desiredPatroniConfig.LoopWait
	}
	if desiredPatroniConfig.MaximumLagOnFailover > 0 && desiredPatroniConfig.MaximumLagOnFailover != effectivePatroniConfig.MaximumLagOnFailover {
		configToSet["maximum_lag_on_failover"] = desiredPatroniConfig.MaximumLagOnFailover
	}
	if desiredPatroniConfig.PgHba != nil && !reflect.DeepEqual(desiredPatroniConfig.PgHba, effectivePatroniConfig.PgHba) {
		configToSet["pg_hba"] = desiredPatroniConfig.PgHba
	}
	if desiredPatroniConfig.RetryTimeout > 0 && desiredPatroniConfig.RetryTimeout != effectivePatroniConfig.RetryTimeout {
		configToSet["retry_timeout"] = desiredPatroniConfig.RetryTimeout
	}
	if desiredPatroniConfig.SynchronousMode != effectivePatroniConfig.SynchronousMode {
		configToSet["synchronous_mode"] = desiredPatroniConfig.SynchronousMode
	}
	if desiredPatroniConfig.SynchronousModeStrict != effectivePatroniConfig.SynchronousModeStrict {
		configToSet["synchronous_mode_strict"] = desiredPatroniConfig.SynchronousModeStrict
	}
	if desiredPatroniConfig.SynchronousNodeCount != effectivePatroniConfig.SynchronousNodeCount {
		configToSet["synchronous_node_count"] = desiredPatroniConfig.SynchronousNodeCount
	}
	if desiredPatroniConfig.TTL > 0 && desiredPatroniConfig.TTL != effectivePatroniConfig.TTL {
		configToSet["ttl"] = desiredPatroniConfig.TTL
	}

	var desiredFailsafe *bool
	if desiredPatroniConfig.FailsafeMode != nil {
		desiredFailsafe = desiredPatroniConfig.FailsafeMode
	} else if c.OpConfig.EnablePatroniFailsafeMode != nil {
		desiredFailsafe = c.OpConfig.EnablePatroniFailsafeMode
	}

	effectiveFailsafe := effectivePatroniConfig.FailsafeMode

	if desiredFailsafe != nil {
		if effectiveFailsafe == nil || *desiredFailsafe != *effectiveFailsafe {
			configToSet["failsafe_mode"] = *desiredFailsafe
		}
	}

	slotsToSet := make(map[string]interface{})
	// check if there is any slot deletion
	for slotName, effectiveSlot := range c.replicationSlots {
		if desiredSlot, exists := desiredPatroniConfig.Slots[slotName]; exists {
			if reflect.DeepEqual(effectiveSlot, desiredSlot) {
				continue
			}
		}
		slotsToSet[slotName] = nil
		delete(c.replicationSlots, slotName)
	}
	// check if specified slots exist in config and if they differ
	for slotName, desiredSlot := range desiredPatroniConfig.Slots {
		// only add slots specified in manifest to c.replicationSlots
		for manifestSlotName := range c.Spec.Patroni.Slots {
			if manifestSlotName == slotName {
				c.replicationSlots[slotName] = desiredSlot
			}
		}
		if effectiveSlot, exists := effectivePatroniConfig.Slots[slotName]; exists {
			if reflect.DeepEqual(desiredSlot, effectiveSlot) {
				continue
			}
		}
		slotsToSet[slotName] = desiredSlot
	}
	if len(slotsToSet) > 0 {
		configToSet["slots"] = slotsToSet
	}

	// compare effective and desired parameters under postgresql section in Patroni config
	for desiredOption, desiredValue := range desiredPgParameters {
		effectiveValue := effectivePgParameters[desiredOption]
		if isBootstrapOnlyParameter(desiredOption) && (effectiveValue != desiredValue) {
			parametersToSet[desiredOption] = desiredValue
			if slices.Contains(requirePrimaryRestartWhenDecreased, desiredOption) {
				effectiveValueNum, errConv := strconv.Atoi(effectiveValue)
				desiredValueNum, errConv2 := strconv.Atoi(desiredValue)
				if errConv != nil || errConv2 != nil {
					continue
				}
				if effectiveValueNum > desiredValueNum {
					restartPrimary = append(restartPrimary, true)
					continue
				}
			}
			restartPrimary = append(restartPrimary, false)
		}
	}

	// check if there exist only config updates that require a restart of the primary
	if len(restartPrimary) > 0 && !slices.Contains(restartPrimary, false) && len(configToSet) == 0 {
		requiresMasterRestart = true
	}

	if len(parametersToSet) > 0 {
		configToSet["postgresql"] = map[string]interface{}{constants.PatroniPGParametersParameterName: parametersToSet}
	}

	if len(configToSet) == 0 {
		return configPatched, requiresMasterRestart, nil
	}

	configToSetJson, err := json.Marshal(configToSet)
	if err != nil {
		c.logger.Debugf("could not convert config patch to JSON: %v", err)
	}

	// try all pods until the first one that is successful, as it doesn't matter which pod
	// carries the request to change configuration through
	podName := util.NameFromMeta(pod.ObjectMeta)
	c.logger.Debugf("patching Postgres config via Patroni API on pod %s with following options: %s",
		podName, configToSetJson)
	if err = c.patroni.SetConfig(pod, configToSet); err != nil {
		return configPatched, requiresMasterRestart, fmt.Errorf("could not patch postgres parameters within pod %s: %v", podName, err)
	}
	configPatched = true

	return configPatched, requiresMasterRestart, nil
}

// syncStandbyClusterConfiguration checks whether standby cluster
// parameters have changed and if necessary sets it via the Patroni API
func (c *Cluster) syncStandbyClusterConfiguration() error {
	var (
		err  error
		pods []v1.Pod
	)

	standbyOptionsToSet := make(map[string]interface{})
	if c.Spec.StandbyCluster != nil {
		c.logger.Infof("turning %q into a standby cluster", c.Name)
		standbyOptionsToSet["create_replica_methods"] = []string{"bootstrap_standby_with_wale", "basebackup_fast_xlog"}
		standbyOptionsToSet["restore_command"] = "envdir \"/run/etc/wal-e.d/env-standby\" /scripts/restore_command.sh \"%f\" \"%p\""

		if c.Spec.StandbyCluster.StandbyHost != "" {
			standbyOptionsToSet["host"] = c.Spec.StandbyCluster.StandbyHost
		} else {
			standbyOptionsToSet["host"] = nil
		}

		if c.Spec.StandbyCluster.StandbyPort != "" {
			standbyOptionsToSet["port"] = c.Spec.StandbyCluster.StandbyPort
		} else {
			standbyOptionsToSet["port"] = nil
		}

		if c.Spec.StandbyCluster.StandbyPrimarySlotName != "" {
			standbyOptionsToSet["primary_slot_name"] = c.Spec.StandbyCluster.StandbyPrimarySlotName
		} else {
			standbyOptionsToSet["primary_slot_name"] = nil
		}
	} else {
		c.logger.Infof("promoting standby cluster and detach from source")
		standbyOptionsToSet = nil
	}

	if pods, err = c.listPods(); err != nil {
		return err
	}
	if len(pods) == 0 {
		return fmt.Errorf("could not call Patroni API: cluster has no pods")
	}
	// try all pods until the first one that is successful, as it doesn't matter which pod
	// carries the request to change configuration through
	for _, pod := range pods {
		podName := util.NameFromMeta(pod.ObjectMeta)
		c.logger.Infof("patching Postgres config via Patroni API on pod %s with following options: %s",
			podName, standbyOptionsToSet)
		if err = c.patroni.SetStandbyClusterParameters(&pod, standbyOptionsToSet); err == nil {
			return nil
		}
		c.logger.Warningf("could not patch postgres parameters within pod %s: %v", podName, err)
	}
	return fmt.Errorf("could not reach Patroni API to set Postgres options: failed on every pod (%d total)",
		len(pods))
}

func (c *Cluster) syncSecrets() error {
	c.logger.Debug("syncing secrets")
	c.setProcessName("syncing secrets")
	errors := make([]string, 0)
	generatedSecrets := c.generateUserSecrets()
	retentionUsers := make([]string, 0)
	currentTime := time.Now()

	for secretUsername, generatedSecret := range generatedSecrets {
		pgUserDegraded := false
		createdSecret, err := c.KubeClient.Secrets(generatedSecret.Namespace).Create(context.TODO(), generatedSecret, metav1.CreateOptions{})
		if err == nil {
			c.Secrets[createdSecret.UID] = createdSecret
			c.logger.Infof("created new secret %s, namespace: %s, uid: %s", util.NameFromMeta(createdSecret.ObjectMeta), generatedSecret.Namespace, createdSecret.UID)
			continue
		}
		if k8sutil.ResourceAlreadyExists(err) {
			updatedSecret, err := c.updateSecret(secretUsername, generatedSecret, &retentionUsers, currentTime)
			if err == nil {
				c.Secrets[updatedSecret.UID] = updatedSecret
				continue
			}
			errors = append(errors, fmt.Sprintf("syncing secret %s failed: %v", util.NameFromMeta(generatedSecret.ObjectMeta), err))
			pgUserDegraded = true
		} else {
			errors = append(errors, fmt.Sprintf("could not create secret for user %s: in namespace %s: %v", secretUsername, generatedSecret.Namespace, err))
			pgUserDegraded = true
		}
		c.updatePgUser(secretUsername, pgUserDegraded)
	}

	// remove rotation users that exceed the retention interval
	if len(retentionUsers) > 0 {
		if err := c.cleanupRotatedUsers(retentionUsers); err != nil {
			errors = append(errors, fmt.Sprintf("error removing users exceeding configured retention interval: %v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) getNextRotationDate(currentDate time.Time) (time.Time, string) {
	nextRotationDate := currentDate.AddDate(0, 0, int(c.OpConfig.PasswordRotationInterval))
	return nextRotationDate, nextRotationDate.Format(time.RFC3339)
}

func (c *Cluster) updateSecret(
	secretUsername string,
	generatedSecret *v1.Secret,
	retentionUsers *[]string,
	currentTime time.Time) (*v1.Secret, error) {
	var (
		secret          *v1.Secret
		err             error
		updateSecret    bool
		updateSecretMsg string
	)

	// get the secret first
	if secret, err = c.KubeClient.Secrets(generatedSecret.Namespace).Get(context.TODO(), generatedSecret.Name, metav1.GetOptions{}); err != nil {
		return generatedSecret, fmt.Errorf("could not get current secret: %v", err)
	}
	c.Secrets[secret.UID] = secret

	// fetch user map to update later
	var userMap map[string]spec.PgUser
	var userKey string
	switch secretUsername {
	case c.systemUsers[constants.SuperuserKeyName].Name:
		userKey = constants.SuperuserKeyName
		userMap = c.systemUsers
	case c.systemUsers[constants.ReplicationUserKeyName].Name:
		userKey = constants.ReplicationUserKeyName
		userMap = c.systemUsers
	default:
		userKey = secretUsername
		userMap = c.pgUsers
	}

	// use system user when pooler is enabled and pooler user is specfied in manifest
	if _, exists := c.systemUsers[constants.ConnectionPoolerUserKeyName]; exists {
		if secretUsername == c.systemUsers[constants.ConnectionPoolerUserKeyName].Name {
			userKey = constants.ConnectionPoolerUserKeyName
			userMap = c.systemUsers
		}
	}
	// use system user when streams are defined and fes_user is specfied in manifest
	if _, exists := c.systemUsers[constants.EventStreamUserKeyName]; exists {
		if secretUsername == c.systemUsers[constants.EventStreamUserKeyName].Name {
			userKey = constants.EventStreamUserKeyName
			userMap = c.systemUsers
		}
	}

	pwdUser := userMap[userKey]
	secretName := util.NameFromMeta(secret.ObjectMeta)

	// do not perform any rotation of reset for standby clusters
	if !isStandbyCluster(&c.Spec) {
		updateSecretMsg, err = c.checkForPasswordRotation(secret, secretUsername, pwdUser, retentionUsers, currentTime)
		if err != nil {
			return nil, fmt.Errorf("error while checking for password rotation: %v", err)
		}
		if updateSecretMsg != "" {
			updateSecret = true
		}
	}

	// if this secret belongs to the infrastructure role and the password has changed - replace it in the secret
	if pwdUser.Password != string(secret.Data["password"]) && pwdUser.Origin == spec.RoleOriginInfrastructure {
		secret = generatedSecret
		updateSecret = true
		updateSecretMsg = fmt.Sprintf("updating the secret %s from the infrastructure roles", secretName)
	} else {
		// for non-infrastructure role - update the role with username and password from secret
		pwdUser.Name = string(secret.Data["username"])
		pwdUser.Password = string(secret.Data["password"])
		// update membership if we deal with a rotation user
		if secretUsername != pwdUser.Name {
			pwdUser.Rotated = true
			pwdUser.MemberOf = []string{secretUsername}
		}
		userMap[userKey] = pwdUser
	}

	if !reflect.DeepEqual(secret.ObjectMeta.OwnerReferences, generatedSecret.ObjectMeta.OwnerReferences) {
		updateSecret = true
		updateSecretMsg = fmt.Sprintf("secret %s owner references do not match the current ones", secretName)
		secret.ObjectMeta.OwnerReferences = generatedSecret.ObjectMeta.OwnerReferences
	}

	if updateSecret {
		c.logger.Infof("%s", updateSecretMsg)
		if secret, err = c.KubeClient.Secrets(secret.Namespace).Update(context.TODO(), secret, metav1.UpdateOptions{}); err != nil {
			return nil, fmt.Errorf("could not update secret: %v", err)
		}
	}

	if changed, _ := c.compareAnnotations(secret.Annotations, generatedSecret.Annotations, nil); changed {
		patchData, err := metaAnnotationsPatch(generatedSecret.Annotations)
		if err != nil {
			return nil, fmt.Errorf("could not form patch for secret annotations: %v", err)
		}
		secret, err = c.KubeClient.Secrets(secret.Namespace).Patch(context.TODO(), secret.Name, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
		if err != nil {
			return nil, fmt.Errorf("could not patch annotations for secret: %v", err)
		}
	}

	return secret, nil
}

func (c *Cluster) checkForPasswordRotation(
	secret *v1.Secret,
	secretUsername string,
	pwdUser spec.PgUser,
	retentionUsers *[]string,
	currentTime time.Time) (string, error) {

	var (
		passwordRotationMsg string
		err                 error
	)

	// if password rotation is enabled update password and username if rotation interval has been passed
	// rotation can be enabled globally or via the manifest (excluding the Postgres superuser)
	rotationEnabledInManifest := secretUsername != constants.SuperuserKeyName &&
		(slices.Contains(c.Spec.UsersWithSecretRotation, secretUsername) ||
			slices.Contains(c.Spec.UsersWithInPlaceSecretRotation, secretUsername))

	// globally enabled rotation is only allowed for manifest and bootstrapped roles
	allowedRoleTypes := []spec.RoleOrigin{spec.RoleOriginManifest, spec.RoleOriginBootstrap}
	rotationAllowed := !pwdUser.IsDbOwner && slices.Contains(allowedRoleTypes, pwdUser.Origin)

	// users can ignore any kind of rotation
	isIgnoringRotation := slices.Contains(c.Spec.UsersIgnoringSecretRotation, secretUsername)

	if ((c.OpConfig.EnablePasswordRotation && rotationAllowed) || rotationEnabledInManifest) && !isIgnoringRotation {
		passwordRotationMsg, err = c.rotatePasswordInSecret(secret, secretUsername, pwdUser.Origin, currentTime, retentionUsers)
		if err != nil {
			c.logger.Warnf("password rotation failed for user %s: %v", secretUsername, err)
		}
	} else {
		// username might not match if password rotation has been disabled again
		usernameFromSecret := string(secret.Data["username"])
		if secretUsername != usernameFromSecret {
			// handle edge case when manifest user conflicts with a user from prepared databases
			if strings.Replace(usernameFromSecret, "-", "_", -1) == strings.Replace(secretUsername, "-", "_", -1) {
				return "", fmt.Errorf("could not update secret because of user name mismatch: expected: %s, got: %s", secretUsername, usernameFromSecret)
			}
			*retentionUsers = append(*retentionUsers, secretUsername)
			secret.Data["username"] = []byte(secretUsername)
			secret.Data["password"] = []byte(util.RandomPassword(constants.PasswordLength))
			secret.Data["nextRotation"] = []byte{}
			passwordRotationMsg = fmt.Sprintf("secret does not contain the role %s - updating username and resetting password", secretUsername)
		}
	}

	return passwordRotationMsg, nil
}

func (c *Cluster) rotatePasswordInSecret(
	secret *v1.Secret,
	secretUsername string,
	roleOrigin spec.RoleOrigin,
	currentTime time.Time,
	retentionUsers *[]string) (string, error) {
	var (
		err                 error
		nextRotationDate    time.Time
		nextRotationDateStr string
		expectedUsername    string
		rotationModeChanged bool
		updateSecretMsg     string
	)

	secretName := util.NameFromMeta(secret.ObjectMeta)

	// initialize password rotation setting first rotation date
	nextRotationDateStr = string(secret.Data["nextRotation"])
	if nextRotationDate, err = time.ParseInLocation(time.RFC3339, nextRotationDateStr, currentTime.UTC().Location()); err != nil {
		nextRotationDate, nextRotationDateStr = c.getNextRotationDate(currentTime)
		secret.Data["nextRotation"] = []byte(nextRotationDateStr)
		updateSecretMsg = fmt.Sprintf("rotation date not found in secret %s. Setting it to %s", secretName, nextRotationDateStr)
	}

	// check if next rotation can happen sooner
	// if rotation interval has been decreased
	currentRotationDate, nextRotationDateStr := c.getNextRotationDate(currentTime)
	if nextRotationDate.After(currentRotationDate) {
		nextRotationDate = currentRotationDate
	}

	// set username and check if it differs from current value in secret
	currentUsername := string(secret.Data["username"])
	if !slices.Contains(c.Spec.UsersWithInPlaceSecretRotation, secretUsername) {
		expectedUsername = fmt.Sprintf("%s%s", secretUsername, currentTime.Format(constants.RotationUserDateFormat))
	} else {
		expectedUsername = secretUsername
	}

	// when changing to in-place rotation update secret immediatly
	// if currentUsername is longer we know it has a date suffix
	// the other way around we can wait until the next rotation date
	if len(currentUsername) > len(expectedUsername) {
		rotationModeChanged = true
		c.logger.Infof("updating secret %s after switching to in-place rotation mode for username: %s", secretName, string(secret.Data["username"]))
	}

	// update password and next rotation date if configured interval has passed
	if currentTime.After(nextRotationDate) || rotationModeChanged {
		// create rotation user if role is not listed for in-place password update
		if !slices.Contains(c.Spec.UsersWithInPlaceSecretRotation, secretUsername) {
			secret.Data["username"] = []byte(expectedUsername)
			c.logger.Infof("updating username in secret %s and creating rotation user %s in the database", secretName, expectedUsername)
			// whenever there is a rotation, check if old rotation users can be deleted
			*retentionUsers = append(*retentionUsers, secretUsername)
		} else {
			// when passwords of system users are rotated in-place, pods have to be replaced
			if roleOrigin == spec.RoleOriginSystem {
				pods, err := c.listPods()
				if err != nil {
					return "", fmt.Errorf("could not list pods of the statefulset: %v", err)
				}
				for _, pod := range pods {
					if err = c.markRollingUpdateForPod(&pod,
						fmt.Sprintf("replace pod due to password rotation of system user %s", secretUsername)); err != nil {
						c.logger.Warnf("marking pod for rolling update due to password rotation failed: %v", err)
					}
				}
			}

			// when password of connection pooler is rotated in-place, pooler pods have to be replaced
			if roleOrigin == spec.RoleOriginConnectionPooler {
				listOptions := metav1.ListOptions{
					LabelSelector: c.poolerLabelsSet(true).String(),
				}
				poolerPods, err := c.listPoolerPods(listOptions)
				if err != nil {
					return "", fmt.Errorf("could not list pods of the pooler deployment: %v", err)
				}
				for _, poolerPod := range poolerPods {
					if err = c.markRollingUpdateForPod(&poolerPod,
						fmt.Sprintf("replace pooler pod due to password rotation of pooler user %s", secretUsername)); err != nil {
						c.logger.Warnf("marking pooler pod for rolling update due to password rotation failed: %v", err)
					}
				}
			}

			// when password of stream user is rotated in-place, it should trigger rolling update in FES deployment
			if roleOrigin == spec.RoleOriginStream {
				c.logger.Warnf("password in secret of stream user %s changed", constants.EventStreamSourceSlotPrefix+constants.UserRoleNameSuffix)
			}

			secret.Data["username"] = []byte(secretUsername)
		}
		secret.Data["password"] = []byte(util.RandomPassword(constants.PasswordLength))
		secret.Data["nextRotation"] = []byte(nextRotationDateStr)
		updateSecretMsg = fmt.Sprintf("updating secret %s due to password rotation - next rotation date: %s", secretName, nextRotationDateStr)
	}

	return updateSecretMsg, nil
}

func (c *Cluster) updatePgUser(secretUsername string, degraded bool) {
	for key, pgUser := range c.pgUsers {
		if pgUser.Name == secretUsername {
			pgUser.Degraded = degraded
			c.pgUsers[key] = pgUser
			return
		}
	}
	for key, pgUser := range c.systemUsers {
		if pgUser.Name == secretUsername {
			pgUser.Degraded = degraded
			c.systemUsers[key] = pgUser
			return
		}
	}
}

func (c *Cluster) syncRoles() (err error) {
	c.setProcessName("syncing roles")

	var (
		dbUsers   spec.PgUserMap
		newUsers  spec.PgUserMap
		userNames []string
	)

	err = c.initDbConn()
	if err != nil {
		return fmt.Errorf("could not init db connection: %v", err)
	}

	defer func() {
		if err2 := c.closeDbConn(); err2 != nil {
			if err == nil {
				err = fmt.Errorf("could not close database connection: %v", err2)
			} else {
				err = fmt.Errorf("could not close database connection: %v (prior error: %v)", err2, err)
			}
		}
	}()

	// mapping between original role name and with deletion suffix
	deletedUsers := map[string]string{}
	newUsers = make(map[string]spec.PgUser)

	// create list of database roles to query
	for _, u := range c.pgUsers {
		pgRole := u.Name
		userNames = append(userNames, pgRole)

		// when a rotation happened add group role to query its rolconfig
		if u.Rotated {
			userNames = append(userNames, u.MemberOf[0])
		}

		// add team member role name with rename suffix in case we need to rename it back
		if u.Origin == spec.RoleOriginTeamsAPI && c.OpConfig.EnableTeamMemberDeprecation {
			deletedUsers[pgRole+c.OpConfig.RoleDeletionSuffix] = pgRole
			userNames = append(userNames, pgRole+c.OpConfig.RoleDeletionSuffix)
		}
	}

	// add team members that exist only in cache
	// to trigger a rename of the role in ProduceSyncRequests
	for _, cachedUser := range c.pgUsersCache {
		if _, exists := c.pgUsers[cachedUser.Name]; !exists {
			userNames = append(userNames, cachedUser.Name)
		}
	}

	// search also for system users
	for _, systemUser := range c.systemUsers {
		userNames = append(userNames, systemUser.Name)
		newUsers[systemUser.Name] = systemUser
	}

	dbUsers, err = c.readPgUsersFromDatabase(userNames)
	if err != nil {
		return fmt.Errorf("error getting users from the database: %v", err)
	}

DBUSERS:
	for _, dbUser := range dbUsers {
		// copy rolconfig to rotation users
		for pgUserName, pgUser := range c.pgUsers {
			if pgUser.Rotated && pgUser.MemberOf[0] == dbUser.Name {
				pgUser.Parameters = dbUser.Parameters
				c.pgUsers[pgUserName] = pgUser
				// remove group role from dbUsers to not count as deleted role
				delete(dbUsers, dbUser.Name)
				continue DBUSERS
			}
		}

		// update pgUsers where a deleted role was found
		// so that they are skipped in ProduceSyncRequests
		originalUsername, foundDeletedUser := deletedUsers[dbUser.Name]
		// check if original user does not exist in dbUsers
		_, originalUserAlreadyExists := dbUsers[originalUsername]
		if foundDeletedUser && !originalUserAlreadyExists {
			recreatedUser := c.pgUsers[originalUsername]
			recreatedUser.Deleted = true
			c.pgUsers[originalUsername] = recreatedUser
		}
	}

	// last but not least copy pgUsers to newUsers to send to ProduceSyncRequests
	for _, pgUser := range c.pgUsers {
		newUsers[pgUser.Name] = pgUser
	}

	pgSyncRequests := c.userSyncStrategy.ProduceSyncRequests(dbUsers, newUsers)
	if err = c.userSyncStrategy.ExecuteSyncRequests(pgSyncRequests, c.pgDb); err != nil {
		return fmt.Errorf("error executing sync statements: %v", err)
	}

	return nil
}

func (c *Cluster) syncDatabases() error {
	c.setProcessName("syncing databases")
	errors := make([]string, 0)
	createDatabases := make(map[string]string)
	alterOwnerDatabases := make(map[string]string)
	preparedDatabases := make([]string, 0)

	if err := c.initDbConn(); err != nil {
		return fmt.Errorf("could not init database connection")
	}
	defer func() {
		if err := c.closeDbConn(); err != nil {
			c.logger.Errorf("could not close database connection: %v", err)
		}
	}()

	currentDatabases, err := c.getDatabases()
	if err != nil {
		return fmt.Errorf("could not get current databases: %v", err)
	}

	// if no prepared databases are specified create a database named like the cluster
	if c.Spec.PreparedDatabases != nil && len(c.Spec.PreparedDatabases) == 0 { // TODO: add option to disable creating such a default DB
		c.Spec.PreparedDatabases = map[string]acidv1.PreparedDatabase{strings.Replace(c.Name, "-", "_", -1): {}}
	}
	for preparedDatabaseName := range c.Spec.PreparedDatabases {
		_, exists := currentDatabases[preparedDatabaseName]
		if !exists {
			createDatabases[preparedDatabaseName] = fmt.Sprintf("%s%s", preparedDatabaseName, constants.OwnerRoleNameSuffix)
			preparedDatabases = append(preparedDatabases, preparedDatabaseName)
		}
	}

	for databaseName, newOwner := range c.Spec.Databases {
		currentOwner, exists := currentDatabases[databaseName]
		if !exists {
			createDatabases[databaseName] = newOwner
		} else if currentOwner != newOwner {
			alterOwnerDatabases[databaseName] = newOwner
		}
	}

	if len(createDatabases)+len(alterOwnerDatabases) == 0 {
		return nil
	}

	for databaseName, owner := range createDatabases {
		if err = c.executeCreateDatabase(databaseName, owner); err != nil {
			errors = append(errors, err.Error())
		}
	}
	for databaseName, owner := range alterOwnerDatabases {
		if err = c.executeAlterDatabaseOwner(databaseName, owner); err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(createDatabases) > 0 {
		// trigger creation of pooler objects in new database in syncConnectionPooler
		if c.ConnectionPooler != nil {
			for _, role := range [2]PostgresRole{Master, Replica} {
				c.ConnectionPooler[role].LookupFunction = false
			}
		}
	}

	// set default privileges for prepared database
	for _, preparedDatabase := range preparedDatabases {
		if err := c.initDbConnWithName(preparedDatabase); err != nil {
			errors = append(errors, fmt.Sprintf("could not init database connection to %s", preparedDatabase))
			continue
		}

		for _, owner := range c.getOwnerRoles(preparedDatabase, c.Spec.PreparedDatabases[preparedDatabase].DefaultUsers) {
			if err = c.execAlterGlobalDefaultPrivileges(owner, preparedDatabase); err != nil {
				errors = append(errors, err.Error())
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("error(s) while syncing databases: %v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) syncPreparedDatabases() error {
	c.setProcessName("syncing prepared databases")
	errors := make([]string, 0)

	for preparedDbName, preparedDB := range c.Spec.PreparedDatabases {
		if err := c.initDbConnWithName(preparedDbName); err != nil {
			errors = append(errors, fmt.Sprintf("could not init connection to database %s: %v", preparedDbName, err))
			continue
		}

		c.logger.Debugf("syncing prepared database %q", preparedDbName)
		// now, prepare defined schemas
		preparedSchemas := preparedDB.PreparedSchemas
		if len(preparedDB.PreparedSchemas) == 0 {
			preparedSchemas = map[string]acidv1.PreparedSchema{"data": {DefaultRoles: util.True()}}
		}
		if err := c.syncPreparedSchemas(preparedDbName, preparedSchemas); err != nil {
			errors = append(errors, err.Error())
			continue
		}

		// install extensions
		if err := c.syncExtensions(preparedDB.Extensions); err != nil {
			errors = append(errors, err.Error())
		}

		if err := c.closeDbConn(); err != nil {
			c.logger.Errorf("could not close database connection: %v", err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("error(s) while syncing prepared databases: %v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) syncPreparedSchemas(databaseName string, preparedSchemas map[string]acidv1.PreparedSchema) error {
	c.setProcessName("syncing prepared schemas")
	errors := make([]string, 0)

	currentSchemas, err := c.getSchemas()
	if err != nil {
		return fmt.Errorf("could not get current schemas: %v", err)
	}

	var schemas []string

	for schema := range preparedSchemas {
		schemas = append(schemas, schema)
	}

	if createPreparedSchemas, equal := util.SubstractStringSlices(schemas, currentSchemas); !equal {
		for _, schemaName := range createPreparedSchemas {
			owner := constants.OwnerRoleNameSuffix
			dbOwner := fmt.Sprintf("%s%s", databaseName, owner)
			if preparedSchemas[schemaName].DefaultRoles == nil || *preparedSchemas[schemaName].DefaultRoles {
				owner = fmt.Sprintf("%s_%s%s", databaseName, schemaName, owner)
			} else {
				owner = dbOwner
			}
			if err = c.executeCreateDatabaseSchema(databaseName, schemaName, dbOwner, owner); err != nil {
				errors = append(errors, err.Error())
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("error(s) while syncing schemas of prepared databases: %v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) syncExtensions(extensions map[string]string) error {
	c.setProcessName("syncing database extensions")
	errors := make([]string, 0)
	createExtensions := make(map[string]string)
	alterExtensions := make(map[string]string)

	currentExtensions, err := c.getExtensions()
	if err != nil {
		return fmt.Errorf("could not get current database extensions: %v", err)
	}

	for extName, newSchema := range extensions {
		currentSchema, exists := currentExtensions[extName]
		if !exists {
			createExtensions[extName] = newSchema
		} else if currentSchema != newSchema {
			alterExtensions[extName] = newSchema
		}
	}

	for extName, schema := range createExtensions {
		if err = c.executeCreateExtension(extName, schema); err != nil {
			errors = append(errors, err.Error())
		}
	}
	for extName, schema := range alterExtensions {
		if err = c.executeAlterExtension(extName, schema); err != nil {
			errors = append(errors, err.Error())
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("error(s) while syncing database extensions: %v", strings.Join(errors, `', '`))
	}

	return nil
}

func (c *Cluster) syncLogicalBackupJob() error {
	var (
		job        *batchv1.CronJob
		desiredJob *batchv1.CronJob
		err        error
	)
	c.setProcessName("syncing the logical backup job")

	// sync the job if it exists

	jobName := c.getLogicalBackupJobName()
	if job, err = c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Get(context.TODO(), jobName, metav1.GetOptions{}); err == nil {

		desiredJob, err = c.generateLogicalBackupJob()
		if err != nil {
			return fmt.Errorf("could not generate the desired logical backup job state: %v", err)
		}
		if !reflect.DeepEqual(job.ObjectMeta.OwnerReferences, desiredJob.ObjectMeta.OwnerReferences) {
			c.logger.Info("new logical backup job's owner references do not match the current ones")
			job, err = c.KubeClient.CronJobs(job.Namespace).Update(context.TODO(), desiredJob, metav1.UpdateOptions{})
			if err != nil {
				return fmt.Errorf("could not update owner references for logical backup job %q: %v", job.Name, err)
			}
			c.logger.Infof("logical backup job %s updated", c.getLogicalBackupJobName())
		}
		if cmp := c.compareLogicalBackupJob(job, desiredJob); !cmp.match {
			c.logger.Infof("logical job %s is not in the desired state and needs to be updated",
				c.getLogicalBackupJobName(),
			)
			if len(cmp.reasons) != 0 {
				for _, reason := range cmp.reasons {
					c.logger.Infof("reason: %s", reason)
				}
			}
			if len(cmp.deletedPodAnnotations) != 0 {
				templateMetadataReq := map[string]map[string]map[string]map[string]map[string]map[string]map[string]*string{
					"spec": {"jobTemplate": {"spec": {"template": {"metadata": {"annotations": {}}}}}}}
				for _, anno := range cmp.deletedPodAnnotations {
					templateMetadataReq["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["annotations"][anno] = nil
				}
				patch, err := json.Marshal(templateMetadataReq)
				if err != nil {
					return fmt.Errorf("could not marshal ObjectMeta for logical backup job %q pod template: %v", jobName, err)
				}

				job, err = c.KubeClient.CronJobs(c.Namespace).Patch(context.TODO(), jobName, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "")
				if err != nil {
					c.logger.Errorf("failed to remove annotations from the logical backup job %q pod template: %v", jobName, err)
					return err
				}
			}
			if err = c.patchLogicalBackupJob(desiredJob); err != nil {
				return fmt.Errorf("could not update logical backup job to match desired state: %v", err)
			}
			c.logger.Info("the logical backup job is synced")
		}
		if !reflect.DeepEqual(job.Labels, desiredJob.Labels) {
			patchData, err := metaLabelsPatch(desiredJob.Labels)
			if err != nil {
				return fmt.Errorf("could not form patch for the logical backup job %q labels: %v", jobName, err)
			}
			_, err = c.KubeClient.CronJobs(c.Namespace).Patch(context.TODO(), jobName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("could not patch labels of the logical backup job %q: %v", jobName, err)
			}
		}
		if changed, _ := c.compareAnnotations(job.Annotations, desiredJob.Annotations, nil); changed {
			patchData, err := metaAnnotationsPatch(desiredJob.Annotations)
			if err != nil {
				return fmt.Errorf("could not form patch for the logical backup job %q: %v", jobName, err)
			}
			_, err = c.KubeClient.CronJobs(c.Namespace).Patch(context.TODO(), jobName, types.MergePatchType, []byte(patchData), metav1.PatchOptions{})
			if err != nil {
				return fmt.Errorf("could not patch annotations of the logical backup job %q: %v", jobName, err)
			}
		}
		c.LogicalBackupJob = desiredJob
		return nil
	}
	if !k8sutil.ResourceNotFound(err) {
		return fmt.Errorf("could not get logical backp job: %v", err)
	}

	// no existing logical backup job, create new one
	c.logger.Info("could not find the cluster's logical backup job")

	if err = c.createLogicalBackupJob(); err == nil {
		c.logger.Infof("created missing logical backup job %s", jobName)
	} else {
		if !k8sutil.ResourceAlreadyExists(err) {
			return fmt.Errorf("could not create missing logical backup job: %v", err)
		}
		c.logger.Infof("logical backup job %s already exists", jobName)
		if _, err = c.KubeClient.CronJobsGetter.CronJobs(c.Namespace).Get(context.TODO(), jobName, metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not fetch existing logical backup job: %v", err)
		}
	}

	return nil
}

func (c *Cluster) syncPodServiceAccount() error {
	sa, err := c.KubeClient.ServiceAccounts(c.Namespace).Get(context.TODO(), c.OpConfig.PodServiceAccountName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not get pod service account %q: %v", c.OpConfig.PodServiceAccountName, err)
	}

	changed := false

	if c.OpConfig.IRSARoleARN != "" {
		if val, ok := sa.Annotations[constants.IRSAAnnotation]; !ok || val != c.OpConfig.IRSARoleARN {
			if sa.Annotations == nil {
				sa.Annotations = make(map[string]string)
			}
			sa.Annotations[constants.IRSAAnnotation] = c.OpConfig.IRSARoleARN
			changed = true
		}
	} else {
		if _, ok := sa.Annotations[constants.IRSAAnnotation]; ok {
			delete(sa.Annotations, constants.IRSAAnnotation)
			changed = true
		}
	}

	if changed {
		if _, err = c.KubeClient.ServiceAccounts(c.Namespace).Update(context.TODO(), sa, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("could not update pod service account %q: %v", sa.Name, err)
		}
		c.logger.Infof("synced annotations on pod service account %q", sa.Name)
	}

	if c.OpConfig.IRSARoleARN != "" {
		c.logIRSAMigrationProgress()
	}

	return nil
}

func (c *Cluster) logIRSAMigrationProgress() {
	pods, err := c.listPods()
	if err != nil {
		c.logger.Warnf("IRSA migration: could not list pods: %v", err)
		return
	}

	total := len(pods)
	remaining := 0
	for _, pod := range pods {
		if _, ok := pod.Annotations[constants.KubeIAmAnnotation]; ok {
			remaining++
		}
	}

	if remaining > 0 {
		c.logger.Infof("IRSA migration in progress: %d/%d pods still carry kube2iam annotation, will be removed on next rotation", remaining, total)
	} else {
		c.logger.Infof("IRSA migration complete: all %d pods have rotated, kube2iam annotation fully drained", total)
	}
}
