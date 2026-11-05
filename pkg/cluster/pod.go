package cluster

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/patroni"
	"github.com/zalando/postgres-operator/v2/pkg/util/retryutil"
)

func (c *Cluster) listPods() ([]v1.Pod, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: c.labelsSet(false).String(),
	}

	pods, err := c.KubeClient.Pods(c.Namespace).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("could not get list of pods: %v", err)
	}

	return pods.Items, nil
}

func (c *Cluster) getRolePods(role PostgresRole) ([]v1.Pod, error) {
	listOptions := metav1.ListOptions{
		LabelSelector: c.roleLabelsSet(false, role).String(),
	}

	pods, err := c.KubeClient.Pods(c.Namespace).List(context.TODO(), listOptions)
	if err != nil {
		return nil, fmt.Errorf("could not get list of pods: %v", err)
	}

	if role == Master && len(pods.Items) > 1 {
		return nil, fmt.Errorf("too many masters")
	}

	return pods.Items, nil
}

// getRollingUpdateAnnotationFromPod returns the value of the rollingUpdate annotation from the given pod
func (c *Cluster) getRollingUpdateAnnotationFromPod(pod *v1.Pod) string {
	return pod.GetAnnotations()[rollingUpdatePodAnnotationKey]
}

// markRollingUpdateForPod sets the indicator for the rolling update requirement
// in the Pod annotation.
func (c *Cluster) markRollingUpdateForPod(pod *v1.Pod, msg string) error {
	// exit early if rolling update annotation is already set to true
	if c.getRollingUpdateAnnotationFromPod(pod) == "true" {
		return nil
	}

	c.logger.Infof("mark rolling update annotation for %s: reason %s", pod.Name, msg)
	annotations := make(map[string]string)
	annotationValue := strconv.FormatBool(true)
	annotations[rollingUpdatePodAnnotationKey] = annotationValue

	patchData, err := metaAnnotationsPatch(annotations)
	if err != nil {
		return fmt.Errorf("could not form patch for pod's rolling update annotation: %v", err)
	}

	err = retryutil.Retry(1*time.Second, 5*time.Second,
		func() (bool, error) {
			_, err2 := c.KubeClient.Pods(pod.Namespace).Patch(
				context.TODO(),
				pod.Name,
				types.MergePatchType,
				[]byte(patchData),
				metav1.PatchOptions{},
				"")
			if err2 != nil {
				return false, err2
			}
			return true, nil
		})
	if err != nil {
		return fmt.Errorf("could not patch pod rolling update annotation %q: %v", patchData, err)
	}

	// reflect the patch locally, so callers inspecting the annotation right after marking see it
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[rollingUpdatePodAnnotationKey] = annotationValue

	return nil
}

func (c *Cluster) deletePods() error {
	c.logger.Debug("deleting pods")
	pods, err := c.listPods()
	if err != nil {
		return err
	}

	for _, obj := range pods {
		podName := util.NameFromMeta(obj.ObjectMeta)

		c.logger.Debugf("deleting pod %q", podName)
		if err := c.deletePod(podName); err != nil {
			c.logger.Errorf("could not delete pod %q: %v", podName, err)
		} else {
			c.logger.Infof("pod %q has been deleted", podName)
		}
	}
	if len(pods) > 0 {
		c.logger.Debug("pods have been deleted")
	} else {
		c.logger.Debug("no pods to delete")
	}

	return nil
}

func (c *Cluster) deletePod(podName spec.NamespacedName) error {
	c.setProcessName("deleting pod %q", podName)
	ch := c.registerPodSubscriber(podName)
	defer c.unregisterPodSubscriber(podName)

	if err := c.KubeClient.Pods(podName.Namespace).Delete(context.TODO(), podName.Name, c.deleteOptions); err != nil {
		return err
	}

	return c.waitForPodDeletion(ch)
}

func (c *Cluster) unregisterPodSubscriber(podName spec.NamespacedName) {
	c.logger.Debugf("unsubscribing from pod %q events", podName)
	c.podSubscribersMu.Lock()
	defer c.podSubscribersMu.Unlock()

	ch, ok := c.podSubscribers[podName]
	if !ok {
		panic("subscriber for pod '" + podName.String() + "' is not found")
	}

	delete(c.podSubscribers, podName)
	close(ch)
}

func (c *Cluster) registerPodSubscriber(podName spec.NamespacedName) chan PodEvent {
	c.logger.Debugf("subscribing to pod %q", podName)
	c.podSubscribersMu.Lock()
	defer c.podSubscribersMu.Unlock()

	ch := make(chan PodEvent)
	if _, ok := c.podSubscribers[podName]; ok {
		panic("pod '" + podName.String() + "' is already subscribed")
	}
	c.podSubscribers[podName] = ch

	return ch
}

func (c *Cluster) movePodFromEndOfLifeNode(pod *v1.Pod) (*v1.Pod, error) {
	var (
		eol    bool
		err    error
		newPod *v1.Pod
	)
	podName := util.NameFromMeta(pod.ObjectMeta)

	if eol, err = c.podIsEndOfLife(pod); err != nil {
		return nil, fmt.Errorf("could not get node %q: %v", pod.Spec.NodeName, err)
	} else if !eol {
		c.logger.Infof("check failed: pod %q is already on a live node", podName)
		return pod, nil
	}

	c.setProcessName("moving pod %q out of end-of-life node %q", podName, pod.Spec.NodeName)
	c.logger.Infof("moving pod %q out of the end-of-life node %q", podName, pod.Spec.NodeName)

	if newPod, err = c.recreatePod(podName); err != nil {
		return nil, fmt.Errorf("could not move pod: %v", err)
	}

	if newPod.Spec.NodeName == pod.Spec.NodeName {
		return nil, fmt.Errorf("pod %q remained on the same node", podName)
	}

	if eol, err = c.podIsEndOfLife(newPod); err != nil {
		return nil, fmt.Errorf("could not get node %q: %v", pod.Spec.NodeName, err)
	} else if eol {
		c.logger.Warningf("pod %q moved to end-of-life node %q", podName, newPod.Spec.NodeName)
		return newPod, nil
	}

	c.logger.Infof("pod %q moved from node %q to node %q", podName, pod.Spec.NodeName, newPod.Spec.NodeName)

	return newPod, nil
}

// MigrateMasterPod migrates master pod via failover to a replica
func (c *Cluster) MigrateMasterPod(podName spec.NamespacedName) error {
	var (
		err error
		eol bool
	)

	oldMaster, err := c.KubeClient.Pods(podName.Namespace).Get(context.TODO(), podName.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not get master pod: %v", err)
	}

	c.logger.Infof("starting process to migrate master pod %q", podName)
	if eol, err = c.podIsEndOfLife(oldMaster); err != nil {
		return fmt.Errorf("could not get node %q: %v", oldMaster.Spec.NodeName, err)
	}
	if !eol {
		c.logger.Debug("no action needed: master pod is already on a live node")
		return nil
	}

	if role := PostgresRole(oldMaster.Labels[c.OpConfig.PodRoleLabel]); role != Master {
		c.logger.Warningf("no action needed: pod %q is not the master (anymore)", podName)
		return nil
	}
	// we must have a statefulset in the cluster for the migration to work
	if c.Statefulset == nil {
		var sset *appsv1.StatefulSet
		if sset, err = c.KubeClient.StatefulSets(c.Namespace).Get(
			context.TODO(),
			c.statefulSetName(),
			metav1.GetOptions{}); err != nil {
			return fmt.Errorf("could not retrieve cluster statefulset: %v", err)
		}
		c.Statefulset = sset
	}
	// we may not have a cached statefulset if the initial cluster sync has aborted, revert to the spec in that case
	masterCandidateName := podName
	var masterCandidatePod *v1.Pod
	if *c.Statefulset.Spec.Replicas > 1 {
		if masterCandidateName, err = c.getSwitchoverCandidate(oldMaster); err != nil {
			return fmt.Errorf("could not find suitable replica pod as candidate for failover: %v", err)
		}
		masterCandidatePod, err = c.KubeClient.Pods(masterCandidateName.Namespace).Get(context.TODO(), masterCandidateName.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("could not get master candidate pod: %v", err)
		}
	} else {
		c.logger.Warningf("migrating single pod cluster %q, this will cause downtime of the Postgres cluster until pod is back", c.clusterName())
	}

	// there are two cases for each postgres cluster that has its master pod on the node to migrate from:
	// - the cluster has some replicas - migrate one of those if necessary and failover to it
	// - there are no replicas - just terminate the master and wait until it respawns
	// in both cases the result is the new master up and running on a new node.

	if masterCandidatePod == nil {
		if _, err = c.movePodFromEndOfLifeNode(oldMaster); err != nil {
			return fmt.Errorf("could not move pod: %v", err)
		}
		return nil
	}

	if _, err = c.movePodFromEndOfLifeNode(masterCandidatePod); err != nil {
		return fmt.Errorf("could not move pod: %v", err)
	}

	scheduleSwitchover := false
	if !c.isInMaintenanceWindow(c.Spec.MaintenanceWindows) {
		c.logger.Infof("postponing switchover, not in maintenance window")
		scheduleSwitchover = true
	}
	err = retryutil.Retry(1*time.Minute, 5*time.Minute,
		func() (bool, error) {
			err := c.Switchover(oldMaster, masterCandidateName, scheduleSwitchover)
			if err != nil {
				c.logger.Errorf("could not switchover to pod %q: %v", masterCandidateName, err)
				return false, nil
			}
			return true, nil
		},
	)

	if err != nil {
		return fmt.Errorf("could not migrate master pod: %v", err)
	}

	return nil
}

// MigrateReplicaPod recreates pod on a new node
func (c *Cluster) MigrateReplicaPod(podName spec.NamespacedName, fromNodeName string) error {
	replicaPod, err := c.KubeClient.Pods(podName.Namespace).Get(context.TODO(), podName.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("could not get pod: %v", err)
	}

	c.logger.Infof("migrating replica pod %q to live node", podName)

	if replicaPod.Spec.NodeName != fromNodeName {
		c.logger.Infof("check failed: pod %q has already migrated to node %q", podName, replicaPod.Spec.NodeName)
		return nil
	}

	if role := PostgresRole(replicaPod.Labels[c.OpConfig.PodRoleLabel]); role != Replica {
		return fmt.Errorf("check failed: pod %q is not a replica", podName)
	}

	_, err = c.movePodFromEndOfLifeNode(replicaPod)
	if err != nil {
		return fmt.Errorf("could not move pod: %v", err)
	}

	return nil
}

func (c *Cluster) getPatroniConfig(pod *v1.Pod) (acidv1.Patroni, map[string]string, error) {
	var (
		patroniConfig acidv1.Patroni
		pgParameters  map[string]string
	)
	podName := util.NameFromMeta(pod.ObjectMeta)
	err := retryutil.Retry(c.OpConfig.PatroniAPICheckInterval.Duration, c.OpConfig.PatroniAPICheckTimeout.Duration,
		func() (bool, error) {
			var err error
			patroniConfig, pgParameters, err = c.patroni.GetConfig(pod)

			if err != nil {
				return false, err
			}
			return true, nil
		},
	)

	if err != nil {
		return acidv1.Patroni{}, nil, fmt.Errorf("could not get Postgres config from pod %s: %v", podName, err)
	}

	return patroniConfig, pgParameters, nil
}

func (c *Cluster) getPatroniMemberData(pod *v1.Pod) (patroni.MemberData, error) {
	var memberData patroni.MemberData
	err := retryutil.Retry(c.OpConfig.PatroniAPICheckInterval.Duration, c.OpConfig.PatroniAPICheckTimeout.Duration,
		func() (bool, error) {
			var err error
			memberData, err = c.patroni.GetMemberData(pod)

			if err != nil {
				return false, err
			}
			return true, nil
		},
	)
	if err != nil {
		return patroni.MemberData{}, fmt.Errorf("could not get member data: %v", err)
	}
	if memberData.State == "creating replica" {
		return patroni.MemberData{}, fmt.Errorf("replica currently being initialized")
	}

	return memberData, nil
}

// podIsNotRunning returns true if a pod is known to be in a non-running state,
// e.g. stuck in CreateContainerConfigError, CrashLoopBackOff, ImagePullBackOff, etc.
// Pods with no status information are not considered non-running, as they may
// simply not have reported status yet.
func podIsNotRunning(pod *v1.Pod) bool {
	if pod.Status.Phase == "" {
		// No status reported yet — don't treat as non-running
		return false
	}
	if pod.Status.Phase != v1.PodRunning {
		return true
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil || cs.State.Terminated != nil {
			return true
		}
	}
	return false
}

// allPodsRunning returns true only if every pod in the list is in a healthy running state.
func (c *Cluster) allPodsRunning(pods []v1.Pod) bool {
	for i := range pods {
		if podIsNotRunning(&pods[i]) {
			return false
		}
	}
	return true
}

func (c *Cluster) recreatePod(podName spec.NamespacedName) (*v1.Pod, error) {
	stopCh := make(chan struct{})
	ch := c.registerPodSubscriber(podName)
	defer c.unregisterPodSubscriber(podName)
	defer close(stopCh)

	err := retryutil.Retry(1*time.Second, 5*time.Second,
		func() (bool, error) {
			err2 := c.KubeClient.Pods(podName.Namespace).Delete(
				context.TODO(),
				podName.Name,
				c.deleteOptions)
			if err2 != nil {
				return false, err2
			}
			return true, nil
		})
	if err != nil {
		return nil, fmt.Errorf("could not delete pod: %v", err)
	}

	if err := c.waitForPodDeletion(ch); err != nil {
		return nil, err
	}
	pod, err := c.waitForPodLabel(ch, stopCh, nil)
	if err != nil {
		return nil, err
	}
	c.logger.Infof("pod %q has been recreated", podName)
	return pod, nil
}

func (c *Cluster) recreatePods(pods []v1.Pod, switchoverCandidates []spec.NamespacedName) error {
	c.setProcessName("starting to recreate pods")
	c.logger.Infof("there are %d pods in the cluster to recreate", len(pods))

	var (
		masterPod, newMasterPod *v1.Pod
	)
	replicas := switchoverCandidates

	for i, pod := range pods {
		role := PostgresRole(pod.Labels[c.OpConfig.PodRoleLabel])

		if role == Master {
			masterPod = &pods[i]
			continue
		}

		podName := util.NameFromMeta(pods[i].ObjectMeta)
		newPod, err := c.recreatePod(podName)
		if err != nil {
			return fmt.Errorf("could not recreate replica pod %q: %v", util.NameFromMeta(pod.ObjectMeta), err)
		}

		newRole := PostgresRole(newPod.Labels[c.OpConfig.PodRoleLabel])
		switch newRole {
		case Replica:
			replicas = append(replicas, util.NameFromMeta(pod.ObjectMeta))
		case Master:
			newMasterPod = newPod
		}
	}

	if masterPod != nil {
		// switchover if
		// 1. we have not observed a new master pod when re-creating former replicas
		// 2. we know possible switchover targets even when no replicas were recreated
		// 3. the master pod is actually running (can't switchover a dead master)
		if newMasterPod == nil && len(replicas) > 0 && !podIsNotRunning(masterPod) {
			masterCandidate, err := c.getSwitchoverCandidate(masterPod)
			if err != nil {
				// do not recreate master now so it will keep the update annotation and switchover will be retried on next sync
				return fmt.Errorf("skipping switchover: %v", err)
			}
			if err := c.Switchover(masterPod, masterCandidate, false); err != nil {
				return fmt.Errorf("could not perform switch over: %v", err)
			}
		} else if newMasterPod == nil && len(replicas) == 0 {
			c.logger.Warningf("cannot perform switch over before re-creating the pod: no replicas")
		} else if podIsNotRunning(masterPod) {
			c.logger.Warningf("master pod %q is not running, skipping switchover and recreating directly",
				util.NameFromMeta(masterPod.ObjectMeta))
		}
		c.logger.Infof("recreating old master pod %q", util.NameFromMeta(masterPod.ObjectMeta))

		if _, err := c.recreatePod(util.NameFromMeta(masterPod.ObjectMeta)); err != nil {
			return fmt.Errorf("could not recreate old master pod %q: %v", util.NameFromMeta(masterPod.ObjectMeta), err)
		}
	}

	return nil
}

func (c *Cluster) getSwitchoverCandidate(master *v1.Pod) (spec.NamespacedName, error) {

	var members []patroni.ClusterMember
	candidates := make([]patroni.ClusterMember, 0)
	syncCandidates := make([]patroni.ClusterMember, 0)

	err := retryutil.Retry(c.OpConfig.PatroniAPICheckInterval.Duration, c.OpConfig.PatroniAPICheckTimeout.Duration,
		func() (bool, error) {
			var err error
			members, err = c.patroni.GetClusterMembers(master)
			if err != nil {
				return false, err
			}

			// look for SyncStandby candidates (which also implies pod is in running state)
			for _, member := range members {
				if PostgresRole(member.Role) == SyncStandby {
					syncCandidates = append(syncCandidates, member)
				}
				if PostgresRole(member.Role) != Leader && PostgresRole(member.Role) != StandbyLeader && slices.Contains([]string{"running", "streaming", "in archive recovery"}, member.State) {
					candidates = append(candidates, member)
				}
			}

			// if synchronous mode is enabled and no SyncStandy was found
			// return false for retry - cannot failover with no sync candidate
			if c.Spec.Patroni.SynchronousMode && len(syncCandidates) == 0 {
				c.logger.Warnf("no sync standby found - retrying fetching cluster members")
				return false, nil
			}

			// retry also in asynchronous mode when no replica candidate was found
			if !c.Spec.Patroni.SynchronousMode && len(candidates) == 0 {
				c.logger.Warnf("no replica candidate found - retrying fetching cluster members")
				return false, nil
			}

			return true, nil
		},
	)
	if err != nil {
		return spec.NamespacedName{}, fmt.Errorf("failed to get Patroni cluster members: %s", err)
	}

	// pick candidate with lowest lag
	if len(syncCandidates) > 0 {
		sort.Slice(syncCandidates, func(i, j int) bool {
			return syncCandidates[i].Lag < syncCandidates[j].Lag
		})
		return spec.NamespacedName{Namespace: master.Namespace, Name: syncCandidates[0].Name}, nil
	}
	if len(candidates) > 0 {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Lag < candidates[j].Lag
		})
		return spec.NamespacedName{Namespace: master.Namespace, Name: candidates[0].Name}, nil
	}

	return spec.NamespacedName{}, fmt.Errorf("no switchover candidate found")
}

func (c *Cluster) podIsEndOfLife(pod *v1.Pod) (bool, error) {
	node, err := c.KubeClient.Nodes().Get(context.TODO(), pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return node.Spec.Unschedulable || !util.MapContains(node.Labels, c.OpConfig.NodeReadinessLabel), nil

}
