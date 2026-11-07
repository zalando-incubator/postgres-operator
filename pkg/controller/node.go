package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/zalando/postgres-operator/v2/pkg/util/retryutil"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/zalando/postgres-operator/v2/pkg/cluster"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
)

func (c *Controller) nodeAdd(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}

	c.logger.Debugf("new node has been added: %s (%s)", util.NameFromMeta(node.ObjectMeta), node.Spec.ProviderID)

	// check if the node became not ready while the operator was down (otherwise we would have caught it in nodeUpdate)
	if !c.nodeIsReady(node) {
		c.moveMasterPodsOffNode(node)
	}
}

func (c *Controller) nodeUpdate(prev, cur interface{}) {
	nodePrev, ok := prev.(*v1.Node)
	if !ok {
		return
	}

	nodeCur, ok := cur.(*v1.Node)
	if !ok {
		return
	}

	if util.MapContains(nodeCur.Labels, map[string]string{"master": "true"}) {
		return
	}

	// do nothing if the node should have already triggered an update or
	// if only one of the label and the unschedulability criteria are met.
	if !c.nodeIsReady(nodePrev) || c.nodeIsReady(nodeCur) {
		return
	}

	c.moveMasterPodsOffNode(nodeCur)

}

func (c *Controller) nodeIsReady(node *v1.Node) bool {
	if util.MapContains(node.Labels, map[string]string{"master": "true"}) {
		return true
	}
	// a node carrying one of the configured taints (e.g. karpenter.sh/disrupted)
	// is being decommissioned, even if it was not cordoned
	if k8sutil.NodeHasAnyTaint(node, c.opConfig.NodeUnreadyTaints) {
		return false
	}
	return !node.Spec.Unschedulable || (len(c.opConfig.NodeReadinessLabel) > 0 && util.MapContains(node.Labels, c.opConfig.NodeReadinessLabel))
}

func (c *Controller) attemptToMoveMasterPodsOffNode(node *v1.Node) error {
	nodeName := util.NameFromMeta(node.ObjectMeta)
	c.logger.Infof("moving pods: node %q became unschedulable and does not have a ready label %q, or has one of the unready taints %q",
		nodeName, c.opConfig.NodeReadinessLabel, c.opConfig.NodeUnreadyTaints)

	opts := metav1.ListOptions{
		LabelSelector: labels.Set(c.opConfig.ClusterLabels).String(),
	}
	podList, err := c.KubeClient.Pods(c.opConfig.WatchedNamespace).List(context.TODO(), opts)
	if err != nil {
		c.logger.Errorf("could not fetch list of the pods: %v", err)
		return err
	}

	nodePods := make([]*v1.Pod, 0)
	for i, pod := range podList.Items {
		if pod.Spec.NodeName == node.Name {
			nodePods = append(nodePods, &podList.Items[i])
		}
	}

	clusters := make(map[*cluster.Cluster]bool)
	masterPods := make(map[*v1.Pod]*cluster.Cluster)
	movedPods := 0
	for _, pod := range nodePods {
		podName := util.NameFromMeta(pod.ObjectMeta)

		role, ok := pod.Labels[c.opConfig.PodRoleLabel]
		if !ok || cluster.PostgresRole(role) != cluster.Master {
			if !ok {
				c.logger.Warningf("could not move pod %q: pod has no role", podName)
			}
			continue
		}

		clusterName := c.podClusterName(pod)

		c.clustersMu.RLock()
		cl, ok := c.clusters[clusterName]
		c.clustersMu.RUnlock()
		if !ok {
			c.logger.Warningf("could not move pod %q: pod does not belong to a known cluster", podName)
			continue
		}

		if !clusters[cl] {
			clusters[cl] = true
		}

		masterPods[pod] = cl
	}

	for cl := range clusters {
		cl.Lock()
	}

	for pod, cl := range masterPods {
		podName := util.NameFromMeta(pod.ObjectMeta)

		if err := cl.MigrateMasterPod(podName); err != nil {
			c.logger.Errorf("could not move master pod %q: %v", podName, err)
		} else {
			movedPods++
		}
	}

	for cl := range clusters {
		cl.Unlock()
	}

	totalPods := len(masterPods)

	c.logger.Infof("%d/%d master pods have been moved out from the %q node",
		movedPods, totalPods, nodeName)

	if leftPods := totalPods - movedPods; leftPods > 0 {
		return fmt.Errorf("could not move master %d/%d pods from the %q node",
			leftPods, totalPods, nodeName)
	}

	return nil
}

func (c *Controller) nodeDelete(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}

	c.logger.Debugf("node has been deleted: %q (%s)", util.NameFromMeta(node.ObjectMeta), node.Spec.ProviderID)
}

func (c *Controller) moveMasterPodsOffNode(node *v1.Node) {
	// retry to move master until configured timeout is reached
	err := retryutil.Retry(1*time.Minute, c.opConfig.MasterPodMoveTimeout.Duration,
		func() (bool, error) {
			err := c.attemptToMoveMasterPodsOffNode(node)
			if err != nil {
				c.logger.Warningf("attempt to move master pods off node %q failed, will retry: %v", node.Name, err)
				return false, nil
			}
			return true, nil
		},
	)

	if err != nil {
		c.logger.Warningf("failed to move master pods from the node %q: %v", node.Name, err)
	}

}
