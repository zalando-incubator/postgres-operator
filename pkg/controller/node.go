package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/pkg/api/v1"

	"github.com/zalando-incubator/postgres-operator/pkg/cluster"
	"github.com/zalando-incubator/postgres-operator/pkg/util"
)

func (c *Controller) nodeListFunc(options metav1.ListOptions) (runtime.Object, error) {
	opts := metav1.ListOptions{
		Watch:           options.Watch,
		ResourceVersion: options.ResourceVersion,
		TimeoutSeconds:  options.TimeoutSeconds,
	}

	return c.KubeClient.Nodes().List(opts)
}

func (c *Controller) nodeWatchFunc(options metav1.ListOptions) (watch.Interface, error) {
	opts := metav1.ListOptions{
		Watch:           options.Watch,
		ResourceVersion: options.ResourceVersion,
		TimeoutSeconds:  options.TimeoutSeconds,
	}

	return c.KubeClient.Nodes().Watch(opts)
}

func (c *Controller) nodeAdd(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}

	c.logger.Debugf("new node has been added: %q (%s)", util.NameFromMeta(node.ObjectMeta), node.Spec.ProviderID)
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
	if nodePrev.Spec.Unschedulable && !util.MapContains(nodePrev.Labels, c.opConfig.NodeReadinessLabel) ||
		!nodeCur.Spec.Unschedulable || util.MapContains(nodeCur.Labels, c.opConfig.NodeReadinessLabel) {
		return
	}
	c.movePodsOffNode(nodeCur)
}

func (c *Controller) movePodsOffNode(node *v1.Node) {
	nameFromMeta := util.NameFromMeta(node.ObjectMeta)
	fromMeta := nameFromMeta
	meta := fromMeta
	c.logger.Infof("moving pods: node %q became unschedulable and does not have a ready label: %q",
		meta, c.opConfig.NodeReadinessLabel)

	opts := metav1.ListOptions{
		LabelSelector: labels.Set(c.opConfig.ClusterLabels).String(),
	}
	podList, err := c.KubeClient.Pods(c.opConfig.Namespace).List(opts)
	if err != nil {
		c.logger.Errorf("could not fetch list of the pods: %v", err)
		return
	}

	nodePods := make([]*v1.Pod, 0)
	for i, pod := range podList.Items {
		if pod.Spec.NodeName == node.Name {
			nodePods = append(nodePods, &podList.Items[i])
		}
	}

	clusters := make(map[*cluster.Cluster]bool)
	masterPods := make(map[*v1.Pod]*cluster.Cluster)
	replicaPods := make(map[*v1.Pod]*cluster.Cluster)
	movedPods := 0
	for _, pod := range nodePods {
		podName := util.NameFromMeta(pod.ObjectMeta)

		role, ok := pod.Labels[c.opConfig.PodRoleLabel]
		if !ok {
			c.logger.Warningf("could not move pod %q: pod has no role", podName)
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

		movedPods++

		if !clusters[cl] {
			clusters[cl] = true
		}

		if cluster.PostgresRole(role) == cluster.Master {
			masterPods[pod] = cl
		} else {
			replicaPods[pod] = cl
		}
	}

	for cl := range clusters {
		cl.Lock()
	}

	for pod, cl := range masterPods {
		podName := util.NameFromMeta(pod.ObjectMeta)

		if err := cl.MigrateMasterPod(podName); err != nil {
			c.logger.Errorf("could not move master pod %q: %v", podName, err)
			movedPods--
		}
	}

	for pod, cl := range replicaPods {
		podName := util.NameFromMeta(pod.ObjectMeta)

		if err := cl.MigrateReplicaPod(podName, node.Name); err != nil {
			c.logger.Errorf("could not move replica pod %q: %v", podName, err)
			movedPods--
		}
	}

	for cl := range clusters {
		cl.Unlock()
	}

	totalPods := len(nodePods)

	c.logger.Infof("%d/%d pods have been moved out from the %q node",
		movedPods, totalPods, meta)

	if leftPods := totalPods - movedPods; leftPods > 0 {
		c.logger.Warnf("could not move %d/%d pods from the %q node",
			leftPods, totalPods, meta)
	}
}

func (c *Controller) nodeDelete(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return
	}

	c.logger.Debugf("node has been deleted: %q (%s)", util.NameFromMeta(node.ObjectMeta), node.Spec.ProviderID)
}
