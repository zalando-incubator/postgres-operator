package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/pkg/cluster"
	"github.com/zalando/postgres-operator/pkg/spec"
	"github.com/zalando/postgres-operator/pkg/util"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/pkg/util/ringlog"
)

func (c *Controller) clusterResync(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(c.opConfig.ResyncPeriod)

	for {
		select {
		case <-ticker.C:
			if err := c.clusterListAndSync(); err != nil {
				c.logger.Errorf("could not list clusters: %v", err)
			}
		case <-stopCh:
			return
		}
	}
}

// clusterListFunc obtains a list of all PostgreSQL clusters
func (c *Controller) listClusters(options metav1.ListOptions) (*acidv1.PostgresqlList, error) {
	var pgList acidv1.PostgresqlList

	// TODO: use the SharedInformer cache instead of quering Kubernetes API directly.
	list, err := c.KubeClient.AcidV1ClientSet.AcidV1().Postgresqls(c.opConfig.WatchedNamespace).List(context.TODO(), options)
	if err != nil {
		c.logger.Errorf("could not list postgresql objects: %v", err)
	}
	if c.controllerID != "" {
		c.logger.Debugf("watch only clusters with controllerID %q", c.controllerID)
	}
	for _, pg := range list.Items {
		if pg.Error == "" && c.hasOwnership(&pg) {
			pgList.Items = append(pgList.Items, pg)
		}
	}

	return &pgList, err
}

// clusterListAndSync lists all manifests and decides whether to run the sync or repair.
func (c *Controller) clusterListAndSync() error {
	var (
		err   error
		event EventType
	)

	currentTime := time.Now().Unix()
	timeFromPreviousSync := currentTime - atomic.LoadInt64(&c.lastClusterSyncTime)
	timeFromPreviousRepair := currentTime - atomic.LoadInt64(&c.lastClusterRepairTime)

	if timeFromPreviousSync >= int64(c.opConfig.ResyncPeriod.Seconds()) {
		event = EventSync
	} else if timeFromPreviousRepair >= int64(c.opConfig.RepairPeriod.Seconds()) {
		event = EventRepair
	}
	if event != "" {
		var list *acidv1.PostgresqlList
		if list, err = c.listClusters(metav1.ListOptions{ResourceVersion: "0"}); err != nil {
			return err
		}
		c.queueEvents(list, event)
	} else {
		c.logger.Infof("not enough time passed since the last sync (%v seconds) or repair (%v seconds)",
			timeFromPreviousSync, timeFromPreviousRepair)
	}
	return nil
}

// queueEvents queues a sync or repair event for every cluster with a valid manifest
func (c *Controller) queueEvents(list *acidv1.PostgresqlList, event EventType) {
	var activeClustersCnt, failedClustersCnt, clustersToRepair int
	for i, pg := range list.Items {
		// XXX: check the cluster status field instead
		if pg.Error != "" {
			failedClustersCnt++
			continue
		}
		activeClustersCnt++
		// check if that cluster needs repair
		if event == EventRepair {
			if pg.Status.Success() {
				continue
			} else {
				clustersToRepair++
			}
		}
		c.queueClusterEvent(nil, &list.Items[i], event)
	}
	if len(list.Items) > 0 {
		if failedClustersCnt > 0 && activeClustersCnt == 0 {
			c.logger.Infof("there are no clusters running. %d are in the failed state", failedClustersCnt)
		} else if failedClustersCnt == 0 && activeClustersCnt > 0 {
			c.logger.Infof("there are %d clusters running", activeClustersCnt)
		} else {
			c.logger.Infof("there are %d clusters running and %d are in the failed state", activeClustersCnt, failedClustersCnt)
		}
		if clustersToRepair > 0 {
			c.logger.Infof("%d clusters are scheduled for a repair scan", clustersToRepair)
		}
	} else {
		c.logger.Infof("no clusters running")
	}
	if event == EventRepair || event == EventSync {
		atomic.StoreInt64(&c.lastClusterRepairTime, time.Now().Unix())
		if event == EventSync {
			atomic.StoreInt64(&c.lastClusterSyncTime, time.Now().Unix())
		}
	}
}

func (c *Controller) acquireInitialListOfClusters() error {
	var (
		list        *acidv1.PostgresqlList
		err         error
		clusterName spec.NamespacedName
	)

	if list, err = c.listClusters(metav1.ListOptions{ResourceVersion: "0"}); err != nil {
		return err
	}
	c.logger.Debugf("acquiring initial list of clusters")
	for _, pg := range list.Items {
		// XXX: check the cluster status field instead
		if pg.Error != "" {
			continue
		}
		clusterName = util.NameFromMeta(pg.ObjectMeta)
		c.addCluster(c.logger, clusterName, &pg)
		c.logger.Debugf("added new cluster: %q", clusterName)
	}
	// initiate initial sync of all clusters.
	c.queueEvents(list, EventSync)
	return nil
}

func (c *Controller) addCluster(lg *logrus.Entry, clusterName spec.NamespacedName, pgSpec *acidv1.Postgresql) *cluster.Cluster {
	cl := cluster.New(c.makeClusterConfig(), c.KubeClient, *pgSpec, lg, c.eventRecorder)
	cl.Run(c.stopCh)
	teamName := strings.ToLower(cl.Spec.TeamID)

	defer c.clustersMu.Unlock()
	c.clustersMu.Lock()

	c.teamClusters[teamName] = append(c.teamClusters[teamName], clusterName)
	c.clusters[clusterName] = cl
	c.clusterLogs[clusterName] = ringlog.New(c.opConfig.RingLogLines)
	c.clusterHistory[clusterName] = ringlog.New(c.opConfig.ClusterHistoryEntries)

	return cl
}

func (c *Controller) processEvent(event ClusterEvent) {
	var clusterName spec.NamespacedName
	var clHistory ringlog.RingLogger

	lg := c.logger.WithField("worker", event.WorkerID)

	if event.EventType == EventAdd || event.EventType == EventSync || event.EventType == EventRepair {
		clusterName = util.NameFromMeta(event.NewSpec.ObjectMeta)
	} else {
		clusterName = util.NameFromMeta(event.OldSpec.ObjectMeta)
	}
	lg = lg.WithField("cluster-name", clusterName)

	c.clustersMu.RLock()
	cl, clusterFound := c.clusters[clusterName]
	if clusterFound {
		clHistory = c.clusterHistory[clusterName]
	}
	c.clustersMu.RUnlock()

	defer c.curWorkerCluster.Store(event.WorkerID, nil)

	if event.EventType == EventRepair {
		runRepair, lastOperationStatus := cl.NeedsRepair()
		if !runRepair {
			lg.Debugf("Observed cluster status %s, repair is not required", lastOperationStatus)
			return
		}
		lg.Debugf("Observed cluster status %s, running sync scan to repair the cluster", lastOperationStatus)
		event.EventType = EventSync
	}

	if event.EventType == EventAdd || event.EventType == EventUpdate || event.EventType == EventSync {
		// handle deprecated parameters by possibly assigning their values to the new ones.
		if event.OldSpec != nil {
			c.mergeDeprecatedPostgreSQLSpecParameters(&event.OldSpec.Spec)
		}
		if event.NewSpec != nil {
			c.warnOnDeprecatedPostgreSQLSpecParameters(&event.NewSpec.Spec)
			c.mergeDeprecatedPostgreSQLSpecParameters(&event.NewSpec.Spec)
		}

		if err := c.submitRBACCredentials(event); err != nil {
			c.logger.Warnf("Pods and/or Patroni may misfunction due to the lack of permissions: %v", err)
		}

	}

	switch event.EventType {
	case EventAdd:
		if clusterFound {
			lg.Debugf("cluster already exists")
			return
		}

		lg.Infof("creation of the cluster started")

		cl = c.addCluster(lg, clusterName, event.NewSpec)

		c.curWorkerCluster.Store(event.WorkerID, cl)

		if err := cl.Create(); err != nil {
			cl.Error = fmt.Sprintf("could not create cluster: %v", err)
			lg.Error(cl.Error)
			c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Create", "%v", cl.Error)

			return
		}

		lg.Infoln("cluster has been created")
	case EventUpdate:
		lg.Infoln("update of the cluster started")

		if !clusterFound {
			lg.Warningln("cluster does not exist")
			return
		}
		c.curWorkerCluster.Store(event.WorkerID, cl)
		if err := cl.Update(event.OldSpec, event.NewSpec); err != nil {
			cl.Error = fmt.Sprintf("could not update cluster: %v", err)
			lg.Error(cl.Error)

			return
		}
		cl.Error = ""
		lg.Infoln("cluster has been updated")

		clHistory.Insert(&spec.Diff{
			EventTime:   event.EventTime,
			ProcessTime: time.Now(),
			Diff:        util.Diff(event.OldSpec, event.NewSpec),
		})
	case EventDelete:
		if !clusterFound {
			lg.Errorf("unknown cluster: %q", clusterName)
			return
		}
		lg.Infoln("deletion of the cluster started")

		teamName := strings.ToLower(cl.Spec.TeamID)

		c.curWorkerCluster.Store(event.WorkerID, cl)
		if err := cl.Delete(); err != nil {
			cl.Error = fmt.Sprintf("Could not delete cluster: %v", err)
			c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Delete", "%v", cl.Error)
		}

		func() {
			defer c.clustersMu.Unlock()
			c.clustersMu.Lock()

			delete(c.clusters, clusterName)
			delete(c.clusterLogs, clusterName)
			delete(c.clusterHistory, clusterName)
			for i, val := range c.teamClusters[teamName] {
				if val == clusterName {
					copy(c.teamClusters[teamName][i:], c.teamClusters[teamName][i+1:])
					c.teamClusters[teamName][len(c.teamClusters[teamName])-1] = spec.NamespacedName{}
					c.teamClusters[teamName] = c.teamClusters[teamName][:len(c.teamClusters[teamName])-1]
					break
				}
			}
		}()

		lg.Infof("cluster has been deleted")
	case EventSync:
		lg.Infof("syncing of the cluster started")

		// no race condition because a cluster is always processed by single worker
		if !clusterFound {
			cl = c.addCluster(lg, clusterName, event.NewSpec)
		}

		c.curWorkerCluster.Store(event.WorkerID, cl)

		// has this cluster been marked as deleted already, then we shall start cleaning up
		if !cl.ObjectMeta.DeletionTimestamp.IsZero() {
			lg.Infof("Cluster has a DeletionTimestamp of %s, starting deletion now.", cl.ObjectMeta.DeletionTimestamp.Format(time.RFC3339))
			if err := cl.Delete(); err != nil {
				cl.Error = fmt.Sprintf("Error deleting cluster and its resources: %v", err)
				c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Delete", "%v", cl.Error)
				lg.Error(cl.Error)
				return
			}
			lg.Infof("cluster has been deleted")
		} else {
			if err := cl.Sync(event.NewSpec); err != nil {
				cl.Error = fmt.Sprintf("could not sync cluster: %v", err)
				c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Sync", "%v", cl.Error)
				lg.Error(cl.Error)
				return
			}
			lg.Infof("cluster has been synced")
		}
		cl.Error = ""
	}
}

func (c *Controller) processClusterEventsQueue(idx int, stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	go func() {
		<-stopCh
		c.clusterEventQueues[idx].Close()
	}()

	for {
		obj, err := c.clusterEventQueues[idx].Pop(cache.PopProcessFunc(func(interface{}) error { return nil }))
		if err != nil {
			if err == cache.ErrFIFOClosed {
				return
			}
			c.logger.Errorf("error when processing cluster events queue: %v", err)
			continue
		}
		event, ok := obj.(ClusterEvent)
		if !ok {
			c.logger.Errorf("could not cast to ClusterEvent")
		}

		c.processEvent(event)
	}
}

func (c *Controller) warnOnDeprecatedPostgreSQLSpecParameters(spec *acidv1.PostgresSpec) {

	deprecate := func(deprecated, replacement string) {
		c.logger.Warningf("Parameter %q is deprecated. Consider setting %q instead", deprecated, replacement)
	}

	noeffect := func(param string, explanation string) {
		c.logger.Warningf("Parameter %q takes no effect. %s", param, explanation)
	}

	if spec.UseLoadBalancer != nil {
		deprecate("useLoadBalancer", "enableMasterLoadBalancer")
	}
	if spec.ReplicaLoadBalancer != nil {
		deprecate("replicaLoadBalancer", "enableReplicaLoadBalancer")
	}

	if len(spec.MaintenanceWindows) > 0 {
		noeffect("maintenanceWindows", "Not implemented.")
	}

	if (spec.UseLoadBalancer != nil || spec.ReplicaLoadBalancer != nil) &&
		(spec.EnableReplicaLoadBalancer != nil || spec.EnableMasterLoadBalancer != nil) {
		c.logger.Warnf("Both old and new load balancer parameters are present in the manifest, ignoring old ones")
	}
}

// mergeDeprecatedPostgreSQLSpecParameters modifies the spec passed to the cluster by setting current parameter
// values from the obsolete ones. Note: while the spec that is modified is a copy made in queueClusterEvent, it is
// still a shallow copy, so be extra careful not to modify values pointer fields point to, but copy them instead.
func (c *Controller) mergeDeprecatedPostgreSQLSpecParameters(spec *acidv1.PostgresSpec) *acidv1.PostgresSpec {
	if (spec.UseLoadBalancer != nil || spec.ReplicaLoadBalancer != nil) &&
		(spec.EnableReplicaLoadBalancer == nil && spec.EnableMasterLoadBalancer == nil) {
		if spec.UseLoadBalancer != nil {
			spec.EnableMasterLoadBalancer = new(bool)
			*spec.EnableMasterLoadBalancer = *spec.UseLoadBalancer
		}
		if spec.ReplicaLoadBalancer != nil {
			spec.EnableReplicaLoadBalancer = new(bool)
			*spec.EnableReplicaLoadBalancer = *spec.ReplicaLoadBalancer
		}
	}
	spec.ReplicaLoadBalancer = nil
	spec.UseLoadBalancer = nil

	return spec
}

func (c *Controller) queueClusterEvent(informerOldSpec, informerNewSpec *acidv1.Postgresql, eventType EventType) {
	var (
		uid          types.UID
		clusterName  spec.NamespacedName
		clusterError string
	)

	if informerOldSpec != nil { //update, delete
		uid = informerOldSpec.GetUID()
		clusterName = util.NameFromMeta(informerOldSpec.ObjectMeta)

		// user is fixing previously incorrect spec
		if eventType == EventUpdate && informerNewSpec.Error == "" && informerOldSpec.Error != "" {
			eventType = EventSync
		}

		// set current error to be one of the new spec if present
		if informerNewSpec != nil {
			clusterError = informerNewSpec.Error
		} else {
			clusterError = informerOldSpec.Error
		}
	} else { //add, sync
		uid = informerNewSpec.GetUID()
		clusterName = util.NameFromMeta(informerNewSpec.ObjectMeta)
		clusterError = informerNewSpec.Error
	}

	if clusterError != "" && eventType != EventDelete {
		c.logger.
			WithField("cluster-name", clusterName).
			Debugf("skipping %q event for the invalid cluster: %s", eventType, clusterError)
		return
	}

	// Don't pass the spec directly from the informer, since subsequent modifications of it would be reflected
	// in the informer internal state, making it incohherent with the actual Kubernetes object (and, as a side
	// effect, the modified state will be returned together with subsequent events).

	workerID := c.clusterWorkerID(clusterName)
	clusterEvent := ClusterEvent{
		EventTime: time.Now(),
		EventType: eventType,
		UID:       uid,
		OldSpec:   informerOldSpec.Clone(),
		NewSpec:   informerNewSpec.Clone(),
		WorkerID:  workerID,
	}

	lg := c.logger.WithField("worker", workerID).WithField("cluster-name", clusterName)
	if err := c.clusterEventQueues[workerID].Add(clusterEvent); err != nil {
		lg.Errorf("error while queueing cluster event: %v", clusterEvent)
	}
	lg.Infof("%q event has been queued", eventType)

	if eventType != EventDelete {
		return
	}
	// A delete event discards all prior requests for that cluster.
	for _, evType := range []EventType{EventAdd, EventSync, EventUpdate, EventRepair} {
		obj, exists, err := c.clusterEventQueues[workerID].GetByKey(queueClusterKey(evType, uid))
		if err != nil {
			lg.Warningf("could not get event from the queue: %v", err)
			continue
		}

		if !exists {
			continue
		}

		err = c.clusterEventQueues[workerID].Delete(obj)
		if err != nil {
			lg.Warningf("could not delete event from the queue: %v", err)
		} else {
			lg.Debugf("event %q has been discarded for the cluster", evType)
		}
	}
}

func (c *Controller) postgresqlAdd(obj interface{}) {
	pg := c.postgresqlCheck(obj)
	if pg != nil {
		// We will not get multiple Add events for the same cluster
		c.queueClusterEvent(nil, pg, EventAdd)
	}

	return
}

func (c *Controller) postgresqlUpdate(prev, cur interface{}) {
	pgOld := c.postgresqlCheck(prev)
	pgNew := c.postgresqlCheck(cur)

	// has this cluster been marked as deleted already, then we shall start cleaning up
	if pgNew != nil && !pgNew.ObjectMeta.DeletionTimestamp.IsZero() {
		c.queueClusterEvent(pgNew, nil, EventDelete)
	} else if pgOld != nil && pgNew != nil {
		// Avoid the inifinite recursion for status updates
		if reflect.DeepEqual(pgOld.Spec, pgNew.Spec) {
			if reflect.DeepEqual(pgNew.Annotations, pgOld.Annotations) {
				return
			}
		}
		c.queueClusterEvent(pgOld, pgNew, EventUpdate)
	}

	return
}

func (c *Controller) postgresqlDelete(obj interface{}) {
	pg := c.postgresqlCheck(obj)
	if pg != nil {
		c.queueClusterEvent(pg, nil, EventDelete)
	}

	return
}

func (c *Controller) postgresqlCheck(obj interface{}) *acidv1.Postgresql {
	pg, ok := obj.(*acidv1.Postgresql)
	if !ok {
		c.logger.Errorf("could not cast to postgresql spec")
		return nil
	}
	if !c.hasOwnership(pg) {
		return nil
	}
	return pg
}

/*
  Ensures the pod service account and role bindings exists in a namespace
  before a PG cluster is created there so that a user does not have to deploy
  these credentials manually.  StatefulSets require the service account to
  create pods; Patroni requires relevant RBAC bindings to access endpoints.

  The operator does not sync accounts/role bindings after creation.
*/
func (c *Controller) submitRBACCredentials(event ClusterEvent) error {

	namespace := event.NewSpec.GetNamespace()

	if err := c.createPodServiceAccount(namespace); err != nil {
		return fmt.Errorf("could not create pod service account %q : %v", c.opConfig.PodServiceAccountName, err)
	}

	if err := c.createRoleBindings(namespace); err != nil {
		return fmt.Errorf("could not create role binding %q : %v", c.PodServiceAccountRoleBinding.Name, err)
	}
	return nil
}

func (c *Controller) createPodServiceAccount(namespace string) error {

	podServiceAccountName := c.opConfig.PodServiceAccountName
	_, err := c.KubeClient.ServiceAccounts(namespace).Get(context.TODO(), podServiceAccountName, metav1.GetOptions{})
	if k8sutil.ResourceNotFound(err) {

		c.logger.Infof(fmt.Sprintf("creating pod service account %q in the %q namespace", podServiceAccountName, namespace))

		// get a separate copy of service account
		// to prevent a race condition when setting a namespace for many clusters
		sa := *c.PodServiceAccount
		if _, err = c.KubeClient.ServiceAccounts(namespace).Create(context.TODO(), &sa, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("cannot deploy the pod service account %q defined in the configuration to the %q namespace: %v", podServiceAccountName, namespace, err)
		}

		c.logger.Infof("successfully deployed the pod service account %q to the %q namespace", podServiceAccountName, namespace)
	} else if k8sutil.ResourceAlreadyExists(err) {
		return nil
	}

	return err
}

func (c *Controller) createRoleBindings(namespace string) error {

	podServiceAccountName := c.opConfig.PodServiceAccountName
	podServiceAccountRoleBindingName := c.PodServiceAccountRoleBinding.Name

	_, err := c.KubeClient.RoleBindings(namespace).Get(context.TODO(), podServiceAccountRoleBindingName, metav1.GetOptions{})
	if k8sutil.ResourceNotFound(err) {

		c.logger.Infof("Creating the role binding %q in the %q namespace", podServiceAccountRoleBindingName, namespace)

		// get a separate copy of role binding
		// to prevent a race condition when setting a namespace for many clusters
		rb := *c.PodServiceAccountRoleBinding
		_, err = c.KubeClient.RoleBindings(namespace).Create(context.TODO(), &rb, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("cannot bind the pod service account %q defined in the configuration to the cluster role in the %q namespace: %v", podServiceAccountName, namespace, err)
		}

		c.logger.Infof("successfully deployed the role binding for the pod service account %q to the %q namespace", podServiceAccountName, namespace)

	} else if k8sutil.ResourceAlreadyExists(err) {
		return nil
	}

	return err
}
