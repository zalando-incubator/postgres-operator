package controller

import (
	"context"
	"encoding/json"
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

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/cluster"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	"github.com/zalando/postgres-operator/v2/pkg/util/k8sutil"
	"github.com/zalando/postgres-operator/v2/pkg/util/ringlog"
)

func (c *Controller) clusterResync(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	period := min(c.opConfig.ResyncPeriod.Duration, c.opConfig.RepairPeriod.Duration)
	ticker := time.NewTicker(period)

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
	list, err := c.KubeClient.PostgresqlsGetter.Postgresqls(c.opConfig.WatchedNamespace).List(context.TODO(), options)
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
	c.logger.Debug("acquiring initial list of clusters")
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

func (c *Controller) addCluster(lg *logrus.Entry, clusterName spec.NamespacedName, pgSpec *acidv1.Postgresql) (*cluster.Cluster, error) {
	if c.opConfig.EnableTeamIdClusternamePrefix {
		if _, err := acidv1.ExtractClusterName(clusterName.Name, pgSpec.Spec.TeamID); err != nil {
			pgSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusInvalid
			c.KubeClient.SetPostgresCRDStatus(clusterName, pgSpec)
			return nil, err
		}
	}

	cl := cluster.New(c.makeClusterConfig(), c.KubeClient, *pgSpec, lg, c.eventRecorder)
	cl.Run(c.stopCh)
	teamName := strings.ToLower(cl.Spec.TeamID)

	defer c.clustersMu.Unlock()
	c.clustersMu.Lock()

	c.teamClusters[teamName] = append(c.teamClusters[teamName], clusterName)
	c.clusters[clusterName] = cl
	c.clusterLogs[clusterName] = ringlog.New(c.opConfig.RingLogLines)
	c.clusterHistory[clusterName] = ringlog.New(c.opConfig.ClusterHistoryEntries)

	return cl, nil
}

func (c *Controller) processEvent(event ClusterEvent, isInInitialList bool) {
	var clusterName spec.NamespacedName
	var clHistory ringlog.RingLogger
	var err error

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
			lg.Debugf("observed cluster status %s, repair is not required", lastOperationStatus)
			return
		}
		lg.Debugf("observed cluster status %s, running sync scan to repair the cluster", lastOperationStatus)
		event.EventType = EventSync
	} else if event.EventType != EventDelete {
		if err = c.submitRBACCredentials(event); err != nil {
			c.logger.Warnf("pods and/or Patroni may misfunction due to the lack of permissions: %v", err)
		}
	}

	switch event.EventType {
	case EventAdd:
		if clusterFound {
			lg.Infof("received add event for already existing Postgres cluster")
			return
		}

		lg.Infof("creating a new Postgres cluster")

		cl, err = c.addCluster(lg, clusterName, event.NewSpec)
		if err != nil {
			lg.Errorf("creation of cluster is blocked: %v", err)
			return
		}

		c.curWorkerCluster.Store(event.WorkerID, cl)

		err = cl.Create()
		if err != nil {
			cl.Status = acidv1.PostgresStatus{PostgresClusterStatus: acidv1.ClusterStatusInvalid}
			cl.Error = fmt.Sprintf("could not create cluster: %v", err)
			lg.Error(cl.Error)
			c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Create", "%v", cl.Error)
			return
		}

		lg.Infoln("cluster has been created")
	case EventUpdate:
		if !clusterFound {
			lg.Warningln("cluster does not exist")
			return
		}
		c.curWorkerCluster.Store(event.WorkerID, cl)

		// Check if this cluster has been marked for deletion
		if !event.NewSpec.ObjectMeta.DeletionTimestamp.IsZero() {
			lg.Infof("cluster has a DeletionTimestamp of %s, starting deletion now.", event.NewSpec.ObjectMeta.DeletionTimestamp.Format(time.RFC3339))
			if err = cl.Delete(); err != nil {
				cl.Error = fmt.Sprintf("error deleting cluster and its resources: %v", err)
				c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Delete", "%v", cl.Error)
				lg.Error(cl.Error)
				return
			}
			lg.Infoln("cluster has been deleted via update event")
			return
		}

		lg.Infoln("update of the cluster started")
		err = cl.Update(event.OldSpec, event.NewSpec)
		if err != nil {
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

		teamName := strings.ToLower(cl.Spec.TeamID)
		c.curWorkerCluster.Store(event.WorkerID, cl)

		// when using finalizers the deletion already happened
		if c.opConfig.EnableFinalizers == nil || !*c.opConfig.EnableFinalizers {
			lg.Infoln("deletion of the cluster started")
			if err := cl.Delete(); err != nil {
				cl.Error = fmt.Sprintf("could not delete cluster: %v", err)
				c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Delete", "%v", cl.Error)
			}
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
			cl, err = c.addCluster(lg, clusterName, event.NewSpec)
			if err != nil {
				lg.Errorf("syncing of cluster is blocked: %v", err)
				return
			}
		}

		c.curWorkerCluster.Store(event.WorkerID, cl)

		// has this cluster been marked as deleted already, then we shall start cleaning up
		if !cl.ObjectMeta.DeletionTimestamp.IsZero() {
			lg.Infof("cluster has a DeletionTimestamp of %s, starting deletion now.", cl.ObjectMeta.DeletionTimestamp.Format(time.RFC3339))
			if err = cl.Delete(); err != nil {
				cl.Error = fmt.Sprintf("error deleting cluster and its resources: %v", err)
				c.eventRecorder.Eventf(cl.GetReference(), v1.EventTypeWarning, "Delete", "%v", cl.Error)
				lg.Error(cl.Error)
				return
			}
		} else {
			if err = cl.Sync(event.NewSpec); err != nil {
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
		(*c.clusterEventQueues[idx]).Close()
	}()

	for {
		_, err := (*c.clusterEventQueues[idx]).Pop(cache.PopProcessFunc(func(obj interface{}, isInitialList bool) error {
			event, ok := obj.(ClusterEvent)
			if !ok {
				c.logger.Errorf("could not cast to cluster event")
				return nil // skip event to keep processing
			}
			c.processEvent(event, isInitialList)
			if err := c.clusterEventStores[idx].Delete(obj); err != nil {
				c.logger.Errorf("failed to delete key from lookup store: %v", err)
			}
			return nil
		}))
		if err != nil {
			if err == cache.ErrFIFOClosed {
				return
			}
			c.logger.Errorf("error when processing cluster events queue: %v", err)
			continue
		}
	}
}

func (c *Controller) queueClusterEvent(informerOldSpec, informerNewSpec *acidv1.Postgresql, eventType EventType) {
	var (
		uid          types.UID
		clusterName  spec.NamespacedName
		clusterError string
	)

	if informerOldSpec != nil { // update, delete
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
	} else { // add, sync
		uid = informerNewSpec.GetUID()
		clusterName = util.NameFromMeta(informerNewSpec.ObjectMeta)
		clusterError = informerNewSpec.Error
	}

	if eventType == EventDelete {
		// when owner references are used operator cannot block deletion
		if c.opConfig.EnableOwnerReferences == nil || !*c.opConfig.EnableOwnerReferences {
			// only allow deletion if delete annotations are set and conditions are met
			if err := c.meetsClusterDeleteAnnotations(informerOldSpec); err != nil {
				c.logger.WithField("cluster-name", clusterName).Warnf(
					"ignoring %q event for cluster %q - manifest does not fulfill delete requirements: %s", eventType, clusterName, err)
				c.logger.WithField("cluster-name", clusterName).Warnf(
					"please, recreate Postgresql resource %q and set annotations to delete properly", clusterName)
				if currentManifest, marshalErr := json.Marshal(informerOldSpec); marshalErr != nil {
					c.logger.WithField("cluster-name", clusterName).Warnf("could not marshal current manifest:\n%+v", informerOldSpec)
				} else {
					c.logger.WithField("cluster-name", clusterName).Warnf("%s\n", string(currentManifest))
				}
				return
			}
		}
	}

	if clusterError != "" && eventType != EventDelete {
		c.logger.WithField("cluster-name", clusterName).Debugf("skipping %q event for the invalid cluster: %s", eventType, clusterError)

		switch eventType {
		case EventAdd:
			informerNewSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusAddFailed
			_, err := c.KubeClient.SetPostgresCRDStatus(clusterName, informerNewSpec)
			if err != nil {
				c.logger.WithField("cluster-name", clusterName).Errorf("could not set PostgresCRD status: %v", err)
			}
			c.eventRecorder.Eventf(c.GetReference(informerNewSpec), v1.EventTypeWarning, "Create", "%v", clusterError)
		case EventUpdate:
			informerNewSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusUpdateFailed
			_, err := c.KubeClient.SetPostgresCRDStatus(clusterName, informerNewSpec)
			if err != nil {
				c.logger.WithField("cluster-name", clusterName).Errorf("could not set PostgresCRD status: %v", err)
			}
			c.eventRecorder.Eventf(c.GetReference(informerNewSpec), v1.EventTypeWarning, "Update", "%v", clusterError)
		default:
			informerNewSpec.Status.PostgresClusterStatus = acidv1.ClusterStatusSyncFailed
			_, err := c.KubeClient.SetPostgresCRDStatus(clusterName, informerNewSpec)
			if err != nil {
				c.logger.WithField("cluster-name", clusterName).Errorf("could not set PostgresCRD status: %v", err)
			}
			c.eventRecorder.Eventf(c.GetReference(informerNewSpec), v1.EventTypeWarning, "Sync", "%v", clusterError)
		}

		return
	}

	// Don't pass the spec directly from the informer, since subsequent modifications of it would be reflected
	// in the informer internal state, making it incoherent with the actual Kubernetes object (and, as a side
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
	if err := c.clusterEventStores[workerID].Add(clusterEvent); err != nil {
		lg.Errorf("error while storing cluster event for lookup: %v", clusterEvent)
	}
	if err := (*c.clusterEventQueues[workerID]).Add(clusterEvent); err != nil {
		lg.Errorf("error while queueing cluster event: %v", clusterEvent)
	}
	lg.Infof("%s event has been queued", eventType)

	if eventType != EventDelete {
		return
	}
	// A delete event discards all prior requests for that cluster.
	for _, evType := range []EventType{EventAdd, EventSync, EventUpdate, EventRepair} {
		obj, exists, err := c.clusterEventStores[workerID].GetByKey(queueClusterKey(evType, uid))
		if err != nil {
			lg.Warningf("could not get event from the lookup store: %v", err)
			continue
		}

		if !exists {
			continue
		}

		err = (*c.clusterEventQueues[workerID]).Delete(obj)
		if err != nil {
			lg.Warningf("could not delete event from the queue: %v", err)
		} else {
			lg.Debugf("event %s has been discarded for the cluster", evType)
		}
		err = c.clusterEventStores[workerID].Delete(obj)
		if err != nil {
			lg.Warningf("could not delete event from the lookup store: %v", err)
		} else {
			lg.Debugf("event %s has been deleted from the lookup store", evType)
		}
	}
}

func (c *Controller) postgresqlAdd(obj interface{}) {
	pg := c.postgresqlCheck(obj)
	if pg != nil {
		// We will not get multiple Add events for the same cluster
		c.queueClusterEvent(nil, pg, EventAdd)
	}
}

func (c *Controller) postgresqlUpdate(prev, cur interface{}) {
	pgOld := c.postgresqlCheck(prev)
	pgNew := c.postgresqlCheck(cur)
	if pgOld != nil && pgNew != nil {
		clusterName := util.NameFromMeta(pgNew.ObjectMeta)

		// Check if DeletionTimestamp was set (resource marked for deletion)
		deletionTimestampChanged := pgOld.ObjectMeta.DeletionTimestamp.IsZero() && !pgNew.ObjectMeta.DeletionTimestamp.IsZero()
		if deletionTimestampChanged {
			c.logger.WithField("cluster-name", clusterName).Infof(
				"UPDATE event: DeletionTimestamp set to %s, queueing event",
				pgNew.ObjectMeta.DeletionTimestamp.Format(time.RFC3339))
			c.queueClusterEvent(pgOld, pgNew, EventUpdate)
			return
		}

		// Avoid the infinite recursion for status updates
		if reflect.DeepEqual(pgOld.Spec, pgNew.Spec) {
			if reflect.DeepEqual(pgNew.Annotations, pgOld.Annotations) {
				return
			}
		}
		c.queueClusterEvent(pgOld, pgNew, EventUpdate)
	}
}

func (c *Controller) postgresqlDelete(obj interface{}) {
	pg := c.postgresqlCheck(obj)
	if pg != nil {
		c.queueClusterEvent(pg, nil, EventDelete)
	}
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
create pods; Patroni requires relevant RBAC bindings to access endpoints
or config maps.

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

		c.logger.Infof("creating pod service account %q in the %q namespace", podServiceAccountName, namespace)

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
