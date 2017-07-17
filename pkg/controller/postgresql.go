package controller

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/pkg/api"
	"k8s.io/client-go/pkg/api/meta"
	"k8s.io/client-go/pkg/fields"
	"k8s.io/client-go/pkg/runtime"
	"k8s.io/client-go/pkg/types"
	"k8s.io/client-go/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/zalando-incubator/postgres-operator/pkg/cluster"
	"github.com/zalando-incubator/postgres-operator/pkg/spec"
	"github.com/zalando-incubator/postgres-operator/pkg/util"
	"github.com/zalando-incubator/postgres-operator/pkg/util/constants"
)

func (c *Controller) clusterResync(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(c.opConfig.ResyncPeriod)

	for {
		select {
		case <-ticker.C:
			c.clusterListFunc(api.ListOptions{ResourceVersion: "0"})
		case <-stopCh:
			return
		}
	}
}

func (c *Controller) clusterListFunc(options api.ListOptions) (runtime.Object, error) {
	c.logger.Info("Getting list of currently running clusters")

	req := c.RestClient.Get().
		RequestURI(fmt.Sprintf(constants.ListClustersURITemplate, c.opConfig.Namespace)).
		VersionedParams(&options, api.ParameterCodec).
		FieldsSelectorParam(fields.Everything())

	object, err := req.Do().Get()

	if err != nil {
		return nil, fmt.Errorf("could not get list of postgresql objects: %v", err)
	}

	objList, err := meta.ExtractList(object)
	if err != nil {
		return nil, fmt.Errorf("could not extract list of postgresql objects: %v", err)
	}

	if time.Now().Unix()-atomic.LoadInt64(&c.lastClusterSyncTime) <= int64(c.opConfig.ResyncPeriod.Seconds()) {
		c.logger.Debugln("skipping resync of clusters")
		return object, err
	}

	var activeClustersCnt, failedClustersCnt int
	for _, obj := range objList {
		pg, ok := obj.(*spec.Postgresql)
		if !ok {
			return nil, fmt.Errorf("could not cast object to postgresql")
		}

		if pg.Error != nil {
			failedClustersCnt++
			continue
		}
		c.queueClusterEvent(nil, pg, spec.EventSync)
		activeClustersCnt++
	}
	if len(objList) > 0 {
		if failedClustersCnt > 0 && activeClustersCnt == 0 {
			c.logger.Infof("There are no clusters running. %d are in the failed state", failedClustersCnt)
		} else if failedClustersCnt == 0 && activeClustersCnt > 0 {
			c.logger.Infof("There are %d clusters running", activeClustersCnt)
		} else {
			c.logger.Infof("There are %d clusters running and %d are in the failed state", activeClustersCnt, failedClustersCnt)
		}
	} else {
		c.logger.Infof("No clusters running")
	}

	atomic.StoreInt64(&c.lastClusterSyncTime, time.Now().Unix())

	return object, err
}

func (c *Controller) clusterWatchFunc(options api.ListOptions) (watch.Interface, error) {
	req := c.RestClient.Get().
		RequestURI(fmt.Sprintf(constants.WatchClustersURITemplate, c.opConfig.Namespace)).
		VersionedParams(&options, api.ParameterCodec).
		FieldsSelectorParam(fields.Everything())
	return req.Watch()
}

func (c *Controller) processEvent(obj interface{}) error {
	var clusterName spec.NamespacedName

	event, ok := obj.(spec.ClusterEvent)
	if !ok {
		return fmt.Errorf("could not cast to ClusterEvent")
	}
	logger := c.logger.WithField("worker", event.WorkerID)

	if event.EventType == spec.EventAdd || event.EventType == spec.EventSync {
		clusterName = util.NameFromMeta(event.NewSpec.Metadata)
	} else {
		clusterName = util.NameFromMeta(event.OldSpec.Metadata)
	}

	c.clustersMu.RLock()
	cl, clusterFound := c.clusters[clusterName]
	c.clustersMu.RUnlock()

	switch event.EventType {
	case spec.EventAdd:
		if clusterFound {
			logger.Debugf("Cluster '%s' already exists", clusterName)
			return nil
		}

		logger.Infof("Creation of the '%s' cluster started", clusterName)

		stopCh := make(chan struct{})
		cl = cluster.New(c.makeClusterConfig(), *event.NewSpec, logger)
		cl.Run(stopCh)

		c.clustersMu.Lock()
		c.clusters[clusterName] = cl
		c.stopChs[clusterName] = stopCh
		c.clustersMu.Unlock()

		if err := cl.Create(); err != nil {
			cl.Error = fmt.Errorf("could not create cluster: %v", err)
			logger.Errorf("%v", cl.Error)

			return nil
		}

		logger.Infof("Cluster '%s' has been created", clusterName)
	case spec.EventUpdate:
		logger.Infof("Update of the '%s' cluster started", clusterName)

		if !clusterFound {
			logger.Warnf("Cluster '%s' does not exist", clusterName)
			return nil
		}
		if err := cl.Update(event.NewSpec); err != nil {
			cl.Error = fmt.Errorf("could not update cluster: %s", err)
			logger.Errorf("%v", cl.Error)

			return nil
		}
		cl.Error = nil
		logger.Infof("Cluster '%s' has been updated", clusterName)
	case spec.EventDelete:
		logger.Infof("Deletion of the '%s' cluster started", clusterName)
		if !clusterFound {
			logger.Errorf("Unknown cluster: %s", clusterName)
			return nil
		}

		if err := cl.Delete(); err != nil {
			logger.Errorf("could not delete cluster '%s': %s", clusterName, err)
			return nil
		}
		close(c.stopChs[clusterName])

		c.clustersMu.Lock()
		delete(c.clusters, clusterName)
		delete(c.stopChs, clusterName)
		c.clustersMu.Unlock()

		logger.Infof("Cluster '%s' has been deleted", clusterName)
	case spec.EventSync:
		logger.Infof("Syncing of the '%s' cluster started", clusterName)

		// no race condition because a cluster is always processed by single worker
		if !clusterFound {
			stopCh := make(chan struct{})
			cl = cluster.New(c.makeClusterConfig(), *event.NewSpec, logger)
			cl.Run(stopCh)

			c.clustersMu.Lock()
			c.clusters[clusterName] = cl
			c.stopChs[clusterName] = stopCh
			c.clustersMu.Unlock()
		}

		if err := cl.Sync(); err != nil {
			cl.Error = fmt.Errorf("could not sync cluster '%s': %v", clusterName, err)
			logger.Errorf("%v", cl.Error)
			return nil
		}
		cl.Error = nil

		logger.Infof("Cluster '%s' has been synced", clusterName)
	}

	return nil
}

func (c *Controller) processClusterEventsQueue(stopCh <-chan struct{}, idx int, wg *sync.WaitGroup) {
	defer wg.Done()

	go func() {
		if _, err := c.clusterEventQueues[idx].Pop(cache.PopProcessFunc(c.processEvent)); err != nil {
			c.logger.Errorf("error when processing cluster events queue: %v", err)
		}
	}()

	<-stopCh
}

func (c *Controller) queueClusterEvent(old, new *spec.Postgresql, eventType spec.EventType) {
	var (
		uid          types.UID
		clusterName  spec.NamespacedName
		clusterError error
	)

	if old != nil { //update, delete
		uid = old.Metadata.GetUID()
		clusterName = util.NameFromMeta(old.Metadata)
		if eventType == spec.EventUpdate && new.Error == nil && old.Error != nil {
			eventType = spec.EventSync
			clusterError = new.Error
		} else {
			clusterError = old.Error
		}
	} else { //add, sync
		uid = new.Metadata.GetUID()
		clusterName = util.NameFromMeta(new.Metadata)
		clusterError = new.Error
	}

	if clusterError != nil && eventType != spec.EventDelete {
		c.logger.Debugf("Skipping %s event for invalid cluster %s (reason: %v)", eventType, clusterName, clusterError)
		return
	}

	workerID := c.clusterWorkerID(clusterName)
	clusterEvent := spec.ClusterEvent{
		EventType: eventType,
		UID:       uid,
		OldSpec:   old,
		NewSpec:   new,
		WorkerID:  workerID,
	}
	//TODO: if we delete cluster, discard all the previous events for the cluster

	if err := c.clusterEventQueues[workerID].Add(clusterEvent); err != nil {
		c.logger.WithField("worker", workerID).Errorf("error when queueing cluster event: %v", clusterEvent)
	}
	c.logger.WithField("worker", workerID).Infof("%s of the '%s' cluster has been queued", eventType, clusterName)
}

func (c *Controller) postgresqlAdd(obj interface{}) {
	pg, ok := obj.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("could not cast to postgresql spec")
		return
	}

	// We will not get multiple Add events for the same cluster
	c.queueClusterEvent(nil, pg, spec.EventAdd)
}

func (c *Controller) postgresqlUpdate(prev, cur interface{}) {
	pgOld, ok := prev.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("could not cast to postgresql spec")
	}
	pgNew, ok := cur.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("could not cast to postgresql spec")
	}
	if pgOld.Metadata.ResourceVersion == pgNew.Metadata.ResourceVersion {
		return
	}
	if reflect.DeepEqual(pgOld.Spec, pgNew.Spec) {
		return
	}

	c.queueClusterEvent(pgOld, pgNew, spec.EventUpdate)
}

func (c *Controller) postgresqlDelete(obj interface{}) {
	pg, ok := obj.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("could not cast to postgresql spec")
		return
	}

	c.queueClusterEvent(pg, nil, spec.EventDelete)
}
