package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/zalando-incubator/postgres-operator/pkg/cluster"
	"github.com/zalando-incubator/postgres-operator/pkg/spec"
	"github.com/zalando-incubator/postgres-operator/pkg/util"
	"github.com/zalando-incubator/postgres-operator/pkg/util/constants"
	"github.com/zalando-incubator/postgres-operator/pkg/util/ringlog"
)

func (c *Controller) clusterResync(stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(c.opConfig.ResyncPeriod)

	for {
		select {
		case <-ticker.C:
			_, err := c.clusterListFunc(metav1.ListOptions{ResourceVersion: "0"})
			if err != nil {
				c.logger.Errorf("Could not list clusters: %v", err)
			}
		case <-stopCh:
			return
		}
	}
}

func (c *Controller) clusterListFunc(options metav1.ListOptions) (runtime.Object, error) {
	var list spec.PostgresqlList
	var activeClustersCnt, failedClustersCnt int

	req := c.RestClient.
		Get().
		Namespace(c.opConfig.Namespace).
		Resource(constants.ResourceName).
		VersionedParams(&options, metav1.ParameterCodec)

	b, err := req.DoRaw()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &list)

	if time.Now().Unix()-atomic.LoadInt64(&c.lastClusterSyncTime) <= int64(c.opConfig.ResyncPeriod.Seconds()) {
		c.logger.Debugln("Skipping resync of clusters")
		return &list, err
	}

	for i, pg := range list.Items {
		if pg.Error != nil {
			failedClustersCnt++
			continue
		}
		c.queueClusterEvent(nil, &list.Items[i], spec.EventSync)
		activeClustersCnt++
	}
	if len(list.Items) > 0 {
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

	return &list, err
}

type tprDecoder struct {
	dec   *json.Decoder
	close func() error
}

func (d *tprDecoder) Close() {
	d.close()
}

func (d *tprDecoder) Decode() (action watch.EventType, object runtime.Object, err error) {
	var e struct {
		Type   watch.EventType
		Object spec.Postgresql
	}
	if err := d.dec.Decode(&e); err != nil {
		return watch.Error, nil, err
	}

	return e.Type, &e.Object, nil
}

func (c *Controller) clusterWatchFunc(options metav1.ListOptions) (watch.Interface, error) {
	options.Watch = true
	r, err := c.RestClient.
		Get().
		Namespace(c.opConfig.Namespace).
		Resource(constants.ResourceName).
		VersionedParams(&options, metav1.ParameterCodec).
		FieldsSelectorParam(nil).
		Stream()

	if err != nil {
		return nil, err
	}

	return watch.NewStreamWatcher(&tprDecoder{
		dec:   json.NewDecoder(r),
		close: r.Close,
	}), nil
}

func (c *Controller) processEvent(event spec.ClusterEvent) {
	var clusterName spec.NamespacedName

	lg := c.logger.WithField("worker", event.WorkerID)

	if event.EventType == spec.EventAdd || event.EventType == spec.EventSync {
		clusterName = util.NameFromMeta(event.NewSpec.ObjectMeta)
	} else {
		clusterName = util.NameFromMeta(event.OldSpec.ObjectMeta)
	}
	lg = lg.WithField("cluster-name", clusterName)

	c.clustersMu.RLock()
	cl, clusterFound := c.clusters[clusterName]
	c.clustersMu.RUnlock()

	switch event.EventType {
	case spec.EventAdd:
		if clusterFound {
			lg.Debugf("Cluster already exists")
			return
		}

		lg.Infof("Creation of the cluster started")

		stopCh := make(chan struct{})
		cl = cluster.New(c.makeClusterConfig(), c.KubeClient, *event.NewSpec, lg)
		cl.Run(stopCh)
		teamName := strings.ToLower(cl.Spec.TeamID)

		func() {
			defer c.clustersMu.Unlock()
			c.clustersMu.Lock()

			c.teamClusters[teamName] = append(c.teamClusters[teamName], clusterName)
			c.clusters[clusterName] = cl
			c.stopChs[clusterName] = stopCh
			c.clusterLogs[clusterName] = ringlog.New(c.opConfig.RingLogLines)
		}()

		if err := cl.Create(); err != nil {
			cl.Error = fmt.Errorf("could not create cluster: %v", err)
			lg.Errorf("%v", cl.Error)

			return
		}

		lg.Infoln("Cluster has been created")
	case spec.EventUpdate:
		lg.Infoln("Update of the cluster started")

		if !clusterFound {
			lg.Warnln("Cluster does not exist")
			return
		}
		if err := cl.Update(event.NewSpec); err != nil {
			cl.Error = fmt.Errorf("could not update cluster: %v", err)
			lg.Errorf("%v", cl.Error)

			return
		}
		cl.Error = nil
		lg.Infoln("Cluster has been updated")
	case spec.EventDelete:
		teamName := strings.ToLower(cl.Spec.TeamID)

		lg.Infoln("Deletion of the cluster started")
		if !clusterFound {
			lg.Errorln("Unknown cluster")
			return
		}

		if err := cl.Delete(); err != nil {
			lg.Errorf("could not delete cluster: %v", err)
			return
		}
		close(c.stopChs[clusterName])

		func() {
			defer c.clustersMu.Unlock()
			c.clustersMu.Lock()

			delete(c.clusters, clusterName)
			delete(c.stopChs, clusterName)
			delete(c.clusterLogs, clusterName)
			for i, val := range c.teamClusters[teamName] { // on relativel
				if val == clusterName {
					copy(c.teamClusters[teamName][i:], c.teamClusters[teamName][i+1:])
					c.teamClusters[teamName][len(c.teamClusters[teamName])-1] = spec.NamespacedName{}
					c.teamClusters[teamName] = c.teamClusters[teamName][:len(c.teamClusters[teamName])-1]
					break
				}
			}
		}()

		lg.Infoln("Cluster has been deleted")
	case spec.EventSync:
		lg.Infoln("Syncing of the cluster started")

		// no race condition because a cluster is always processed by single worker
		if !clusterFound {
			stopCh := make(chan struct{})
			cl = cluster.New(c.makeClusterConfig(), c.KubeClient, *event.NewSpec, lg)
			teamName := strings.ToLower(cl.Spec.TeamID)
			cl.Run(stopCh)

			func() {
				c.clustersMu.Lock()
				defer c.clustersMu.Unlock()

				c.clusters[clusterName] = cl
				c.stopChs[clusterName] = stopCh
				c.teamClusters[teamName] = append(c.teamClusters[teamName], clusterName)
				c.clusterLogs[clusterName] = ringlog.New(c.opConfig.RingLogLines)
			}()
		}

		if err := cl.Sync(); err != nil {
			cl.Error = fmt.Errorf("could not sync cluster: %v", err)
			lg.Error(cl.Error)
			return
		}
		cl.Error = nil

		lg.Infoln("Cluster has been synced")
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
			if err == cache.FIFOClosedError {
				return
			}
			c.logger.Errorf("Error when processing cluster events queue: %v", err)
			continue
		}
		event, ok := obj.(spec.ClusterEvent)
		if !ok {
			c.logger.Errorf("Could not cast to ClusterEvent")
		}

		c.processEvent(event)
	}
}

func (c *Controller) queueClusterEvent(old, new *spec.Postgresql, eventType spec.EventType) {
	var (
		uid          types.UID
		clusterName  spec.NamespacedName
		clusterError error
	)

	if old != nil { //update, delete
		uid = old.GetUID()
		clusterName = util.NameFromMeta(old.ObjectMeta)
		if eventType == spec.EventUpdate && new.Error == nil && old.Error != nil {
			eventType = spec.EventSync
			clusterError = new.Error
		} else {
			clusterError = old.Error
		}
	} else { //add, sync
		uid = new.GetUID()
		clusterName = util.NameFromMeta(new.ObjectMeta)
		clusterError = new.Error
	}

	if clusterError != nil && eventType != spec.EventDelete {
		c.logger.
			WithField("cluster-name", clusterName).
			Debugf("Skipping %q event for the invalid cluster: %v", eventType, clusterError)
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

	lg := c.logger.WithField("worker", workerID).WithField("cluster-name", clusterName)
	lg.Debugf("Adding %q event to the worker's queue", clusterEvent.EventType)
	if err := c.clusterEventQueues[workerID].Add(clusterEvent); err != nil {
		lg.Errorf("error when queueing cluster event: %v", clusterEvent)
	}
	lg.Infof("%q event has been queued", eventType)
}

func (c *Controller) postgresqlAdd(obj interface{}) {
	pg, ok := obj.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("Could not cast to postgresql spec")
		return
	}

	// We will not get multiple Add events for the same cluster
	c.queueClusterEvent(nil, pg, spec.EventAdd)
}

func (c *Controller) postgresqlUpdate(prev, cur interface{}) {
	pgOld, ok := prev.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("Could not cast to postgresql spec")
	}
	pgNew, ok := cur.(*spec.Postgresql)
	if !ok {
		c.logger.Errorf("Could not cast to postgresql spec")
	}
	if pgOld.ResourceVersion == pgNew.ResourceVersion {
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
		c.logger.Errorf("Could not cast to postgresql spec")
		return
	}

	c.queueClusterEvent(pg, nil, spec.EventDelete)
}
