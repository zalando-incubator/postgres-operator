package controller

import (
	"fmt"
	"sync/atomic"

	"github.com/Sirupsen/logrus"

	"github.com/zalando-incubator/postgres-operator/pkg/spec"
	"github.com/zalando-incubator/postgres-operator/pkg/util/config"
)

// ClusterStatus provides status of the cluster
func (c *Controller) ClusterStatus(team, cluster string) (*spec.ClusterStatus, error) {
	clusterName := spec.NamespacedName{
		Namespace: c.opConfig.Namespace,
		Name:      team + "-" + cluster,
	}

	c.clustersMu.RLock()
	cl, ok := c.clusters[clusterName]
	c.clustersMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("could not find cluster")
	}

	status := cl.GetStatus()
	status.Worker = c.clusterWorkerID(clusterName)

	return status, nil
}

// TeamClustersStatus dumps logs of all the team clusters
func (c *Controller) TeamClustersStatus(team string) ([]*spec.ClusterStatus, error) {
	c.clustersMu.RLock()

	clusterNames, ok := c.teamClusters[team]
	if !ok {
		c.clustersMu.RUnlock()
		return nil, fmt.Errorf("could not find clusters for the team")
	}

	var resp = make([]*spec.ClusterStatus, len(clusterNames))
	for i, clName := range clusterNames {
		cl := c.clusters[clName]

		resp[i] = cl.GetStatus()
		resp[i].Worker = c.clusterWorkerID(clName)

	}
	c.clustersMu.RUnlock()

	return resp, nil
}

// GetConfig returns controller config
func (c *Controller) GetConfig() *spec.ControllerConfig {
	return &c.config
}

// GetOperatorConfig returns operator config
func (c *Controller) GetOperatorConfig() *config.Config {
	return c.opConfig
}

// GetStatus dumps current config and status of the controller
func (c *Controller) GetStatus() *spec.ControllerStatus {
	c.clustersMu.RLock()
	clustersCnt := len(c.clusters)
	c.clustersMu.RUnlock()

	return &spec.ControllerStatus{
		LastSyncTime: atomic.LoadInt64(&c.lastClusterSyncTime),
		Clusters:     clustersCnt,
	}
}

// ClusterLogs dumps cluster ring logs
func (c *Controller) ClusterLogs(team, name string) ([]*spec.LogEntry, error) {
	clusterName := spec.NamespacedName{
		Namespace: c.opConfig.Namespace,
		Name:      team + "-" + name,
	}

	c.clustersMu.RLock()
	cl, ok := c.clusterLogs[clusterName]
	c.clustersMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("could not find cluster")
	}

	res := make([]*spec.LogEntry, 0)
	for _, e := range cl.Walk() {
		logEntry := e.(*spec.LogEntry)
		logEntry.ClusterName = nil

		res = append(res, logEntry)
	}

	return res, nil
}

// WorkerLogs dumps logs of the worker
func (c *Controller) WorkerLogs(workerID uint32) ([]*spec.LogEntry, error) {
	lg, ok := c.workerLogs[workerID]
	if !ok {
		return nil, fmt.Errorf("could not find worker")
	}

	res := make([]*spec.LogEntry, 0)
	for _, e := range lg.Walk() {
		logEntry := e.(*spec.LogEntry)
		logEntry.Worker = nil

		res = append(res, logEntry)
	}

	return res, nil
}

// Levels returns logrus levels for which hook must fire
func (c *Controller) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire is a logrus hook
func (c *Controller) Fire(e *logrus.Entry) error {
	var clusterName spec.NamespacedName

	v, ok := e.Data["cluster-name"]
	if !ok {
		return nil
	}
	clusterName = v.(spec.NamespacedName)
	c.clustersMu.RLock()
	clusterRingLog, ok := c.clusterLogs[clusterName]
	c.clustersMu.RUnlock()
	if !ok {
		return nil
	}

	logEntry := &spec.LogEntry{
		Time:        e.Time,
		Level:       e.Level,
		ClusterName: &clusterName,
		Message:     e.Message,
	}

	if v, hasWorker := e.Data["worker"]; hasWorker {
		id := v.(uint32)

		logEntry.Worker = &id
	}
	clusterRingLog.Insert(logEntry)

	if logEntry.Worker == nil {
		return nil
	}
	c.workerLogs[*logEntry.Worker].Insert(logEntry) // workerLogs map is immutable. No need to lock it

	return nil
}

// ListQueue dumps cluster event queue of the provided worker
func (c *Controller) ListQueue(workerID uint32) (*spec.QueueDump, error) {
	if workerID >= uint32(len(c.clusterEventQueues)) {
		return nil, fmt.Errorf("could not find worker")
	}

	q := c.clusterEventQueues[workerID]
	return &spec.QueueDump{
		Keys: q.ListKeys(),
		List: q.List(),
	}, nil
}

// GetWorkersCnt returns number of the workers
func (c *Controller) GetWorkersCnt() uint32 {
	return c.opConfig.Workers
}
