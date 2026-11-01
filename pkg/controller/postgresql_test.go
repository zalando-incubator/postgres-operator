package controller

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
	"github.com/zalando/postgres-operator/v2/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

var (
	True  = true
	False = false
)

func newPostgresqlTestController() *Controller {
	controller := NewController(&spec.ControllerConfig{}, "postgresql-test")
	return controller
}

var postgresqlTestController = newPostgresqlTestController()

func TestControllerOwnershipOnPostgresql(t *testing.T) {
	tests := []struct {
		name  string
		pg    *acidv1.Postgresql
		owned bool
		error string
	}{
		{
			"Postgres cluster with defined ownership of mocked controller",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"acid.zalan.do/controller": "postgresql-test"},
				},
			},
			True,
			"Postgres cluster should be owned by operator, but controller says no",
		},
		{
			"Postgres cluster with defined ownership of another controller",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"acid.zalan.do/controller": "stups-test"},
				},
			},
			False,
			"Postgres cluster should be owned by another operator, but controller say yes",
		},
		{
			"Test Postgres cluster without defined ownership",
			&acidv1.Postgresql{},
			False,
			"Postgres cluster should be owned by operator with empty controller ID, but controller says yes",
		},
	}
	for _, tt := range tests {
		if postgresqlTestController.hasOwnership(tt.pg) != tt.owned {
			t.Errorf("%s: %v", tt.name, tt.error)
		}
	}
}

func TestMeetsClusterDeleteAnnotations(t *testing.T) {
	// set delete annotations in configuration
	postgresqlTestController.opConfig.DeleteAnnotationDateKey = "delete-date"
	postgresqlTestController.opConfig.DeleteAnnotationNameKey = "delete-clustername"

	currentTime := time.Now()
	today := currentTime.Format("2006-01-02") // go's reference date
	clusterName := "acid-test-cluster"

	tests := []struct {
		name  string
		pg    *acidv1.Postgresql
		error string
	}{
		{
			"Postgres cluster with matching delete annotations",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date":        today,
						"delete-clustername": clusterName,
					},
				},
			},
			"",
		},
		{
			"Postgres cluster with violated delete date annotation",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date":        "2020-02-02",
						"delete-clustername": clusterName,
					},
				},
			},
			fmt.Sprintf("annotation delete-date not matching the current date: got 2020-02-02, expected %s", today),
		},
		{
			"Postgres cluster with violated delete cluster name annotation",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date":        today,
						"delete-clustername": "acid-minimal-cluster",
					},
				},
			},
			fmt.Sprintf("annotation delete-clustername not matching the cluster name: got acid-minimal-cluster, expected %s", clusterName),
		},
		{
			"Postgres cluster with missing delete annotations",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name:        clusterName,
					Annotations: map[string]string{},
				},
			},
			"annotation delete-date not set in manifest to allow cluster deletion",
		},
		{
			"Postgres cluster with missing delete cluster name annotation",
			&acidv1.Postgresql{
				ObjectMeta: metav1.ObjectMeta{
					Name: clusterName,
					Annotations: map[string]string{
						"delete-date": today,
					},
				},
			},
			"annotation delete-clustername not set in manifest to allow cluster deletion",
		},
	}
	for _, tt := range tests {
		if err := postgresqlTestController.meetsClusterDeleteAnnotations(tt.pg); err != nil {
			if !reflect.DeepEqual(err.Error(), tt.error) {
				t.Errorf("Expected error %q, got: %v", tt.error, err)
			}
		}
	}
}

func TestQueueClusterEvents(t *testing.T) {
	c := NewController(&spec.ControllerConfig{}, "queue-test")
	c.opConfig.Workers = 1
	c.opConfig.EnableOwnerReferences = util.True() // allows deletion without annotations
	c.clusterEventStores = []cache.Store{cache.NewStore(clusterEventKeyFunc)}
	c.clusterEventQueues = []*cache.FIFO{cache.NewFIFO(clusterEventKeyFunc)}

	pgWithVersion := func(resourceVersion string) *acidv1.Postgresql {
		return &acidv1.Postgresql{ObjectMeta: metav1.ObjectMeta{
			Name: "acid-test", Namespace: "default", UID: "uid-1", ResourceVersion: resourceVersion}}
	}
	sorted := func(types []EventType) []EventType {
		sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })
		return types
	}
	stored := func() []EventType {
		var types []EventType
		for _, obj := range c.clusterEventStores[0].List() {
			types = append(types, obj.(ClusterEvent).EventType)
		}
		return sorted(types)
	}
	// a closed queue still hands out the remaining events
	drainQueue := func() []EventType {
		var types []EventType
		c.clusterEventQueues[0].Close()
		for {
			obj, err := c.clusterEventQueues[0].Pop(func(interface{}, bool) error { return nil })
			if err != nil {
				return sorted(types)
			}
			types = append(types, obj.(ClusterEvent).EventType)
		}
	}

	// updates are not merged, because each one is applied based on its own old and new spec
	c.queueClusterEvent(pgWithVersion("1"), pgWithVersion("2"), EventUpdate)
	c.queueClusterEvent(pgWithVersion("2"), pgWithVersion("3"), EventUpdate)
	// syncs replace each other
	c.queueClusterEvent(nil, pgWithVersion("3"), EventSync)
	c.queueClusterEvent(nil, pgWithVersion("3"), EventSync)

	expected := []EventType{EventSync, EventUpdate, EventUpdate}
	if got := stored(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected pending events %v, got %v", expected, got)
	}

	// a delete discards all pending events of the cluster
	c.queueClusterEvent(pgWithVersion("3"), nil, EventDelete)

	expected = []EventType{EventDelete}
	if got := stored(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected pending events %v after delete, got %v", expected, got)
	}
	if got := drainQueue(); !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected queued events %v after delete, got %v", expected, got)
	}
}
