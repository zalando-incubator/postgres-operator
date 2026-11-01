package controller

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
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

// newPostgresqlTestControllerWithQueues builds a controller with just enough of the
// event machinery for queueClusterEvent to run. The real operator sets these up in
// initController, which also reads infrastructure roles and starts the API server.
func newPostgresqlTestControllerWithQueues() *Controller {
	c := NewController(&spec.ControllerConfig{}, "postgresql-test")
	c.opConfig.Workers = 1
	keyFn := func(obj interface{}) (string, error) {
		e, ok := obj.(ClusterEvent)
		if !ok {
			return "", fmt.Errorf("could not cast to cluster event")
		}
		return queueClusterKey(e.EventType, e.UID), nil
	}
	c.clusterEventStores = []cache.Store{cache.NewStore(keyFn)}
	c.clusterEventQueues = []*cache.FIFO{cache.NewFIFO(keyFn)}
	return c
}

func testPostgresqlOwnedBy(controller string) *acidv1.Postgresql {
	return &acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "acid-test",
			Namespace:   "default",
			UID:         "00000000-0000-0000-0000-000000000001",
			Annotations: map[string]string{"acid.zalan.do/controller": controller},
		},
	}
}

// Changing the controller annotation to this operator's ID makes postgresqlCheck reject
// the previous manifest, so without an explicit branch the event is dropped entirely and
// the cluster is only picked up by the next resync.
func TestPostgresqlUpdateQueuesSyncWhenClusterIsAdopted(t *testing.T) {
	c := newPostgresqlTestControllerWithQueues()

	c.postgresqlUpdate(testPostgresqlOwnedBy("another-operator"), testPostgresqlOwnedBy("postgresql-test"))

	events := c.clusterEventStores[0].List()
	if len(events) != 1 {
		t.Fatalf("adopting a cluster should queue exactly one event, queued %d", len(events))
	}
	event, ok := events[0].(ClusterEvent)
	if !ok {
		t.Fatalf("queued object is not a ClusterEvent: %T", events[0])
	}
	if event.EventType != EventSync {
		t.Errorf("adoption should queue %q, queued %q", EventSync, event.EventType)
	}
	if event.OldSpec != nil {
		t.Errorf("there is no trustworthy old spec on adoption, got %+v", event.OldSpec)
	}
	if event.NewSpec == nil || event.NewSpec.Name != "acid-test" {
		t.Errorf("the new spec should be the adopted cluster, got %+v", event.NewSpec)
	}
}

// An ordinary edit to a cluster this operator already owns must still queue an update.
func TestPostgresqlUpdateStillQueuesUpdateForOwnedCluster(t *testing.T) {
	c := newPostgresqlTestControllerWithQueues()

	owned := testPostgresqlOwnedBy("postgresql-test")
	edited := testPostgresqlOwnedBy("postgresql-test")
	edited.Spec.NumberOfInstances = 3

	c.postgresqlUpdate(owned, edited)

	events := c.clusterEventStores[0].List()
	if len(events) != 1 {
		t.Fatalf("editing an owned cluster should queue exactly one event, queued %d", len(events))
	}
	event := events[0].(ClusterEvent)
	if event.EventType != EventUpdate {
		t.Errorf("an owned edit should queue %q, queued %q", EventUpdate, event.EventType)
	}
	if event.OldSpec == nil {
		t.Error("an owned edit has a usable old spec and should carry it")
	}
}

// Losing ownership is deliberately left alone: stopping management is a different
// decision from starting it, and this operator cannot know the new owner is ready.
func TestPostgresqlUpdateQueuesNothingWhenOwnershipIsLost(t *testing.T) {
	c := newPostgresqlTestControllerWithQueues()

	c.postgresqlUpdate(testPostgresqlOwnedBy("postgresql-test"), testPostgresqlOwnedBy("another-operator"))

	if events := c.clusterEventStores[0].List(); len(events) != 0 {
		t.Errorf("losing ownership should queue nothing, queued %d", len(events))
	}
}
