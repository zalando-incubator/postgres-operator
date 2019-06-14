package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var parseTimeTests = []struct {
	in  string
	out metav1.Time
	err error
}{
	{"16:08", mustParseTime("16:08"), nil},
	{"11:00", mustParseTime("11:00"), nil},
	{"23:59", mustParseTime("23:59"), nil},

	{"26:09", metav1.Now(), errors.New(`parsing time "26:09": hour out of range`)},
	{"23:69", metav1.Now(), errors.New(`parsing time "23:69": minute out of range`)},
}

var parseWeekdayTests = []struct {
	in  string
	out time.Weekday
	err error
}{
	{"Wed", time.Wednesday, nil},
	{"Sunday", time.Weekday(0), errors.New("incorrect weekday")},
	{"", time.Weekday(0), errors.New("incorrect weekday")},
}

var clusterNames = []struct {
	in          string
	inTeam      string
	clusterName string
	err         error
}{
	{"acid-test", "acid", "test", nil},
	{"test-my-name", "test", "my-name", nil},
	{"my-team-another-test", "my-team", "another-test", nil},
	{"------strange-team-cluster", "-----", "strange-team-cluster",
		errors.New(`name must confirm to DNS-1035, regex used for validation is "^[a-z]([-a-z0-9]*[a-z0-9])?$"`)},
	{"fooobar-fooobarfooobarfooobarfooobarfooobarfooobarfooobarfooobar", "fooobar", "",
		errors.New("name cannot be longer than 58 characters")},
	{"acid-test", "test", "", errors.New("name must match {TEAM}-{NAME} format")},
	{"-test", "", "", errors.New("team name is empty")},
	{"-test", "-", "", errors.New("name must match {TEAM}-{NAME} format")},
	{"", "-", "", errors.New("cluster name must match {TEAM}-{NAME} format. Got cluster name '', team name '-'")},
	{"-", "-", "", errors.New("cluster name must match {TEAM}-{NAME} format. Got cluster name '-', team name '-'")},
	// user may specify the team part of the full cluster name differently from the team name returned by the Teams API
	// in the case the actual Teams API name is long enough, this will fail the check
	{"foo-bar", "qwerty", "", errors.New("cluster name must match {TEAM}-{NAME} format. Got cluster name 'foo-bar', team name 'qwerty'")},
}

var cloneClusterDescriptions = []struct {
	in  *CloneDescription
	err error
}{
	{&CloneDescription{"foo+bar", "", "NotEmpty", "", "", "", "", nil}, nil},
	{&CloneDescription{"foo+bar", "", "", "", "", "", "", nil},
		errors.New(`clone cluster name must confirm to DNS-1035, regex used for validation is "^[a-z]([-a-z0-9]*[a-z0-9])?$"`)},
	{&CloneDescription{"foobar123456789012345678901234567890123456789012345678901234567890", "", "", "", "", "", "", nil},
		errors.New("clone cluster name must be no longer than 63 characters")},
	{&CloneDescription{"foobar", "", "", "", "", "", "", nil}, nil},
}

var maintenanceWindows = []struct {
	in  []byte
	out MaintenanceWindow
	err error
}{{[]byte(`"Tue:10:00-20:00"`),
	MaintenanceWindow{
		Everyday:  false,
		Weekday:   time.Tuesday,
		StartTime: mustParseTime("10:00"),
		EndTime:   mustParseTime("20:00"),
	}, nil},
	{[]byte(`"Mon:10:00-10:00"`),
		MaintenanceWindow{
			Everyday:  false,
			Weekday:   time.Monday,
			StartTime: mustParseTime("10:00"),
			EndTime:   mustParseTime("10:00"),
		}, nil},
	{[]byte(`"Sun:00:00-00:00"`),
		MaintenanceWindow{
			Everyday:  false,
			Weekday:   time.Sunday,
			StartTime: mustParseTime("00:00"),
			EndTime:   mustParseTime("00:00"),
		}, nil},
	{[]byte(`"01:00-10:00"`),
		MaintenanceWindow{
			Everyday:  true,
			Weekday:   time.Sunday,
			StartTime: mustParseTime("01:00"),
			EndTime:   mustParseTime("10:00"),
		}, nil},
	{[]byte(`"Mon:12:00-11:00"`), MaintenanceWindow{}, errors.New(`'From' time must be prior to the 'To' time`)},
	{[]byte(`"Wed:33:00-00:00"`), MaintenanceWindow{}, errors.New(`could not parse start time: parsing time "33:00": hour out of range`)},
	{[]byte(`"Wed:00:00-26:00"`), MaintenanceWindow{}, errors.New(`could not parse end time: parsing time "26:00": hour out of range`)},
	{[]byte(`"Sunday:00:00-00:00"`), MaintenanceWindow{}, errors.New(`could not parse weekday: incorrect weekday`)},
	{[]byte(`":00:00-10:00"`), MaintenanceWindow{}, errors.New(`could not parse weekday: incorrect weekday`)},
	{[]byte(`"Mon:10:00-00:00"`), MaintenanceWindow{}, errors.New(`'From' time must be prior to the 'To' time`)},
	{[]byte(`"Mon:00:00:00-10:00:00"`), MaintenanceWindow{}, errors.New(`incorrect maintenance window format`)},
	{[]byte(`"Mon:00:00"`), MaintenanceWindow{}, errors.New("incorrect maintenance window format")},
	{[]byte(`"Mon:00:00-00:00:00"`), MaintenanceWindow{}, errors.New("could not parse end time: incorrect time format")}}

var postgresStatus = []struct {
	in  []byte
	out PostgresStatus
	err error
}{
	{[]byte(`{"PostgresClusterStatus":"Running"}`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusRunning}, nil},
	{[]byte(`{"PostgresClusterStatus":""}`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusUnknown}, nil},
	{[]byte(`"Running"`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusRunning}, nil},
	{[]byte(`""`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusUnknown}, nil}}

var unmarshalCluster = []struct {
	in      []byte
	out     Postgresql
	marshal []byte
	err     error
}{
	// example with simple status field
	{
		in: []byte(`{
	  "kind": "Postgresql","apiVersion": "acid.zalan.do/v1",
	  "metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": 100}}`),
		out: Postgresql{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Postgresql",
				APIVersion: "acid.zalan.do/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "acid-testcluster1",
			},
			Status: PostgresStatus{PostgresClusterStatus: ClusterStatusInvalid},
			// This error message can vary between Go versions, so compute it for the current version.
			Error: json.Unmarshal([]byte(`{"teamId": 0}`), &PostgresSpec{}).Error(),
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null, "standby_cluster": null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":{}},"status":"Invalid"}`),
		err:     nil},
	// example with /status subresource
	{
		in: []byte(`{
	  "kind": "Postgresql","apiVersion": "acid.zalan.do/v1",
	  "metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": 100}}`),
		out: Postgresql{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Postgresql",
				APIVersion: "acid.zalan.do/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "acid-testcluster1",
			},
			Status: PostgresStatus{PostgresClusterStatus: ClusterStatusInvalid},
			// This error message can vary between Go versions, so compute it for the current version.
			Error: json.Unmarshal([]byte(`{"teamId": 0}`), &PostgresSpec{}).Error(),
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null, "standby_cluster": null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":{}},"status":{"PostgresClusterStatus":"Invalid"}}`),
		err:     nil},
	// example with detailed input manifest
	{
		in: []byte(`{
	  "kind": "Postgresql",
	  "apiVersion": "acid.zalan.do/v1",
	  "metadata": {
	    "name": "acid-testcluster1"
	  },
	  "spec": {
	    "teamId": "ACID",
	    "volume": {
	      "size": "5Gi",
	      "storageClass": "SSD"
	    },
	    "numberOfInstances": 2,
	    "users": {
	      "zalando": [
	        "superuser",
	        "createdb"
	      ]
	    },
	    "allowedSourceRanges": [
	      "127.0.0.1/32"
	    ],
	    "postgresql": {
	      "version": "9.6",
	      "parameters": {
	        "shared_buffers": "32MB",
	        "max_connections": "10",
	        "log_statement": "all"
	      }
	    },
	    "resources": {
	      "requests": {
	        "cpu": "10m",
	        "memory": "50Mi"
	      },
	      "limits": {
	        "cpu": "300m",
	        "memory": "3000Mi"
	      }
	    },
	    "clone" : {
	     "cluster": "acid-batman"
	     },
	    "patroni": {
	      "initdb": {
	        "encoding": "UTF8",
	        "locale": "en_US.UTF-8",
	        "data-checksums": "true"
	      },
	      "pg_hba": [
	        "hostssl all all 0.0.0.0/0 md5",
	        "host    all all 0.0.0.0/0 md5"
	      ],
	      "ttl": 30,
	      "loop_wait": 10,
	      "retry_timeout": 10,
		    "maximum_lag_on_failover": 33554432,
			  "slots" : {
				  "permanent_logical_1" : {
					  "type"     : "logical",
					  "database" : "foo",
					  "plugin"   : "pgoutput"
			       }
			  }
	  	},
	  	"maintenanceWindows": [
	    	"Mon:01:00-06:00",
	    	"Sat:00:00-04:00",
	    	"05:00-05:15"
	  	]
	  }
		}`),
		out: Postgresql{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Postgresql",
				APIVersion: "acid.zalan.do/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "acid-testcluster1",
			},
			Spec: PostgresSpec{
				PostgresqlParam: PostgresqlParam{
					PgVersion: "9.6",
					Parameters: map[string]string{
						"shared_buffers":  "32MB",
						"max_connections": "10",
						"log_statement":   "all",
					},
				},
				Volume: Volume{
					Size:         "5Gi",
					StorageClass: "SSD",
				},
				Patroni: Patroni{
					InitDB: map[string]string{
						"encoding":       "UTF8",
						"locale":         "en_US.UTF-8",
						"data-checksums": "true",
					},
					PgHba:                []string{"hostssl all all 0.0.0.0/0 md5", "host    all all 0.0.0.0/0 md5"},
					TTL:                  30,
					LoopWait:             10,
					RetryTimeout:         10,
					MaximumLagOnFailover: 33554432,
					Slots:                map[string]map[string]string{"permanent_logical_1": {"type": "logical", "database": "foo", "plugin": "pgoutput"}},
				},
				Resources: Resources{
					ResourceRequests: ResourceDescription{CPU: "10m", Memory: "50Mi"},
					ResourceLimits:   ResourceDescription{CPU: "300m", Memory: "3000Mi"},
				},

				TeamID:              "ACID",
				AllowedSourceRanges: []string{"127.0.0.1/32"},
				NumberOfInstances:   2,
				Users:               map[string]UserFlags{"zalando": {"superuser", "createdb"}},
				MaintenanceWindows: []MaintenanceWindow{{
					Everyday:  false,
					Weekday:   time.Monday,
					StartTime: mustParseTime("01:00"),
					EndTime:   mustParseTime("06:00"),
				}, {
					Everyday:  false,
					Weekday:   time.Saturday,
					StartTime: mustParseTime("00:00"),
					EndTime:   mustParseTime("04:00"),
				},
					{
						Everyday:  true,
						Weekday:   time.Sunday,
						StartTime: mustParseTime("05:00"),
						EndTime:   mustParseTime("05:15"),
					},
				},
				Clone: CloneDescription{
					ClusterName: "acid-batman",
				},
				ClusterName: "testcluster1",
			},
			Error: "",
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"9.6","parameters":{"log_statement":"all","max_connections":"10","shared_buffers":"32MB"}},"volume":{"size":"5Gi","storageClass":"SSD"},"patroni":{"initdb":{"data-checksums":"true","encoding":"UTF8","locale":"en_US.UTF-8"},"pg_hba":["hostssl all all 0.0.0.0/0 md5","host    all all 0.0.0.0/0 md5"],"ttl":30,"loop_wait":10,"retry_timeout":10,"maximum_lag_on_failover":33554432,"slots":{"permanent_logical_1":{"database":"foo","plugin":"pgoutput","type":"logical"}}, "standby_cluster": null},"resources":{"requests":{"cpu":"10m","memory":"50Mi"},"limits":{"cpu":"300m","memory":"3000Mi"}},"teamId":"ACID","allowedSourceRanges":["127.0.0.1/32"],"numberOfInstances":2,"users":{"zalando":["superuser","createdb"]},"maintenanceWindows":["Mon:01:00-06:00","Sat:00:00-04:00","05:00-05:15"],"clone":{"cluster":"acid-batman"}},"status":{"PostgresClusterStatus":""}}`),
		err:     nil},
	// example with teamId set in input
	{
		in: []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1","metadata": {"name": "teapot-testcluster1"}, "spec": {"teamId": "acid"}}`),
		out: Postgresql{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Postgresql",
				APIVersion: "acid.zalan.do/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "teapot-testcluster1",
			},
			Spec:   PostgresSpec{TeamID: "acid"},
			Status: PostgresStatus{PostgresClusterStatus: ClusterStatusInvalid},
			Error:  errors.New("name must match {TEAM}-{NAME} format").Error(),
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"teapot-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null, "standby_cluster": null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":{}},"status":{"PostgresClusterStatus":"Invalid"}}`),
		err:     nil},
	// clone example
	{
		in: []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1","metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": "acid", "clone": {"cluster": "team-batman"}}}`),
		out: Postgresql{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Postgresql",
				APIVersion: "acid.zalan.do/v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: "acid-testcluster1",
			},
			Spec: PostgresSpec{
				TeamID: "acid",
				Clone: CloneDescription{
					ClusterName: "team-batman",
				},
				ClusterName: "testcluster1",
			},
			Error: "",
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null, "standby_cluster": null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":{"cluster":"team-batman"}},"status":{"PostgresClusterStatus":""}}`),
		err:     nil},
	// erroneous examples
	{
		in:      []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1"`),
		out:     Postgresql{},
		marshal: []byte{},
		err:     errors.New("unexpected end of JSON input")},
	{
		in:      []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster","creationTimestamp":qaz},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":{}},"status":{"PostgresClusterStatus":"Invalid"}}`),
		out:     Postgresql{},
		marshal: []byte{},
		err:     errors.New("invalid character 'q' looking for beginning of value")}}

var postgresqlList = []struct {
	in  []byte
	out PostgresqlList
	err error
}{
	{[]byte(`{"apiVersion":"v1","items":[{"apiVersion":"acid.zalan.do/v1","kind":"Postgresql","metadata":{"labels":{"team":"acid"},"name":"acid-testcluster42","namespace":"default","resourceVersion":"30446957","selfLink":"/apis/acid.zalan.do/v1/namespaces/default/postgresqls/acid-testcluster42","uid":"857cd208-33dc-11e7-b20a-0699041e4b03"},"spec":{"allowedSourceRanges":["185.85.220.0/22"],"numberOfInstances":1,"postgresql":{"version":"9.6"},"teamId":"acid","volume":{"size":"10Gi"}},"status":{"PostgresClusterStatus":"Running"}}],"kind":"List","metadata":{},"resourceVersion":"","selfLink":""}`),
		PostgresqlList{
			TypeMeta: metav1.TypeMeta{
				Kind:       "List",
				APIVersion: "v1",
			},
			Items: []Postgresql{{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Postgresql",
					APIVersion: "acid.zalan.do/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "acid-testcluster42",
					Namespace:       "default",
					Labels:          map[string]string{"team": "acid"},
					ResourceVersion: "30446957",
					SelfLink:        "/apis/acid.zalan.do/v1/namespaces/default/postgresqls/acid-testcluster42",
					UID:             "857cd208-33dc-11e7-b20a-0699041e4b03",
				},
				Spec: PostgresSpec{
					ClusterName:         "testcluster42",
					PostgresqlParam:     PostgresqlParam{PgVersion: "9.6"},
					Volume:              Volume{Size: "10Gi"},
					TeamID:              "acid",
					AllowedSourceRanges: []string{"185.85.220.0/22"},
					NumberOfInstances:   1,
				},
				Status: PostgresStatus{
					PostgresClusterStatus: ClusterStatusRunning,
				},
				Error: "",
			}},
		},
		nil},
	{[]byte(`{"apiVersion":"v1","items":[{"apiVersion":"acid.zalan.do/v1","kind":"Postgresql","metadata":{"labels":{"team":"acid"},"name":"acid-testcluster42","namespace"`),
		PostgresqlList{},
		errors.New("unexpected end of JSON input")}}

func mustParseTime(s string) metav1.Time {
	v, err := time.Parse("15:04", s)
	if err != nil {
		panic(err)
	}

	return metav1.Time{Time: v.UTC()}
}

func TestParseTime(t *testing.T) {
	for _, tt := range parseTimeTests {
		aTime, err := parseTime(tt.in)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("ParseTime expected error: %v, got: %v", tt.err, err)
			}
			continue
		} else if tt.err != nil {
			t.Errorf("Expected error: %v", tt.err)
		}

		if aTime != tt.out {
			t.Errorf("Expected time: %v, got: %v", tt.out, aTime)
		}
	}
}

func TestWeekdayTime(t *testing.T) {
	for _, tt := range parseWeekdayTests {
		aTime, err := parseWeekday(tt.in)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("ParseWeekday expected error: %v, got: %v", tt.err, err)
			}
			continue
		} else if tt.err != nil {
			t.Errorf("Expected error: %v", tt.err)
		}

		if aTime != tt.out {
			t.Errorf("Expected weekday: %v, got: %v", tt.out, aTime)
		}
	}
}

func TestClusterName(t *testing.T) {
	for _, tt := range clusterNames {
		name, err := extractClusterName(tt.in, tt.inTeam)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("extractClusterName expected error: %v, got: %v", tt.err, err)
			}
			continue
		} else if tt.err != nil {
			t.Errorf("Expected error: %v", tt.err)
		}
		if name != tt.clusterName {
			t.Errorf("Expected cluserName: %q, got: %q", tt.clusterName, name)
		}
	}
}

func TestCloneClusterDescription(t *testing.T) {
	for _, tt := range cloneClusterDescriptions {
		if err := validateCloneClusterDescription(tt.in); err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("testCloneClusterDescription expected error: %v, got: %v", tt.err, err)
			}
		} else if tt.err != nil {
			t.Errorf("Expected error: %v", tt.err)
		}
	}
}

func TestUnmarshalMaintenanceWindow(t *testing.T) {
	for _, tt := range maintenanceWindows {
		var m MaintenanceWindow
		err := m.UnmarshalJSON(tt.in)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("MaintenanceWindow unmarshal expected error: %v, got %v", tt.err, err)
			}
			continue
		} else if tt.err != nil {
			t.Errorf("Expected error: %v", tt.err)
		}

		if !reflect.DeepEqual(m, tt.out) {
			t.Errorf("Expected maintenance window: %#v, got: %#v", tt.out, m)
		}
	}
}

func TestMarshalMaintenanceWindow(t *testing.T) {
	for _, tt := range maintenanceWindows {
		if tt.err != nil {
			continue
		}

		s, err := tt.out.MarshalJSON()
		if err != nil {
			t.Errorf("Marshal Error: %v", err)
		}

		if !bytes.Equal(s, tt.in) {
			t.Errorf("Expected Marshal: %q, got: %q", string(tt.in), string(s))
		}
	}
}

func TestUnmarshalPostgresStatus(t *testing.T) {
	for _, tt := range postgresStatus {
		var ps PostgresStatus
		err := ps.UnmarshalJSON(tt.in)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("CR status unmarshal expected error: %v, got %v", tt.err, err)
			}
			continue
			//} else if tt.err != nil {
			//t.Errorf("Expected error: %v", tt.err)
		}

		if !reflect.DeepEqual(ps, tt.out) {
			t.Errorf("Expected status: %#v, got: %#v", tt.out, ps)
		}
	}
}

func TestPostgresUnmarshal(t *testing.T) {
	for _, tt := range unmarshalCluster {
		var cluster Postgresql
		err := cluster.UnmarshalJSON(tt.in)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("Unmarshal expected error: %v, got: %v", tt.err, err)
			}
			continue
		} else if tt.err != nil {
			t.Errorf("Expected error: %v", tt.err)
		}

		if !reflect.DeepEqual(cluster, tt.out) {
			t.Errorf("Expected Postgresql: %#v, got %#v", tt.out, cluster)
		}
	}
}

func TestMarshal(t *testing.T) {
	for _, tt := range unmarshalCluster {
		if tt.err != nil {
			continue
		}

		// Unmarshal and marshal example to capture api changes
		var cluster Postgresql
		err := cluster.UnmarshalJSON(tt.marshal)
		if err != nil {
			if tt.err == nil || err.Error() != tt.err.Error() {
				t.Errorf("Backwards compatibility unmarshal expected error: %v, got: %v", tt.err, err)
			}
			continue
		}
		expected, err := json.Marshal(cluster)
		if err != nil {
			t.Errorf("Backwards compatibility marshal error: %v", err)
		}

		m, err := json.Marshal(tt.out)
		if err != nil {
			t.Errorf("Marshal error: %v", err)
		}
		if !bytes.Equal(m, expected) {
			t.Errorf("Marshal Postgresql \nexpected: %q, \ngot:      %q", string(expected), string(m))
		}
	}
}

func TestPostgresMeta(t *testing.T) {
	for _, tt := range unmarshalCluster {
		if a := tt.out.GetObjectKind(); a != &tt.out.TypeMeta {
			t.Errorf("GetObjectKindMeta \nexpected: %v, \ngot:       %v", tt.out.TypeMeta, a)
		}

		if a := tt.out.GetObjectMeta(); reflect.DeepEqual(a, tt.out.ObjectMeta) {
			t.Errorf("GetObjectMeta \nexpected: %v, \ngot:       %v", tt.out.ObjectMeta, a)
		}
	}
}

func TestPostgresListMeta(t *testing.T) {
	for _, tt := range postgresqlList {
		if tt.err != nil {
			continue
		}

		if a := tt.out.GetObjectKind(); a != &tt.out.TypeMeta {
			t.Errorf("GetObjectKindMeta expected: %v, got: %v", tt.out.TypeMeta, a)
		}

		if a := tt.out.GetListMeta(); reflect.DeepEqual(a, tt.out.ListMeta) {
			t.Errorf("GetObjectMeta expected: %v, got: %v", tt.out.ListMeta, a)
		}

		return
	}
}

func TestPostgresqlClone(t *testing.T) {
	for _, tt := range unmarshalCluster {
		cp := &tt.out
		cp.Error = ""
		clone := cp.Clone()
		if !reflect.DeepEqual(clone, cp) {
			t.Errorf("TestPostgresqlClone expected: \n%#v\n, got \n%#v", cp, clone)
		}

	}
}
