package v1

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/zalando/postgres-operator/pkg/util"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var parseTimeTests = []struct {
	about string
	in    string
	out   metav1.Time
	err   error
}{
	{"parse common time with minutes", "16:08", mustParseTime("16:08"), nil},
	{"parse time with zeroed minutes", "11:00", mustParseTime("11:00"), nil},
	{"parse corner case last minute of the day", "23:59", mustParseTime("23:59"), nil},

	{"expect error as hour is out of range", "26:09", metav1.Now(), errors.New(`parsing time "26:09": hour out of range`)},
	{"expect error as minute is out of range", "23:69", metav1.Now(), errors.New(`parsing time "23:69": minute out of range`)},
}

var parseWeekdayTests = []struct {
	about string
	in    string
	out   time.Weekday
	err   error
}{
	{"parse common weekday", "Wed", time.Wednesday, nil},
	{"expect error as weekday is invalid", "Sunday", time.Weekday(0), errors.New("incorrect weekday")},
	{"expect error as weekday is empty", "", time.Weekday(0), errors.New("incorrect weekday")},
}

var clusterNames = []struct {
	about       string
	in          string
	inTeam      string
	clusterName string
	err         error
}{
	{"common team and cluster name", "acid-test", "acid", "test", nil},
	{"cluster name with hyphen", "test-my-name", "test", "my-name", nil},
	{"cluster and team name with hyphen", "my-team-another-test", "my-team", "another-test", nil},
	{"expect error as cluster name is just hyphens", "------strange-team-cluster", "-----", "strange-team-cluster",
		errors.New(`name must confirm to DNS-1035, regex used for validation is "^[a-z]([-a-z0-9]*[a-z0-9])?$"`)},
	{"expect error as cluster name is too long", "fooobar-fooobarfooobarfooobarfooobarfooobarfooobarfooobarfooobar", "fooobar", "",
		errors.New("name cannot be longer than 58 characters")},
	{"expect error as cluster name does not match {TEAM}-{NAME} format", "acid-test", "test", "", errors.New("name must match {TEAM}-{NAME} format")},
	{"expect error as team and cluster name are empty", "-test", "", "", errors.New("team name is empty")},
	{"expect error as cluster name is empty and team name is a hyphen", "-test", "-", "", errors.New("name must match {TEAM}-{NAME} format")},
	{"expect error as cluster name is empty, team name is a hyphen and cluster name is empty", "", "-", "", errors.New("cluster name must match {TEAM}-{NAME} format. Got cluster name '', team name '-'")},
	{"expect error as cluster and team name are hyphens", "-", "-", "", errors.New("cluster name must match {TEAM}-{NAME} format. Got cluster name '-', team name '-'")},
	// user may specify the team part of the full cluster name differently from the team name returned by the Teams API
	// in the case the actual Teams API name is long enough, this will fail the check
	{"expect error as team name does not match", "foo-bar", "qwerty", "", errors.New("cluster name must match {TEAM}-{NAME} format. Got cluster name 'foo-bar', team name 'qwerty'")},
}

var cloneClusterDescriptions = []struct {
	about string
	in    *CloneDescription
	err   error
}{
	{"cluster name invalid but EndTimeSet is not empty", &CloneDescription{"foo+bar", "", "NotEmpty", "", "", "", "", nil}, nil},
	{"expect error as cluster name does not match DNS-1035", &CloneDescription{"foo+bar", "", "", "", "", "", "", nil},
		errors.New(`clone cluster name must confirm to DNS-1035, regex used for validation is "^[a-z]([-a-z0-9]*[a-z0-9])?$"`)},
	{"expect error as cluster name is too long", &CloneDescription{"foobar123456789012345678901234567890123456789012345678901234567890", "", "", "", "", "", "", nil},
		errors.New("clone cluster name must be no longer than 63 characters")},
	{"common cluster name", &CloneDescription{"foobar", "", "", "", "", "", "", nil}, nil},
}

var maintenanceWindows = []struct {
	about string
	in    []byte
	out   MaintenanceWindow
	err   error
}{{"regular scenario",
	[]byte(`"Tue:10:00-20:00"`),
	MaintenanceWindow{
		Everyday:  false,
		Weekday:   time.Tuesday,
		StartTime: mustParseTime("10:00"),
		EndTime:   mustParseTime("20:00"),
	}, nil},
	{"starts and ends at the same time",
		[]byte(`"Mon:10:00-10:00"`),
		MaintenanceWindow{
			Everyday:  false,
			Weekday:   time.Monday,
			StartTime: mustParseTime("10:00"),
			EndTime:   mustParseTime("10:00"),
		}, nil},
	{"starts and ends 00:00 on sunday",
		[]byte(`"Sun:00:00-00:00"`),
		MaintenanceWindow{
			Everyday:  false,
			Weekday:   time.Sunday,
			StartTime: mustParseTime("00:00"),
			EndTime:   mustParseTime("00:00"),
		}, nil},
	{"without day indication should define to sunday",
		[]byte(`"01:00-10:00"`),
		MaintenanceWindow{
			Everyday:  true,
			Weekday:   time.Sunday,
			StartTime: mustParseTime("01:00"),
			EndTime:   mustParseTime("10:00"),
		}, nil},
	{"expect error as 'From' is later than 'To'", []byte(`"Mon:12:00-11:00"`), MaintenanceWindow{}, errors.New(`'From' time must be prior to the 'To' time`)},
	{"expect error as 'From' is later than 'To' with 00:00 corner case", []byte(`"Mon:10:00-00:00"`), MaintenanceWindow{}, errors.New(`'From' time must be prior to the 'To' time`)},
	{"expect error as 'From' time is not valid", []byte(`"Wed:33:00-00:00"`), MaintenanceWindow{}, errors.New(`could not parse start time: parsing time "33:00": hour out of range`)},
	{"expect error as 'To' time is not valid", []byte(`"Wed:00:00-26:00"`), MaintenanceWindow{}, errors.New(`could not parse end time: parsing time "26:00": hour out of range`)},
	{"expect error as weekday is not valid", []byte(`"Sunday:00:00-00:00"`), MaintenanceWindow{}, errors.New(`could not parse weekday: incorrect weekday`)},
	{"expect error as weekday is empty", []byte(`":00:00-10:00"`), MaintenanceWindow{}, errors.New(`could not parse weekday: incorrect weekday`)},
	{"expect error as maintenance window set seconds", []byte(`"Mon:00:00:00-10:00:00"`), MaintenanceWindow{}, errors.New(`incorrect maintenance window format`)},
	{"expect error as 'To' time set seconds", []byte(`"Mon:00:00-00:00:00"`), MaintenanceWindow{}, errors.New("could not parse end time: incorrect time format")},
	{"expect error as 'To' time is missing", []byte(`"Mon:00:00"`), MaintenanceWindow{}, errors.New("incorrect maintenance window format")}}

var postgresStatus = []struct {
	about string
	in    []byte
	out   PostgresStatus
	err   error
}{
	{"cluster running", []byte(`{"PostgresClusterStatus":"Running"}`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusRunning}, nil},
	{"cluster status undefined", []byte(`{"PostgresClusterStatus":""}`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusUnknown}, nil},
	{"cluster running without full JSON format", []byte(`"Running"`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusRunning}, nil},
	{"cluster status empty", []byte(`""`),
		PostgresStatus{PostgresClusterStatus: ClusterStatusUnknown}, nil}}

var tmp postgresqlCopy
var unmarshalCluster = []struct {
	about   string
	in      []byte
	out     Postgresql
	marshal []byte
	err     error
}{
	{
		about: "example with simple status field",
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
			Error: json.Unmarshal([]byte(`{
				"kind": "Postgresql","apiVersion": "acid.zalan.do/v1",
				"metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": 100}}`), &tmp).Error(),
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":null},"status":"Invalid"}`),
		err:     nil},
	{
		about: "example with /status subresource",
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
			Error: json.Unmarshal([]byte(`{
				"kind": "Postgresql","apiVersion": "acid.zalan.do/v1",
				"metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": 100}}`), &tmp).Error(),
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":null},"status":{"PostgresClusterStatus":"Invalid"}}`),
		err:     nil},
	{
		about: "example with detailed input manifest and deprecated pod_priority_class_name -> podPriorityClassName",
		in: []byte(`{
	  "kind": "Postgresql",
	  "apiVersion": "acid.zalan.do/v1",
	  "metadata": {
	    "name": "acid-testcluster1"
	  },
	  "spec": {
	    "teamId": "acid",
		"pod_priority_class_name": "spilo-pod-priority",
	    "volume": {
	      "size": "5Gi",
	      "storageClass": "SSD",
	      "subPath": "subdir"
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
		"enableShmVolume": false,
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
				PodPriorityClassNameOld: "spilo-pod-priority",
				Volume: Volume{
					Size:         "5Gi",
					StorageClass: "SSD",
					SubPath:      "subdir",
				},
				ShmVolume: util.False(),
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
					SynchronousNodeCount: 1,
					Slots:                map[string]map[string]string{"permanent_logical_1": {"type": "logical", "database": "foo", "plugin": "pgoutput"}},
				},
				Resources: Resources{
					ResourceRequests: ResourceDescription{CPU: "10m", Memory: "50Mi"},
					ResourceLimits:   ResourceDescription{CPU: "300m", Memory: "3000Mi"},
				},

				TeamID:              "acid",
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
				Clone: &CloneDescription{
					ClusterName: "acid-batman",
				},
				ClusterName: "testcluster1",
			},
			Error: "",
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"9.6","parameters":{"log_statement":"all","max_connections":"10","shared_buffers":"32MB"}},"pod_priority_class_name":"spilo-pod-priority","volume":{"size":"5Gi","storageClass":"SSD", "subPath": "subdir"},"enableShmVolume":false,"patroni":{"initdb":{"data-checksums":"true","encoding":"UTF8","locale":"en_US.UTF-8"},"pg_hba":["hostssl all all 0.0.0.0/0 md5","host    all all 0.0.0.0/0 md5"],"ttl":30,"loop_wait":10,"retry_timeout":10,"maximum_lag_on_failover":33554432,"slots":{"permanent_logical_1":{"database":"foo","plugin":"pgoutput","type":"logical"}}},"resources":{"requests":{"cpu":"10m","memory":"50Mi"},"limits":{"cpu":"300m","memory":"3000Mi"}},"teamId":"acid","allowedSourceRanges":["127.0.0.1/32"],"numberOfInstances":2,"users":{"zalando":["superuser","createdb"]},"maintenanceWindows":["Mon:01:00-06:00","Sat:00:00-04:00","05:00-05:15"],"clone":{"cluster":"acid-batman"}},"status":{"PostgresClusterStatus":""}}`),
		err:     nil},
	{
		about: "example with teamId set in input",
		in:    []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1","metadata": {"name": "teapot-testcluster1"}, "spec": {"teamId": "acid"}}`),
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
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"teapot-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null} ,"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":null},"status":{"PostgresClusterStatus":"Invalid"}}`),
		err:     nil},
	{
		about: "example with clone",
		in:    []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1","metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": "acid", "clone": {"cluster": "team-batman"}}}`),
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
				Clone: &CloneDescription{
					ClusterName: "team-batman",
				},
				ClusterName: "testcluster1",
			},
			Error: "",
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":{"cluster":"team-batman"}},"status":{"PostgresClusterStatus":""}}`),
		err:     nil},
	{
		about: "standby example",
		in:    []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1","metadata": {"name": "acid-testcluster1"}, "spec": {"teamId": "acid", "standby": {"s3_wal_path": "s3://custom/path/to/bucket/"}}}`),
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
				StandbyCluster: &StandbyDescription{
					S3WalPath: "s3://custom/path/to/bucket/",
				},
				ClusterName: "testcluster1",
			},
			Error: "",
		},
		marshal: []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster1","creationTimestamp":null},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"standby":{"s3_wal_path":"s3://custom/path/to/bucket/"}},"status":{"PostgresClusterStatus":""}}`),
		err:     nil},
	{
		about:   "expect error on malformatted JSON",
		in:      []byte(`{"kind": "Postgresql","apiVersion": "acid.zalan.do/v1"`),
		out:     Postgresql{},
		marshal: []byte{},
		err:     errors.New("unexpected end of JSON input")},
	{
		about:   "expect error on JSON with field's value malformatted",
		in:      []byte(`{"kind":"Postgresql","apiVersion":"acid.zalan.do/v1","metadata":{"name":"acid-testcluster","creationTimestamp":qaz},"spec":{"postgresql":{"version":"","parameters":null},"volume":{"size":"","storageClass":""},"patroni":{"initdb":null,"pg_hba":null,"ttl":0,"loop_wait":0,"retry_timeout":0,"maximum_lag_on_failover":0,"slots":null},"resources":{"requests":{"cpu":"","memory":""},"limits":{"cpu":"","memory":""}},"teamId":"acid","allowedSourceRanges":null,"numberOfInstances":0,"users":null,"clone":null},"status":{"PostgresClusterStatus":"Invalid"}}`),
		out:     Postgresql{},
		marshal: []byte{},
		err:     errors.New("invalid character 'q' looking for beginning of value"),
	},
}

var postgresqlList = []struct {
	about string
	in    []byte
	out   PostgresqlList
	err   error
}{
	{"expect success", []byte(`{"apiVersion":"v1","items":[{"apiVersion":"acid.zalan.do/v1","kind":"Postgresql","metadata":{"labels":{"team":"acid"},"name":"acid-testcluster42","namespace":"default","resourceVersion":"30446957","selfLink":"/apis/acid.zalan.do/v1/namespaces/default/postgresqls/acid-testcluster42","uid":"857cd208-33dc-11e7-b20a-0699041e4b03"},"spec":{"allowedSourceRanges":["185.85.220.0/22"],"numberOfInstances":1,"postgresql":{"version":"9.6"},"teamId":"acid","volume":{"size":"10Gi"}},"status":{"PostgresClusterStatus":"Running"}}],"kind":"List","metadata":{},"resourceVersion":"","selfLink":""}`),
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
	{"expect error on malformatted JSON", []byte(`{"apiVersion":"v1","items":[{"apiVersion":"acid.zalan.do/v1","kind":"Postgresql","metadata":{"labels":{"team":"acid"},"name":"acid-testcluster42","namespace"`),
		PostgresqlList{},
		errors.New("unexpected end of JSON input")}}

var podAnnotations = []struct {
	about       string
	in          []byte
	annotations map[string]string
	err         error
}{{
	about: "common annotations",
	in: []byte(`{
		"kind": "Postgresql",
		"apiVersion": "acid.zalan.do/v1",
		"metadata": {
			"name": "acid-testcluster1"
		},
		"spec": {
			"podAnnotations": {
				"foo": "bar"
			},
			"teamId": "acid",
			"clone": {
				"cluster": "team-batman"
			}
		}
	}`),
	annotations: map[string]string{"foo": "bar"},
	err:         nil},
}

var serviceAnnotations = []struct {
	about       string
	in          []byte
	annotations map[string]string
	err         error
}{
	{
		about: "common single annotation",
		in: []byte(`{
			"kind": "Postgresql",
			"apiVersion": "acid.zalan.do/v1",
			"metadata": {
				"name": "acid-testcluster1"
			},
			"spec": {
				"serviceAnnotations": {
					"foo": "bar"
				},
				"teamId": "acid",
				"clone": {
					"cluster": "team-batman"
				}
			}
		}`),
		annotations: map[string]string{"foo": "bar"},
		err:         nil,
	},
	{
		about: "common two annotations",
		in: []byte(`{
			"kind": "Postgresql",
			"apiVersion": "acid.zalan.do/v1",
			"metadata": {
				"name": "acid-testcluster1"
			},
			"spec": {
				"serviceAnnotations": {
					"foo": "bar",
					"post": "gres"
				},
				"teamId": "acid",
				"clone": {
					"cluster": "team-batman"
				}
			}
		}`),
		annotations: map[string]string{"foo": "bar", "post": "gres"},
		err:         nil,
	},
}

func mustParseTime(s string) metav1.Time {
	v, err := time.Parse("15:04", s)
	if err != nil {
		panic(err)
	}

	return metav1.Time{Time: v.UTC()}
}

func TestParseTime(t *testing.T) {
	for _, tt := range parseTimeTests {
		t.Run(tt.about, func(t *testing.T) {
			aTime, err := parseTime(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("ParseTime expected error: %v, got: %v", tt.err, err)
				}
				return
			} else if tt.err != nil {
				t.Errorf("Expected error: %v", tt.err)
			}

			if aTime != tt.out {
				t.Errorf("Expected time: %v, got: %v", tt.out, aTime)
			}
		})
	}
}

func TestWeekdayTime(t *testing.T) {
	for _, tt := range parseWeekdayTests {
		t.Run(tt.about, func(t *testing.T) {
			aTime, err := parseWeekday(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("ParseWeekday expected error: %v, got: %v", tt.err, err)
				}
				return
			} else if tt.err != nil {
				t.Errorf("Expected error: %v", tt.err)
			}

			if aTime != tt.out {
				t.Errorf("Expected weekday: %v, got: %v", tt.out, aTime)
			}
		})
	}
}

func TestPodAnnotations(t *testing.T) {
	for _, tt := range podAnnotations {
		t.Run(tt.about, func(t *testing.T) {
			var cluster Postgresql
			err := cluster.UnmarshalJSON(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("Unable to marshal cluster with podAnnotations: expected %v got %v", tt.err, err)
				}
				return
			}
			for k, v := range cluster.Spec.PodAnnotations {
				found, expected := v, tt.annotations[k]
				if found != expected {
					t.Errorf("Didn't find correct value for key %v in for podAnnotations: Expected %v found %v", k, expected, found)
				}
			}
		})
	}
}

func TestServiceAnnotations(t *testing.T) {
	for _, tt := range serviceAnnotations {
		t.Run(tt.about, func(t *testing.T) {
			var cluster Postgresql
			err := cluster.UnmarshalJSON(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("Unable to marshal cluster with serviceAnnotations: expected %v got %v", tt.err, err)
				}
				return
			}
			for k, v := range cluster.Spec.ServiceAnnotations {
				found, expected := v, tt.annotations[k]
				if found != expected {
					t.Errorf("Didn't find correct value for key %v in for serviceAnnotations: Expected %v found %v", k, expected, found)
				}
			}
		})
	}
}

func TestClusterName(t *testing.T) {
	for _, tt := range clusterNames {
		t.Run(tt.about, func(t *testing.T) {
			name, err := extractClusterName(tt.in, tt.inTeam)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("extractClusterName expected error: %v, got: %v", tt.err, err)
				}
				return
			} else if tt.err != nil {
				t.Errorf("Expected error: %v", tt.err)
			}
			if name != tt.clusterName {
				t.Errorf("Expected cluserName: %q, got: %q", tt.clusterName, name)
			}
		})
	}
}

func TestCloneClusterDescription(t *testing.T) {
	for _, tt := range cloneClusterDescriptions {
		t.Run(tt.about, func(t *testing.T) {
			if err := validateCloneClusterDescription(tt.in); err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("testCloneClusterDescription expected error: %v, got: %v", tt.err, err)
				}
			} else if tt.err != nil {
				t.Errorf("Expected error: %v", tt.err)
			}
		})
	}
}

func TestUnmarshalMaintenanceWindow(t *testing.T) {
	for _, tt := range maintenanceWindows {
		t.Run(tt.about, func(t *testing.T) {
			var m MaintenanceWindow
			err := m.UnmarshalJSON(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("MaintenanceWindow unmarshal expected error: %v, got %v", tt.err, err)
				}
				return
			} else if tt.err != nil {
				t.Errorf("Expected error: %v", tt.err)
			}

			if !reflect.DeepEqual(m, tt.out) {
				t.Errorf("Expected maintenance window: %#v, got: %#v", tt.out, m)
			}
		})
	}
}

func TestMarshalMaintenanceWindow(t *testing.T) {
	for _, tt := range maintenanceWindows {
		t.Run(tt.about, func(t *testing.T) {
			if tt.err != nil {
				return
			}

			s, err := tt.out.MarshalJSON()
			if err != nil {
				t.Errorf("Marshal Error: %v", err)
			}

			if !bytes.Equal(s, tt.in) {
				t.Errorf("Expected Marshal: %q, got: %q", string(tt.in), string(s))
			}
		})
	}
}

func TestUnmarshalPostgresStatus(t *testing.T) {
	for _, tt := range postgresStatus {
		t.Run(tt.about, func(t *testing.T) {

			var ps PostgresStatus
			err := ps.UnmarshalJSON(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("CR status unmarshal expected error: %v, got %v", tt.err, err)
				}
				return
			}

			if !reflect.DeepEqual(ps, tt.out) {
				t.Errorf("Expected status: %#v, got: %#v", tt.out, ps)
			}
		})
	}
}

func TestPostgresUnmarshal(t *testing.T) {
	for _, tt := range unmarshalCluster {
		t.Run(tt.about, func(t *testing.T) {
			var cluster Postgresql
			err := cluster.UnmarshalJSON(tt.in)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("Unmarshal expected error: %v, got: %v", tt.err, err)
				}
				return
			} else if tt.err != nil {
				t.Errorf("Expected error: %v", tt.err)
			}

			if !reflect.DeepEqual(cluster, tt.out) {
				t.Errorf("Expected Postgresql: %#v, got %#v", tt.out, cluster)
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	for _, tt := range unmarshalCluster {
		t.Run(tt.about, func(t *testing.T) {

			if tt.err != nil {
				return
			}

			// Unmarshal and marshal example to capture api changes
			var cluster Postgresql
			err := cluster.UnmarshalJSON(tt.marshal)
			if err != nil {
				if tt.err == nil || err.Error() != tt.err.Error() {
					t.Errorf("Backwards compatibility unmarshal expected error: %v, got: %v", tt.err, err)
				}
				return
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
		})
	}
}

func TestPostgresMeta(t *testing.T) {
	for _, tt := range unmarshalCluster {
		t.Run(tt.about, func(t *testing.T) {

			if a := tt.out.GetObjectKind(); a != &tt.out.TypeMeta {
				t.Errorf("GetObjectKindMeta \nexpected: %v, \ngot:       %v", tt.out.TypeMeta, a)
			}

			if a := tt.out.GetObjectMeta(); reflect.DeepEqual(a, tt.out.ObjectMeta) {
				t.Errorf("GetObjectMeta \nexpected: %v, \ngot:       %v", tt.out.ObjectMeta, a)
			}
		})
	}
}

func TestPostgresListMeta(t *testing.T) {
	for _, tt := range postgresqlList {
		t.Run(tt.about, func(t *testing.T) {
			if tt.err != nil {
				return
			}

			if a := tt.out.GetObjectKind(); a != &tt.out.TypeMeta {
				t.Errorf("GetObjectKindMeta expected: %v, got: %v", tt.out.TypeMeta, a)
			}

			if a := tt.out.GetListMeta(); reflect.DeepEqual(a, tt.out.ListMeta) {
				t.Errorf("GetObjectMeta expected: %v, got: %v", tt.out.ListMeta, a)
			}

			return
		})
	}
}

func TestPostgresqlClone(t *testing.T) {
	for _, tt := range unmarshalCluster {
		t.Run(tt.about, func(t *testing.T) {
			cp := &tt.out
			cp.Error = ""
			clone := cp.Clone()
			if !reflect.DeepEqual(clone, cp) {
				t.Errorf("TestPostgresqlClone expected: \n%#v\n, got \n%#v", cp, clone)
			}
		})
	}
}
