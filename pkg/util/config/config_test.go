package config

import (
	"fmt"
	"reflect"
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
)

var getMapPairsFromStringTest = []struct {
	in       string
	expected []string
	err      error
}{
	{"log_statement:all, work_mem:'4GB'", []string{"log_statement:all", "work_mem:'4GB'"}, nil},
	{`log_statement:none, search_path:'"$user", public'`, []string{"log_statement:none", `search_path:'"$user", public'`}, nil},
	{`search_path:'"$user"`, nil, fmt.Errorf("unmatched quote starting at position 13")},
	{"", []string{""}, nil},
	{",,log_statement:all	,", []string{"", "", "log_statement:all", ""}, nil},
}

var MarshalJSONTest = []struct {
	in 		StringTemplate
	out 	[]byte
	err		error	
}{
	{StringTemplate("test"),[]byte{0x22, 0x74, 0x65, 0x73, 0x74, 0x22},nil},
}

var FormatTest = []struct {
	in			StringTemplate
	formatter	[]string
	out			string
}{
	{StringTemplate("{co}ed"),[]string{"co","test"},"tested",},
}

var test_string = "bar"
var test_uint = uint(123)
var test_number = 123
var test_bool = true
var test_float = 32.5
var test_slice = make([]string, 3)
var test_map = make(map[string]int)
var test_map1 = make(map[string]int)
var test_map2 = make(map[string]int)
var test_time = time.Duration(1000)

var ProcessFieldTest = []struct {
	inA			string
	inB			reflect.Value
	err			error
}	{	
	{"foo",reflect.ValueOf(&test_string),nil,},
	{"123",reflect.ValueOf(&test_number),nil,},
	{"123",reflect.ValueOf(&test_uint),nil,},
	{"false",reflect.ValueOf(&test_bool),nil,},
	{"23.5",reflect.ValueOf(&test_float),nil,},
	{"foo,bar",reflect.ValueOf(&test_slice),nil,},
	{"'foo':123,'bar':123",reflect.ValueOf(&test_map),nil,},
	{"1000µs",reflect.ValueOf(&test_time),nil,},
	{"'foo','bar'",reflect.ValueOf(&test_map1),fmt.Errorf("invalid map item: \"'foo'\""),},
	{`search_path:'"$user"`,reflect.ValueOf(&test_map2),fmt.Errorf("could not split value \"search_path:'\\\"$user\\\"\" into map items: unmatched quote starting at position 13"),},
}

func TestProcessField(t *testing.T) {
	for _, tt:= range ProcessFieldTest {
		err := processField(tt.inA, tt.inB)
		if err != tt.err && ((err == nil || tt.err == nil) || (err.Error() != tt.err.Error())){
			t.Errorf("TestProcessField of string %s called with %s: expected error %#v, got %#v", tt.inA, tt.inB, tt.err, err)
		}
	}
}

func TestCopy(t *testing.T){
	cfg := Config{
		Resources : Resources{
			MinInstances : 1,
			MaxInstances : 2,
			ClusterLabels : make(map[string]string),
		},
		Auth : Auth{
			SuperUsername : "postgres",
		},
		Workers: 2,
		ConnectionPooler : ConnectionPooler {
			User : "pooler",
		},
	}
	cfg_copy := Copy(&cfg)
	assert.Equal(t, cfg_copy, cfg)
}

func TestValidate(t *testing.T) {
	no_of_conn_pool_instances := int32(2)
	cfg := Config{
		Resources : Resources{
			MinInstances : 1,
			MaxInstances : 2,
		},
		Auth : Auth{
			SuperUsername : "postgres",
		},
		Workers: 2,
		ConnectionPooler : ConnectionPooler {
			NumberOfInstances : &no_of_conn_pool_instances,
			User : "pooler",
		},
	}
	err := validate(&cfg)
	assert.NoError(t, err)

	cfg.Resources.MinInstances = 3
	err = validate(&cfg)
	assert.Equal(t, fmt.Errorf("minimum number of instances 3 is set higher than the maximum number 2"), err)

	cfg.Workers = 0
	err = validate(&cfg)
	assert.Equal(t, fmt.Errorf("number of workers should be higher than 0"), err)

	no_of_conn_pool_instances = int32(0)
	err = validate(&cfg)
	assert.Equal(t, fmt.Errorf("number of connection pooler instances should be higher than 1"), err)

	cfg.ConnectionPooler.User = "postgres"
	err = validate(&cfg)
	assert.Equal(t, fmt.Errorf("Connection pool user is not allowed to be the same as super user, username: postgres"), err)
}


func TestFormat(t *testing.T) {
	for _, tt:= range FormatTest {
		got := tt.in.Format(tt.formatter...)
		if !reflect.DeepEqual(got, tt.out){
			t.Errorf("Format of %s called with %s: expected %s, got %s", tt.in, tt.formatter, tt.out, got)
		}
	}
}

func TestMarshalJSON(t *testing.T){
	for _, tt := range MarshalJSONTest {
		got, _ := tt.in.MarshalJSON()
		if !reflect.DeepEqual(got, tt.out) {
			t.Errorf("MarshalJSON with %s: expected %#v, got %#v", tt.in, tt.out, got)
		}
	}	
}

func TestGetMapPairsFromString(t *testing.T) {
	for _, tt := range getMapPairsFromStringTest {
		got, err := getMapPairsFromString(tt.in)
		if err != tt.err && ((err == nil || tt.err == nil) || (err.Error() != tt.err.Error())) {
			t.Errorf("TestGetMapPairsFromString with %s: expected error: %#v, got %#v", tt.in, tt.err, err)
		}
		if !reflect.DeepEqual(got, tt.expected) {
			t.Errorf("TestGetMapPairsFromString with %s: expected %#v, got %#v", tt.in, tt.expected, got)
		}
	}
}
