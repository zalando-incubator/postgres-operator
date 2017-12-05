package cluster

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/zalando-incubator/postgres-operator/pkg/spec"
	"github.com/zalando-incubator/postgres-operator/pkg/util/config"
	"github.com/zalando-incubator/postgres-operator/pkg/util/k8sutil"
	"github.com/zalando-incubator/postgres-operator/pkg/util/teams"
	"reflect"
	"testing"
)

var logger = logrus.New().WithField("test", "cluster")
var cl = New(Config{OpConfig: config.Config{ProtectedRoles: []string{"admin"},
	Auth: config.Auth{SuperUsername: "postgres",
		ReplicationUsername: "standby"}}},
	k8sutil.KubernetesClient{}, spec.Postgresql{}, logger)

func TestInitRobotUsers(t *testing.T) {
	testName := "TestInitRobotUsers"
	tests := []struct {
		manifestUsers map[string]spec.UserFlags
		infraRoles    map[string]spec.PgUser
		result        map[string]spec.PgUser
		err           error
	}{
		{
			manifestUsers: map[string]spec.UserFlags{"foo": {"superuser", "createdb"}},
			infraRoles:    map[string]spec.PgUser{"foo": {Name: "foo", Password: "bar"}},
			result: map[string]spec.PgUser{"foo": {Name: "foo", Password: "bar",
				Flags: []string{"CREATEDB", "LOGIN", "SUPERUSER"}}},
			err: nil,
		},
		{
			manifestUsers: map[string]spec.UserFlags{"!fooBar": {"superuser", "createdb"}},
			err:           fmt.Errorf(`invalid username: "!fooBar"`),
		},
		{
			manifestUsers: map[string]spec.UserFlags{"foobar": {"!superuser", "createdb"}},
			err: fmt.Errorf(`invalid flags for user "foobar": ` +
				`user flag "!superuser" is not alphanumeric`),
		},
		{
			manifestUsers: map[string]spec.UserFlags{"foobar": {"superuser1", "createdb"}},
			err: fmt.Errorf(`invalid flags for user "foobar": ` +
				`user flag "SUPERUSER1" is not valid`),
		},
		{
			manifestUsers: map[string]spec.UserFlags{"foobar": {"inherit", "noinherit"}},
			err: fmt.Errorf(`invalid flags for user "foobar": ` +
				`conflicting user flags: "NOINHERIT" and "INHERIT"`),
		},
		{
			manifestUsers: map[string]spec.UserFlags{"admin": {"superuser"}, "postgres": {"createdb"}},
			infraRoles:    map[string]spec.PgUser{},
			result:        map[string]spec.PgUser{},
			err:           nil,
		},
	}
	for _, tt := range tests {
		cl.Spec.Users = tt.manifestUsers
		cl.pgUsers = tt.infraRoles
		if err := cl.initRobotUsers(); err != nil {
			if tt.err == nil {
				t.Errorf("%s got an unexpected error: %v", testName, err)
			}
			if err.Error() != tt.err.Error() {
				t.Errorf("%s expected error %v, got %v", testName, tt.err, err)
			}
		} else {
			if !reflect.DeepEqual(cl.pgUsers, tt.result) {
				t.Errorf("%s expected: %#v, got %#v", testName, tt.result, cl.pgUsers)
			}
		}
	}
}

type mockOAuthTokenGetter struct {
}

func (m *mockOAuthTokenGetter) getOAuthToken() (string, error) {
	return "", nil
}

type mockTeamsAPIClient struct {
	members []string
}

func (m *mockTeamsAPIClient) TeamInfo(teamID, token string) (tm *teams.Team, err error) {
	return &teams.Team{Members: m.members}, nil
}

func (m *mockTeamsAPIClient) setMembers(members []string) {
	m.members = members
}

func TestInitHumanUsers(t *testing.T) {

	var mockTeamsAPI mockTeamsAPIClient
	cl.oauthTokenGetter = &mockOAuthTokenGetter{}
	cl.teamsAPIClient = &mockTeamsAPI
	testName := "TestInitHumanUsers"

	cl.OpConfig.EnableTeamSuperuser = true
	cl.OpConfig.EnableTeamsAPI = true
	cl.OpConfig.PamRoleName = "zalandos"
	cl.Spec.TeamID = "test"

	tests := []struct {
		existingRoles map[string]spec.PgUser
		teamRoles     []string
		result        map[string]spec.PgUser
	}{
		{
			existingRoles: map[string]spec.PgUser{"foo": {Name: "foo", Flags: []string{"NOLOGIN"}},
				"bar": {Name: "bar", Flags: []string{"NOLOGIN"}}},
			teamRoles: []string{"foo"},
			result: map[string]spec.PgUser{"foo": {Name: "foo", MemberOf: []string{cl.OpConfig.PamRoleName}, Flags: []string{"LOGIN", "SUPERUSER"}},
				"bar": {Name: "bar", Flags: []string{"NOLOGIN"}}},
		},
		{
			existingRoles: map[string]spec.PgUser{},
			teamRoles:     []string{"admin", "standby"},
			result:        map[string]spec.PgUser{},
		},
	}

	for _, tt := range tests {
		cl.pgUsers = tt.existingRoles
		mockTeamsAPI.setMembers(tt.teamRoles)
		if err := cl.initHumanUsers(); err != nil {
			t.Errorf("%s got an unexpected error %v", testName, err)
		}

		if !reflect.DeepEqual(cl.pgUsers, tt.result) {
			t.Errorf("%s expects %#v, got %#v", testName, tt.result, cl.pgUsers)
		}
	}
}
