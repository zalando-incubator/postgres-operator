package cluster

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	acidv1 "github.com/zalando/postgres-operator/v2/pkg/apis/acid.zalan.do/v1"
)

type mockConnector struct {
	rows          driver.Rows
	execErrAt     int
	execErr       error
	failQuery     string
	failQueryLeft int
	execCalls     int
	execQueries   []string
	beginCalls    int
	commitCalls   int
	rollbackCalls int
}

func (m *mockConnector) Connect(context.Context) (driver.Conn, error) {
	return &mockConn{connector: m, rows: m.rows}, nil
}

func (m *mockConnector) Driver() driver.Driver {
	return nil
}

type mockConn struct {
	connector *mockConnector
	rows      driver.Rows
}

func (m *mockConn) Prepare(string) (driver.Stmt, error) {
	return nil, nil
}

func (m *mockConn) Begin() (driver.Tx, error) {
	m.connector.beginCalls++
	return &mockTx{connector: m.connector}, nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if rows, ok := m.rows.(*fakeRows); ok {
		return &fakeRows{columns: rows.columns, rows: rows.rows, rowErrs: rows.rowErrs}, nil
	}
	return m.rows, nil
}

func (m *mockConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	m.connector.execCalls++
	m.connector.execQueries = append(m.connector.execQueries, query)
	if m.connector.failQueryLeft > 0 && strings.Contains(query, m.connector.failQuery) {
		m.connector.failQueryLeft--
		return nil, errors.New("simulated ACL failure")
	}
	if m.connector.execErrAt > 0 && m.connector.execCalls == m.connector.execErrAt {
		return nil, m.connector.execErr
	}
	return driver.RowsAffected(0), nil
}

type mockTx struct {
	connector *mockConnector
}

func (m *mockTx) Commit() error {
	m.connector.commitCalls++
	return nil
}

func (m *mockTx) Rollback() error {
	m.connector.rollbackCalls++
	return nil
}

type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	rowErrs map[int]error
	pos     int
}

func (f *fakeRows) Columns() []string {
	if len(f.columns) > 0 {
		return f.columns
	}
	return []string{"datname", "owner"}
}

func (f *fakeRows) Close() error {
	return nil
}

func (f *fakeRows) Next(dest []driver.Value) error {
	if f.pos >= len(f.rows) {
		return io.EOF
	}

	if err := f.rowErrs[f.pos]; err != nil {
		f.pos++
		return err
	}

	copy(dest, f.rows[f.pos])
	f.pos++
	return nil
}

func TestExecCreateDatabaseSchemaTransaction(t *testing.T) {
	t.Run("success commits", func(t *testing.T) {
		connector := &mockConnector{}
		cluster, db := newClusterWithMockDB(t, connector)
		defer db.Close()

		err := cluster.execCreateDatabaseSchema(
			"app", "data", "app_owner", "app_owner",
			createDatabaseSchemaSQL, "creating database schema", "create database schema",
		)

		require.NoError(t, err)
		require.Equal(t, 2, connector.execCalls)
		require.Equal(t, 1, connector.beginCalls)
		require.Equal(t, 1, connector.commitCalls)
		require.Equal(t, 0, connector.rollbackCalls)
	})

	t.Run("privilege failure rolls back", func(t *testing.T) {
		privilegeErr := errors.New("simulated privilege failure")
		connector := &mockConnector{
			execErrAt: 2,
			execErr:   privilegeErr,
		}
		cluster, db := newClusterWithMockDB(t, connector)
		defer db.Close()

		err := cluster.execCreateDatabaseSchema(
			"app", "data", "app_owner", "app_owner",
			createDatabaseSchemaSQL, "creating database schema", "create database schema",
		)

		require.Error(t, err)
		require.Contains(t, err.Error(), privilegeErr.Error())
		require.Equal(t, 2, connector.execCalls)
		require.Equal(t, 1, connector.beginCalls)
		require.Equal(t, 0, connector.commitCalls)
		require.Equal(t, 1, connector.rollbackCalls)
	})
}

func newClusterWithMockDB(t *testing.T, connector *mockConnector) (*Cluster, *sql.DB) {
	t.Helper()

	db := sql.OpenDB(connector)
	cluster := &Cluster{
		pgDb:   db,
		logger: logrus.New().WithField("test", t.Name()),
		Postgresql: acidv1.Postgresql{
			Spec: acidv1.PostgresSpec{
				PreparedDatabases: map[string]acidv1.PreparedDatabase{
					"app": {DefaultUsers: false},
				},
			},
		},
	}
	return cluster, db
}
