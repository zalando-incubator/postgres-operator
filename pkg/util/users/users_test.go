package users

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
	"github.com/zalando/postgres-operator/v2/pkg/spec"
)

// readOnlyConn fails every statement like a database that is still in recovery
type readOnlyConn struct{}

func (readOnlyConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (readOnlyConn) Close() error                        { return nil }
func (readOnlyConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }
func (readOnlyConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, &pq.Error{Severity: "ERROR", Code: pqerror.ReadOnlySQLTransaction,
		Message: "cannot execute statement in a read-only transaction"}
}

type readOnlyDriver struct{}

func (readOnlyDriver) Open(string) (driver.Conn, error) { return readOnlyConn{}, nil }

func init() {
	sql.Register("readonly", readOnlyDriver{})
}

func TestExecuteSyncRequestsKeepsErrors(t *testing.T) {
	db, err := sql.Open("readonly", "")
	if err != nil {
		t.Fatalf("could not open database: %v", err)
	}
	defer db.Close()

	strategy := DefaultUserSyncStrategy{PasswordEncryption: "md5", RoleDeletionSuffix: "_deleted"}
	requests := []spec.PgSyncUserRequest{
		{Kind: spec.PGSyncUserAdd, User: spec.PgUser{Name: "foo"}},
		{Kind: spec.PGsyncUserAlter, User: spec.PgUser{Name: "bar", Password: "secret"}},
	}

	err = strategy.ExecuteSyncRequests(requests, db)
	if err == nil {
		t.Fatal("expected sync requests to fail")
	}

	expectedPrefix := `could not execute sync requests for users: could not create user "foo": `
	if !strings.HasPrefix(err.Error(), expectedPrefix) {
		t.Errorf("expected error to start with %q, got %q", expectedPrefix, err.Error())
	}
	if !strings.Contains(err.Error(), `', 'could not alter user "bar": `) {
		t.Errorf("expected errors of all requests in the message, got %q", err.Error())
	}
	// callers need the database error, e.g. to retry while the database is read-only
	if pq.As(err, pqerror.ReadOnlySQLTransaction) == nil {
		t.Errorf("expected read-only transaction error to be wrapped, got %q", err.Error())
	}
}
