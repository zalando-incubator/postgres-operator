package cluster

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
)

func TestIsTransientDbError(t *testing.T) {
	readOnly := &pq.Error{Code: pqerror.ReadOnlySQLTransaction, Message: "cannot execute ALTER ROLE in a read-only transaction"}

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"read-only transaction", readOnly, true},
		{"database starting up", &pq.Error{Code: pqerror.CannotConnectNow}, true},
		{"admin shutdown", &pq.Error{Code: pqerror.AdminShutdown}, true},
		{"crash shutdown", &pq.Error{Code: pqerror.CrashShutdown}, true},
		{"connection failure", &pq.Error{Code: pqerror.ConnectionFailure}, true},
		{"bad connection", driver.ErrBadConn, true},
		{"connection closed", io.ErrUnexpectedEOF, true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"wrapped read-only transaction", fmt.Errorf("could not check if database is in recovery: %w", readOnly), true},
		{"read-only transaction among other errors", fmt.Errorf("could not execute sync requests for users: %w",
			errors.Join(errors.New("could not rename user"), fmt.Errorf("could not alter user %q: %w", "foo", readOnly))), true},
		{"duplicate role", &pq.Error{Code: pqerror.DuplicateObject}, false},
		{"authentication failure", &pq.Error{Code: pqerror.InvalidPassword}, false},
		{"other error", errors.New("could not rename user"), false},
		{"no error", nil, false},
	}

	for _, tt := range tests {
		if got := isTransientDbError(tt.err); got != tt.expected {
			t.Errorf("%s: expected %v, got %v", tt.name, tt.expected, got)
		}
	}
}
