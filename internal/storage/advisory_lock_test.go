package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
)

func TestReleaseAdvisoryLockConnectionReuse(t *testing.T) {
	queryErr := errors.New("nonfatal unlock error")
	for _, tc := range []struct {
		name      string
		value     driver.Value
		queryErr  error
		canceled  bool
		wantError bool
	}{
		{name: "confirmed unlock", value: true},
		{name: "unlock error", queryErr: queryErr, wantError: true},
		{name: "lock not held", value: false, wantError: true},
		{name: "invalid result", value: "invalid boolean", wantError: true},
		{name: "canceled release", value: true, canceled: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connector := &lockTestConnector{value: tc.value, queryErr: tc.queryErr}
			db := sql.OpenDB(connector)
			defer db.Close()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			err = ReleaseAdvisoryLock(ctx, conn, 42)
			if (err != nil) != tc.wantError {
				t.Fatalf("release error = %v, wantError = %v", err, tc.wantError)
			}
			if tc.queryErr != nil && !errors.Is(err, tc.queryErr) {
				t.Fatalf("original SQL error not preserved: %v", err)
			}
			if tc.canceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("context cancellation not preserved: %v", err)
			}
			var wantClosed int32
			if tc.wantError {
				wantClosed = 1
			}
			if got := connector.closed.Load(); got != wantClosed {
				t.Fatalf("physical closes = %d, want %d", got, wantClosed)
			}
			if err := DiscardConnection(conn); err != nil {
				t.Fatalf("already closed handle: %v", err)
			}
			next, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			if got := connector.opened.Load(); got != 1+wantClosed {
				t.Fatalf("physical opens = %d, want %d", got, 1+wantClosed)
			}
		})
	}
}

func TestDiscardConnectionClosesPhysicalSession(t *testing.T) {
	connector := &lockTestConnector{}
	db := sql.OpenDB(connector)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := DiscardConnection(conn); err != nil {
		t.Fatal(err)
	}
	if connector.closed.Load() != 1 {
		t.Fatal("driver connection was not closed")
	}
	if err := conn.PingContext(context.Background()); !errors.Is(err, sql.ErrConnDone) {
		t.Fatalf("discarded handle still usable: %v", err)
	}
	if err := DiscardConnection(nil); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseAdvisoryLock(context.Background(), nil, 42); err != nil {
		t.Fatal(err)
	}
}

type lockTestConnector struct {
	value    driver.Value
	queryErr error
	opened   atomic.Int32
	closed   atomic.Int32
}

func (c *lockTestConnector) Connect(context.Context) (driver.Conn, error) {
	c.opened.Add(1)
	return &lockTestConn{connector: c}, nil
}

func (c *lockTestConnector) Driver() driver.Driver { return c }

func (c *lockTestConnector) Open(string) (driver.Conn, error) {
	return c.Connect(context.Background())
}

type lockTestConn struct{ connector *lockTestConnector }

func (c *lockTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}

func (c *lockTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

func (c *lockTestConn) Close() error {
	c.connector.closed.Add(1)
	return nil
}

func (c *lockTestConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.connector.queryErr != nil {
		return nil, c.connector.queryErr
	}
	return &lockTestRows{value: c.connector.value}, nil
}

type lockTestRows struct {
	value driver.Value
	done  bool
}

func (r *lockTestRows) Columns() []string { return []string{"pg_advisory_unlock"} }
func (r *lockTestRows) Close() error      { return nil }
func (r *lockTestRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = r.value
	r.done = true
	return nil
}
