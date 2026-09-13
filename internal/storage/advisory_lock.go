package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

// DiscardConnection вилучає фізичне з'єднання з pool, якщо стан session lock невідомий.
func DiscardConnection(conn *sql.Conn) error {
	if conn == nil {
		return nil
	}

	// Close повертає сесію в pool; ErrBadConn натомість змушує database/sql закрити її.
	err := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return nil
	}
	return err
}

// ReleaseAdvisoryLock повертає сесію в pool лише після підтвердженого unlock.
func ReleaseAdvisoryLock(ctx context.Context, conn *sql.Conn, key int64) error {
	if conn == nil {
		return nil
	}

	var released bool
	err := conn.QueryRowContext(ctx, `SELECT pg_advisory_unlock($1)`, key).Scan(&released)
	if err == nil && !released {
		err = errors.New("advisory lock was not held by the session")
	}
	if err != nil {
		return errors.Join(err, DiscardConnection(conn))
	}
	return conn.Close()
}
