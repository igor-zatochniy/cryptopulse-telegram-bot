package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/pressly/goose/v3"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/migrations"
)

const migrationAdvisoryLockKey int64 = 0x43727970746f

// ApplyMigrations serializes schema upgrades across replicas and applies the
// immutable migration set embedded in the current application build.
func ApplyMigrations(ctx context.Context, db *sql.DB) (int, error) {
	migrationCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	lockConn, err := db.Conn(migrationCtx)
	if err != nil {
		return 0, fmt.Errorf("open migration lock connection: %w", err)
	}

	if _, err := lockConn.ExecContext(
		migrationCtx,
		`SELECT pg_advisory_lock($1)`,
		migrationAdvisoryLockKey,
	); err != nil {
		return 0, fmt.Errorf("acquire migration advisory lock: %w", errors.Join(err, DiscardConnection(lockConn)))
	}
	defer releaseMigrationLock(lockConn)

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.Files)
	if err != nil {
		return 0, fmt.Errorf("initialize migration provider: %w", err)
	}

	results, err := provider.Up(migrationCtx)
	if err != nil {
		return 0, fmt.Errorf("apply database migrations: %w", err)
	}
	return len(results), nil
}

func releaseMigrationLock(conn *sql.Conn) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := ReleaseAdvisoryLock(releaseCtx, conn, migrationAdvisoryLockKey); err != nil {
		slog.Error("failed to release migration advisory lock", "error", err)
	}
}
