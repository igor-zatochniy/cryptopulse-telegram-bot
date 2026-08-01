// Package storage створює та налаштовує PostgreSQL connection pool.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/workers"
)

// Open підключається до PostgreSQL і повертає готовий до використання pool.
func Open(ctx context.Context, databaseURL string) (*sql.DB, error) {
	return openPool(
		ctx,
		databaseURL,
		"query",
		workers.DatabaseQueryPoolMaxOpenConnections,
		workers.DatabaseQueryPoolMaxIdleConnections,
	)
}

// OpenLockPool створює окремий pool для session-level advisory locks.
func OpenLockPool(ctx context.Context, databaseURL string) (*sql.DB, error) {
	return openPool(
		ctx,
		databaseURL,
		"lock",
		workers.DatabaseLockPoolMaxOpenConnections,
		workers.DatabaseLockPoolMaxIdleConnections,
	)
}

func openPool(
	ctx context.Context,
	databaseURL string,
	poolName string,
	maxOpenConnections int,
	maxIdleConnections int,
) (*sql.DB, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres %s pool: %w", poolName, err)
	}

	db.SetMaxOpenConns(maxOpenConnections)
	db.SetMaxIdleConns(maxIdleConnections)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres %s pool: %w", poolName, err)
	}

	return db, nil
}
