//go:build integration

package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/igor-zatochniy/cryptopulse-telegram-bot/internal/storage"
)

func TestIntegrationAdvisoryLockDiscardsUncertainSession(t *testing.T) {
	for _, lockType := range []string{"chat", "cron", "migration"} {
		for _, failure := range []string{"acquire", "unlock", "unlock_false"} {
			t.Run(lockType+"/"+failure, func(t *testing.T) {
				db := setupIntegrationDB(t)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				db.SetMaxOpenConns(2)
				db.SetMaxIdleConns(2)

				observer, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer observer.Close()
				conn, err := db.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()

				var pid int
				if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
					t.Fatal(err)
				}
				installAdvisoryLockFailure(t, ctx, conn, failure)

				// Помилка SQL не закриває сесію: lock має залишитися до нашого cleanup.
				const probeKey int64 = 123456789
				if _, err := conn.ExecContext(ctx, `SELECT pg_catalog.pg_advisory_lock($1)`, probeKey); err != nil {
					t.Fatal(err)
				}
				query := `SELECT pg_advisory_unlock($1)`
				if failure == "acquire" {
					query = `SELECT pg_advisory_lock($1)`
				}
				_, probeErr := conn.ExecContext(ctx, query, probeKey)
				if failure != "unlock_false" {
					assertAdvisoryLockTestError(t, probeErr)
				}
				var alive int
				if err := conn.QueryRowContext(ctx, `SELECT 1`).Scan(&alive); err != nil {
					t.Fatalf("fault unexpectedly closed the physical session: %v", err)
				}
				var held bool
				if err := observer.QueryRowContext(ctx,
					`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory')`, pid,
				).Scan(&held); err != nil || !held {
					t.Fatalf("expected a live session still holding its lock: held=%v err=%v", held, err)
				}
				if _, err := conn.ExecContext(ctx, `SELECT pg_catalog.pg_advisory_unlock_all()`); err != nil {
					t.Fatal(err)
				}
				// Залишаємо у pool лише підготовлену сесію; observer утримує другу.
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}

				app := &App{db: db}
				switch lockType {
				case "chat":
					locked, key, err := app.acquireTelegramChatAdvisoryLock(ctx, 812345)
					if failure == "acquire" {
						assertAdvisoryLockTestError(t, err)
					} else {
						if err != nil {
							t.Fatal(err)
						}
						releaseTelegramChatAdvisoryLock(ctx, locked, key)
					}
				case "cron":
					locked, acquired, err := app.acquireCronAdvisoryLock(ctx)
					if failure == "acquire" {
						assertAdvisoryLockTestError(t, err)
					} else {
						if err != nil || !acquired {
							t.Fatalf("acquire cron lock: acquired=%v err=%v", acquired, err)
						}
						releaseCronAdvisoryLock(ctx, locked)
					}
				case "migration":
					// Goose потребує ще одного з'єднання, крім lock та observer.
					db.SetMaxOpenConns(3)
					_, err := storage.ApplyMigrations(ctx, db)
					if failure == "acquire" {
						assertAdvisoryLockTestError(t, err)
					} else if err != nil {
						t.Fatalf("apply migrations: %v", err)
					}
				}

				// Backend завершується асинхронно після закриття клієнтського socket.
				deadline := time.Now().Add(2 * time.Second)
				for {
					var remaining int
					if err := observer.QueryRowContext(ctx, `SELECT
						(SELECT COUNT(*) FROM pg_stat_activity WHERE pid = $1) +
						(SELECT COUNT(*) FROM pg_locks WHERE pid = $1 AND locktype = 'advisory')`, pid,
					).Scan(&remaining); err != nil {
						t.Fatal(err)
					}
					if remaining == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("physical session %d or its advisory locks survived cleanup", pid)
					}
					time.Sleep(10 * time.Millisecond)
				}

				// SQL-функція записує ключ саме останньої спроби, включно з migration lock.
				var key int64
				if err := observer.QueryRowContext(ctx, `SELECT last_value FROM test_lock_key`).Scan(&key); err != nil {
					t.Fatal(err)
				}
				var acquired bool
				if err := observer.QueryRowContext(ctx, `SELECT pg_catalog.pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil || !acquired {
					t.Fatalf("second session cannot acquire released key: acquired=%v err=%v", acquired, err)
				}
				if _, err := observer.ExecContext(ctx, `SELECT pg_catalog.pg_advisory_unlock($1)`, key); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func assertAdvisoryLockTestError(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "P0001" {
		t.Fatalf("expected nonfatal PostgreSQL test error P0001, got %v", err)
	}
}

func installAdvisoryLockFailure(t *testing.T, ctx context.Context, conn *sql.Conn, failure string) {
	t.Helper()
	statements := []string{
		`CREATE SCHEMA lock_fault`,
		`CREATE SEQUENCE public.test_lock_key MINVALUE -9223372036854775808 MAXVALUE 9223372036854775807 START 1`,
		`SET search_path = lock_fault, public, pg_catalog`,
	}
	functions := map[string]string{"pg_advisory_unlock": "boolean"}
	if failure == "acquire" {
		functions = map[string]string{"pg_advisory_lock": "void", "pg_try_advisory_lock": "boolean"}
	}
	for name, resultType := range functions {
		body := `PERFORM pg_catalog.setval('public.test_lock_key', key);`
		if failure == "acquire" {
			body += ` PERFORM pg_catalog.pg_advisory_lock(key);`
		}
		if failure == "unlock_false" {
			body += ` RETURN false;`
		} else {
			body += ` RAISE EXCEPTION 'injected advisory lock failure';`
		}
		statements = append(statements, fmt.Sprintf(
			`CREATE FUNCTION lock_fault.%s(key bigint) RETURNS %s AS $$ BEGIN %s END $$ LANGUAGE plpgsql`,
			name, resultType, body,
		))
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatalf("install advisory lock failure: %v", err)
		}
	}
}
