package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestInvalidPostgresConfigurationDoesNotExposeCredentials(t *testing.T) {
	for name, dsn := range map[string]string{
		"query password":    "postgres://audit_user@127.0.0.1/audit?password=AUDIT_ONLY_PASSWORD&sslmode=invalid",
		"userinfo password": "postgres://audit_user:AUDIT_ONLY_PASSWORD@127.0.0.1:invalid/audit",
		"encoded password":  "postgres://audit_user@127.0.0.1/audit?password=AUDIT%5FONLY%5FPASSWORD&sslmode=invalid",
		"keyword password":  "host=127.0.0.1 user=audit_user password=AUDIT_ONLY_PASSWORD sslmode=invalid",
	} {
		t.Run(name, func(t *testing.T) {
			for _, open := range []func(context.Context, string) (*sql.DB, error){Open, OpenLockPool} {
				db, err := open(context.Background(), dsn)
				if db != nil {
					_ = db.Close()
					t.Fatal("unexpected connection pool")
				}
				if err == nil {
					t.Fatal("expected invalid configuration error")
				}
				var output bytes.Buffer
				slog.New(slog.NewJSONHandler(&output, nil)).Error("application stopped", "error", err)
				for _, secret := range []string{"AUDIT_ONLY_PASSWORD", "AUDIT%5FONLY%5FPASSWORD", "postgres://", "audit_user"} {
					if strings.Contains(output.String(), secret) {
						t.Fatalf("log contains connection configuration: %s", output.String())
					}
				}
				if !strings.Contains(err.Error(), "invalid PostgreSQL connection configuration") {
					t.Fatalf("missing safe diagnostic: %v", err)
				}
			}
		})
	}
}

func TestPostgresPoolErrorPreservesNonConfigurationErrors(t *testing.T) {
	err := postgresPoolError("ping", "query", context.DeadlineExceeded)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost error classification: %v", err)
	}
}
