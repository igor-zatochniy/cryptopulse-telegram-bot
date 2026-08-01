package metrics

import (
	"database/sql"
	"testing"

	dto "github.com/prometheus/client_model/go"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestObserveDBPoolPublishesConfiguredLimit(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://unused")
	if err != nil {
		t.Fatalf("open test database handle: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(7)

	ObserveDBPool("test", db)

	metric := &dto.Metric{}
	if err := DBPoolConnections.WithLabelValues("test", "max_open").Write(metric); err != nil {
		t.Fatalf("read pool metric: %v", err)
	}
	if got := metric.GetGauge().GetValue(); got != 7 {
		t.Fatalf("max open connections metric = %.0f, want 7", got)
	}
}
