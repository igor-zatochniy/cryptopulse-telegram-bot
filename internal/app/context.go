package app

import (
	"context"
	"time"
)

// finalizationContext зберігає context values, але дає durable finalizer-у
// коротке вікно для запису результату після скасування worker context.
func finalizationContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}
