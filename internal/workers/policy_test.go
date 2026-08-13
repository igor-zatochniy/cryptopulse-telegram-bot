package workers

import "testing"

func TestLockConnectionBudgetCoversEveryLongLivedHolder(t *testing.T) {
	requiredConnections := MaxTelegramUpdateWorkerCount +
		NotificationWorkerCount +
		TelegramReplyWorkerCount +
		CronLockConnectionReserve +
		LockPoolOperationalReserve
	if requiredConnections != DatabaseLockPoolMaxOpenConnections {
		t.Fatalf(
			"lock connection budget = %d, required = %d",
			DatabaseLockPoolMaxOpenConnections,
			requiredConnections,
		)
	}
}
