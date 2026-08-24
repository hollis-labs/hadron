package runtime

import (
	"context"
	"time"

	"github.com/hollis-labs/hadron/workflow/values"
	"github.com/hollis-labs/hadron/workflow/verification"
)

// PersistVerificationForTest exposes the package's durable replay contract to
// black-box tests without widening the production API.
func PersistVerificationForTest(ctx context.Context, store StateStore, retention RetentionHook, redactor *values.Redactor, attempt AttemptID, report verification.Report, at time.Time) (VerificationRecord, error) {
	return persistVerification(ctx, store, retention, redactor, attempt, report, at)
}
