package execution

import (
	"testing"
	"time"

	"github.com/hollis-labs/hadron/internal/blueprint"
)

func TestCalcRetryDelay_Fixed(t *testing.T) {
	step := blueprint.Step{
		RetryDelaySecs: 2,
		RetryBackoff:   "fixed",
	}
	for attempt := 1; attempt <= 3; attempt++ {
		d := calcRetryDelay(step, attempt)
		if d != 2*time.Second {
			t.Fatalf("attempt %d: expected 2s, got %s", attempt, d)
		}
	}
}

func TestCalcRetryDelay_FixedDefault(t *testing.T) {
	// Empty backoff string defaults to fixed.
	step := blueprint.Step{
		RetryDelaySecs: 3,
	}
	d := calcRetryDelay(step, 1)
	if d != 3*time.Second {
		t.Fatalf("expected 3s, got %s", d)
	}
}

func TestCalcRetryDelay_Exponential(t *testing.T) {
	step := blueprint.Step{
		RetryDelaySecs: 1,
		RetryBackoff:   "exponential",
	}
	// attempt 1: base * 2^0 = 1s
	// attempt 2: base * 2^1 = 2s
	// attempt 3: base * 2^2 = 4s
	expected := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
	for i, want := range expected {
		attempt := i + 1
		got := calcRetryDelay(step, attempt)
		if got != want {
			t.Fatalf("attempt %d: expected %s, got %s", attempt, want, got)
		}
	}
}

func TestCalcRetryDelay_Linear(t *testing.T) {
	step := blueprint.Step{
		RetryDelaySecs: 2,
		RetryBackoff:   "linear",
	}
	// attempt 1: base * 1 = 2s
	// attempt 2: base * 2 = 4s
	// attempt 3: base * 3 = 6s
	expected := []time.Duration{2 * time.Second, 4 * time.Second, 6 * time.Second}
	for i, want := range expected {
		attempt := i + 1
		got := calcRetryDelay(step, attempt)
		if got != want {
			t.Fatalf("attempt %d: expected %s, got %s", attempt, want, got)
		}
	}
}

func TestCalcRetryDelay_ExponentialWithCap(t *testing.T) {
	step := blueprint.Step{
		RetryDelaySecs: 1,
		RetryBackoff:   "exponential",
		RetryMaxDelay:  3,
	}
	// attempt 1: 1s (1*2^0)
	// attempt 2: 2s (1*2^1)
	// attempt 3: 3s (capped from 4s)
	expected := []time.Duration{1 * time.Second, 2 * time.Second, 3 * time.Second}
	for i, want := range expected {
		attempt := i + 1
		got := calcRetryDelay(step, attempt)
		if got != want {
			t.Fatalf("attempt %d: expected %s, got %s", attempt, want, got)
		}
	}
}

func TestCalcRetryDelay_LinearWithCap(t *testing.T) {
	step := blueprint.Step{
		RetryDelaySecs: 2,
		RetryBackoff:   "linear",
		RetryMaxDelay:  5,
	}
	// attempt 1: 2s
	// attempt 2: 4s
	// attempt 3: 5s (capped from 6s)
	expected := []time.Duration{2 * time.Second, 4 * time.Second, 5 * time.Second}
	for i, want := range expected {
		attempt := i + 1
		got := calcRetryDelay(step, attempt)
		if got != want {
			t.Fatalf("attempt %d: expected %s, got %s", attempt, want, got)
		}
	}
}

func TestCalcRetryDelay_ZeroBase(t *testing.T) {
	step := blueprint.Step{
		RetryDelaySecs: 0,
		RetryBackoff:   "exponential",
	}
	d := calcRetryDelay(step, 1)
	if d != 0 {
		t.Fatalf("expected 0 with zero base delay, got %s", d)
	}
}
