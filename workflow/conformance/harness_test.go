package conformance

import (
	"errors"
	"testing"
)

func TestCheckOutcomeFailureMessages(t *testing.T) {
	tests := []struct {
		name        string
		fixture     Fixture
		runErr      error
		want        string
		wantNoError bool
	}{
		{
			name: "expected pass succeeds",
			fixture: Fixture{
				Set:         GraphValidationFixtures,
				Name:        "minimal-pass",
				Expectation: ExpectPass,
			},
			wantNoError: true,
		},
		{
			name: "expected failure rejects",
			fixture: Fixture{
				Set:         GraphValidationFixtures,
				Name:        "minimal-fail",
				Expectation: ExpectFail,
			},
			runErr:      errors.New("fixture rejected"),
			wantNoError: true,
		},
		{
			name: "unexpected rejection",
			fixture: Fixture{
				Set:         GraphValidationFixtures,
				Name:        "minimal-pass",
				Expectation: ExpectPass,
			},
			runErr: errors.New("fixture rejected"),
			want:   "conformance compiler/graph-validation/minimal-pass: expected pass, got error: fixture rejected",
		},
		{
			name: "unexpected acceptance",
			fixture: Fixture{
				Set:         GraphValidationFixtures,
				Name:        "minimal-fail",
				Expectation: ExpectFail,
			},
			want: "conformance compiler/graph-validation/minimal-fail: expected failure, got success",
		},
		{
			name: "unknown expectation",
			fixture: Fixture{
				Set:         GraphValidationFixtures,
				Name:        "minimal-unknown",
				Expectation: "unknown",
			},
			want: "conformance compiler/graph-validation/minimal-unknown: unknown expectation \"unknown\"",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkOutcome("compiler", test.fixture, test.runErr)
			if test.wantNoError {
				if err != nil {
					t.Fatalf("checkOutcome() error = %v", err)
				}
				return
			}
			if err == nil || err.Error() != test.want {
				t.Fatalf("checkOutcome() error = %v, want %q", err, test.want)
			}
		})
	}
}
