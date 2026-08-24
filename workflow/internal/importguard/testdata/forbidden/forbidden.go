// Package forbidden is a negative test fixture for the workflow import guard.
// It lives under testdata so normal Go package discovery ignores it.
package forbidden

import _ "github.com/hollis-labs/hadron/internal/persistence"
