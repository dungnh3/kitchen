package sdk_test

import (
	"os"
	"testing"
)

// unsetEnv removes n for the duration of the test (t.Setenv already
// registered the restore).
func unsetEnv(t *testing.T, n string) {
	t.Helper()
	if err := os.Unsetenv(n); err != nil {
		t.Fatal(err)
	}
}
