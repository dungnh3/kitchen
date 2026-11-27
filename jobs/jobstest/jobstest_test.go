package jobstest_test

import (
	"testing"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/internal/drivertest"
	"github.com/dungnh3/kitchen/jobs/jobstest"
)

func TestConformance(t *testing.T) {
	drivertest.Run(t.Context(), t, func(*testing.T) jobs.Store { return jobstest.New() })
}
