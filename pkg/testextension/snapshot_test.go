package testextension

import (
	"testing"

	"github.com/G-Research/yunikorn-core/pkg/scheduler/objects"
	"github.com/G-Research/yunikorn-core/pkg/webservice"
	"gotest.tools/v3/assert"
)

func TestApplicationSnapshot(t *testing.T) {
	app := objects.NewTestApplication(t)
	want := webservice.GetApplicationDAO(app, app.GetApplicationSummary("rmid"))
	got := app.DAO()
	assert.DeepEqual(t, want, got)
}
