package wrftasks

import (
	"fmt"
	"time"

	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/tasks"
	"github.com/meteocima/wrfda-runner/v2/folders"
	"github.com/meteocima/wrfda-runner/v2/runner"
)

// NewWRFTask ...
func NewWRFTask(startDate time.Time) *tasks.Task {
	dtPart := startDate.Format("2006010215")
	endDate := startDate.Add(48 * time.Hour)

	tskID := fmt.Sprintf("WRF-%s", dtPart)
	tsk := tasks.New(tskID, func(vs *ctx.Context) error {
		wrfDir := folders.WRFWorkDir(startDate, 3)

		if vs.Exists(wrfDir) {
			return fmt.Errorf("working directory `%s` already exists for WRF main run for date %s", wrfDir, dtPart)
		}

		runner.BuildWRFDir(vs, startDate, endDate, 3)
		runner.RunWRFStep(vs, startDate, 3)

		return nil
	})
	tsk.Description = fmt.Sprintf("WRF main run for date %s", dtPart)
	return tsk
}
