package wrftasks

import (
	"fmt"
	"time"

	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/runner"

	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/tasks"
	"github.com/meteocima/wrfda-runner/folders"
)

// NewWPSTask ...
func NewWPSTask(startDate time.Time) *tasks.Task {
	dtPart := startDate.Format("2006010215")

	tskID := fmt.Sprintf("WPS-%s", dtPart)
	tsk := tasks.New(tskID, func(vs *ctx.Context) error {
		dtWorkdir := folders.WorkdirForDate(startDate)
		wpsDir := folders.WPSWorkDir(startDate)
		if vs.Exists(wpsDir) {
			return fmt.Errorf("WPS working directory `%s` already exists", wpsDir)
		}

		if !vs.Exists(dtWorkdir) {
			runner.BuildWorkdirForDate(vs, conf.WPSThenDAPhase, startDate)
		}

		endDate := startDate.Add(48 * time.Hour)
		runner.BuildWPSDir(vs, startDate, endDate, conf.GFS)
		runner.RunWPS(vs, startDate, endDate)
		for step := 1; step <= 3; step++ {
			runner.BuildNamelistForReal(vs, startDate, endDate, step)
			runner.RunReal(vs, startDate, step, conf.WPSPhase)
		}
		return nil
	})
	tsk.Description = fmt.Sprintf("WPS preprocessing for date %s", dtPart)
	return tsk
}
