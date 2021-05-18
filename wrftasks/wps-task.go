package wrftasks

import (
	"fmt"
	"time"

	"github.com/meteocima/wrfda-runner/v2/conf"
	"github.com/meteocima/wrfda-runner/v2/runner"

	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/tasks"
	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/v2/folders"
)

// NewWPSTask ...
func NewWPSTask(startDate time.Time) *tasks.Task {
	dtPart := startDate.Format("2006010215")

	tskID := fmt.Sprintf("WPS-%s", dtPart)
	tsk := tasks.New(tskID, func(vs *ctx.Context) error {
		wpsDir := folders.WPSWorkDir(startDate)
		if vs.Exists(wpsDir) {
			return fmt.Errorf("WPS working directory `%s` already exists", wpsDir)
		}

		endDate := startDate.Add(48 * time.Hour)

		workdirOnOrchestrator := folders.WorkdirForDate(startDate)
		//if !vs.Exists(workdirOnOrchestrator) {
		//	runner.BuildWorkdirForDate(vs, workdirOnOrchestrator, conf.WPSThenDAPhase, startDate, endDate)
		//}

		workdirOnSimulation := vpath.New("simulation", workdirOnOrchestrator.Path)
		if !vs.Exists(workdirOnSimulation) {
			runner.BuildWorkdirForDate(vs, workdirOnSimulation, conf.WPSThenDAPhase, startDate, endDate)
		}

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
