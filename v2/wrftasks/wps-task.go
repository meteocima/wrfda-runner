package wrftasks

/*
import (
	"fmt"
	"strings"
	"sync"
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
		//wpsDir := folders.WPSWorkDir(startDate)
		//if vs.Exists(wpsDir) {
		//	return fmt.Errorf("WPS working directory `%s` already exists", wpsDir)
		//}

		endDate := startDate.Add(48 * time.Hour)

		workdirOnOrchestrator := folders.WorkdirForDate(startDate)
		//if !vs.Exists(workdirOnOrchestrator) {
		//	runner.BuildWorkdirForDate(vs, workdirOnOrchestrator, conf.WPSThenDAPhase, startDate, endDate)
		//}

		hostsS, hasHosts := conf.Config.Env["I_MPI_HYDRA_HOSTS_GROUP"]

		hosts := strings.Split(hostsS, ",")

		if !hasHosts {
			hosts = append(hosts, "simulation")
		}

		alldone := sync.WaitGroup{}
		alldone.Add(len(hosts))
		for idx, host := range hosts {
			go func(host string, idx int) {
				workdirOnHost := vpath.New(host, workdirOnOrchestrator.Path)
				if !vs.Exists(workdirOnHost) {
					runner.BuildWorkdirForDate(vs, workdirOnHost, conf.WPSThenDAPhase, startDate, idx == 0)
				}
				alldone.Done()
			}(host, idx)
		}
		alldone.Wait()

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
*/
