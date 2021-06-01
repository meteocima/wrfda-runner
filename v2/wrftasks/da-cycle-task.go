package wrftasks

import (
	"fmt"
	"strings"
	"time"

	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/tasks"
	"github.com/meteocima/wrfda-runner/v2/conf"
	"github.com/meteocima/wrfda-runner/v2/folders"
	"github.com/meteocima/wrfda-runner/v2/runner"
)

func checkDirExists(vs *ctx.Context, startDate time.Time, cycle int) error {
	domainCount := runner.ReadDomainCount(vs, conf.DAPhase)

	for domain := 1; domain <= domainCount; domain++ {
		daDir := folders.DAWorkDir(startDate, domain, cycle)

		if vs.Exists(daDir) {
			return fmt.Errorf("working directory `%s` already exists for DA cycle %d, domain %d", daDir, cycle, domain)
		}
	}

	if cycle < 3 {
		return nil
	}

	wrfDir := folders.WRFWorkDir(startDate, cycle)

	if vs.Exists(wrfDir) {
		return fmt.Errorf("working directory `%s` already exists for WRF cycle %d", wrfDir, cycle)
	}

	return nil
}

// NewDACycleTask ...
func NewDACycleTask(startDate time.Time, cycle int) *tasks.Task {
	dtPart := startDate.Format("2006010215")
	endDate := startDate.Add(48 * time.Hour)

	tskID := fmt.Sprintf("WRFDA-%s-CYCLE-%d", dtPart, cycle)
	tsk := tasks.New(tskID, func(vs *ctx.Context) error {

		if err := checkDirExists(vs, startDate, cycle); err != nil {
			return err
		}

		hostsS, hasHosts := conf.Config.Env["I_MPI_HYDRA_HOSTS_GROUP"]

		hosts := strings.Split(hostsS, ",")

		if !hasHosts {
			hosts = append(hosts, "simulation")
		}

		for _, host := range hosts {
			runner.BuildDAStepDir(vs, startDate, endDate, cycle, host)
		}

		runner.RunDAStep(vs, startDate, cycle)

		if cycle >= 3 {
			return nil
		}

		for _, host := range hosts {
			runner.BuildWRFDir(vs, startDate, endDate, cycle, host)
		}
		runner.RunWRFStep(vs, startDate, cycle)

		return nil
	})
	tsk.Description = fmt.Sprintf("WRFDA cycle %d of date %s", cycle, dtPart)
	return tsk
}
