package runner

import (
	"time"

	"github.com/meteocima/wrfda-runner/common"
	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/folders"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"

	"github.com/meteocima/virtual-server/connection"
)

func BuildNamelistForReal(vs *ctx.Context, start, end time.Time, step int) {
	assimStartDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wpsDir := folders.WPSWorkDir(start)

	// build namelist for real
	conf.RenderNameList(
		vs,
		"namelist.real",
		wpsDir.Join("namelist.input"),
		namelist.Args{
			Start: assimStartDate,
			End:   end,
		},
	)
}

// RunReal ...
func RunReal(vs *ctx.Context, startDate time.Time, step int, phase conf.RunPhase) {
	domainCount := ReadDomainCount(vs, phase)
	if vs.Err != nil {
		return
	}
	wpsDir := folders.WPSWorkDir(startDate)

	vs.LogInfo("real for cycle %d", step)

	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", common.RealProcCount, "./real.exe"},
		connection.RunOptions{
			OutFromLog: wpsDir.Join("rsl.out.0000"),
			Cwd:        wpsDir,
		},
	)

	indir := folders.InputsDir(startDate)
	vs.MkDir(indir)

	vs.Copy(wpsDir.Join("wrfbdy_d01"), indir.Join("wrfbdy_d01_da%02d", step))

	if step != 1 {
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		vs.Copy(
			wpsDir.Join("wrfinput_d%02d", domain),
			indir.Join("wrfinput_d%02d", domain),
		)
	}

}

func BuildWPSDir(vs *ctx.Context, start, end time.Time, ds conf.InputDataset) {
	if vs.Err != nil {
		return
	}
	wpsDir := folders.WPSWorkDir(start)
	vs.LogInfo("Build WPS work directory on `%s`", wpsDir.String())
	wpsPrg := folders.Cfg.WPSPrg
	wrfPrgStep := folders.Cfg.WRFAssStepPrg

	vs.MkDir(wpsDir)

	// build namelist for wrf
	conf.RenderNameList(
		vs,
		"namelist.wps",
		wpsDir.Join("namelist.wps"),
		namelist.Args{
			Start: start.Add(-6 * time.Hour),
			End:   end,
		},
	)

	vs.Link(wpsPrg.Join("link_grib.csh"), wpsDir.Join("link_grib.csh"))
	vs.Link(wpsPrg.Join("ungrib.exe"), wpsDir.Join("ungrib.exe"))
	vs.Link(wpsPrg.Join("metgrid.exe"), wpsDir.Join("metgrid.exe"))
	vs.Link(wpsPrg.Join("util/avg_tsfc.exe"), wpsDir.Join("avg_tsfc.exe"))
	vs.Link(wrfPrgStep.Join("run/real.exe"), wpsDir.Join("real.exe"))
	vs.Link(wpsPrg.Join("geogrid.exe"), wpsDir.Join("geogrid.exe"))

	if ds == conf.GFS {
		vs.Link(wpsPrg.Join("ungrib/Variable_Tables/Vtable.GFS"), wpsDir.Join("Vtable"))
	} else if ds == conf.IFS {
		vs.Link(wpsPrg.Join("ungrib/Variable_Tables/Vtable.ECMWF"), wpsDir.Join("Vtable"))
	}
}

func RunWPS(vs *ctx.Context, start, end time.Time) {
	if vs.Err != nil {
		return
	}

	vs.LogInfo("WPS pre-process for date %s", start.Format("2006020115"))

	wpsDir := folders.WPSWorkDir(start)

	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", common.GeogridProcCount, "./geogrid.exe"},
		connection.RunOptions{
			OutFromLog: wpsDir.Join("geogrid.log.0000"),
			Cwd:        wpsDir,
		},
	)

	vs.Exec(wpsDir.Join("./link_grib.csh"), []string{"../gfs/*"}, connection.RunOptions{
		Cwd: wpsDir,
	})

	vs.Exec(wpsDir.Join("./ungrib.exe"), []string{}, connection.RunOptions{

		Cwd: wpsDir,
	})

	if end.Sub(start) > 24*time.Hour {
		vs.Exec(wpsDir.Join("./avg_tsfc.exe"), []string{"../gfs/*"}, connection.RunOptions{

			Cwd: wpsDir,
		})

	}

	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", common.MetgridProcCount, "./metgrid.exe"},
		connection.RunOptions{
			OutFromLog: wpsDir.Join("metgrid.log.0000"),
			Cwd:        wpsDir,
		},
	)

}

/*
// NewWpsTask ...
func NewWpsTask(startDate time.Time) *WpsTask {
	dtPart := startDate.Format("200602011504")
	tskID := FindTaskID(fmt.Sprintf("wps-%s", dtPart))
	tsk := &WpsTask{
		SimpleTask: SimpleTask{
			id:          tskID,
			description: fmt.Sprintf("WPS preprocessing for date `%s`:%s", dtPart, tskID),
		},
		startDate: startDate,
	}

	tsk.infoLog = OpenTaskLog(tsk.InfoLogFilePath())
	tsk.detailedLog = OpenTaskLog(tsk.DetailedLogFilePath())

	tasks[tskID] = tsk
	return tsk
}

// Run ...
func (tsk *WpsTask) Run() error {
	err := runner.RemoveRunFolder(tsk.startDate, conf.Workdir, tsk.infoLog, tsk.infoLog)
	if err != nil {
		return err
	}

	return runner.Run(tsk.startDate, tsk.startDate, conf.Workdir, runnerConf.WPSPhase, runnerConf.GFS, tsk.infoLog, tsk.detailedLog)
}
*/
