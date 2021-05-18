package runner

import (
	"time"

	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/folders"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"

	"github.com/meteocima/virtual-server/connection"
)

// BuildNamelistForReal ...
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

	logFile := wpsDir.Join("rsl.out.0000")
	vs.Exec(
		vpath.New("simulation", "mpirun"),
		[]string{"-n", conf.Config.Procs.RealProcCount, "./real.exe"},
		&connection.RunOptions{
			OutFromLog: &logFile,
			Cwd:        wpsDir,
			Env:        conf.Config.Env.ToSlice(),
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

// BuildWPSDir ..
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

// RunWPS ...
func RunWPS(vs *ctx.Context, start, end time.Time) {
	if vs.Err != nil {
		return
	}

	vs.LogInfo("Start WPS pre-process for date %s", start.Format("2006020115"))

	wpsDir := folders.WPSWorkDir(start)

	logFile := wpsDir.Join("geogrid.log.0000")
	vs.Exec(
		vpath.New("simulation", "mpirun"),
		[]string{"-n", conf.Config.Procs.GeogridProcCount, "./geogrid.exe"},
		&connection.RunOptions{
			OutFromLog: &logFile,
			Cwd:        wpsDir,
			Env:        conf.Config.Env.ToSlice(),
		},
	)

	vs.Exec(wpsDir.Join("./link_grib.csh"), []string{"../gfs/*"}, &connection.RunOptions{
		Cwd: wpsDir,
	})

	vs.Exec(wpsDir.Join("./ungrib.exe"), []string{}, &connection.RunOptions{
		Cwd: wpsDir,
	})

	if end.Sub(start) > 24*time.Hour {
		vs.Exec(wpsDir.Join("./avg_tsfc.exe"), []string{}, &connection.RunOptions{

			Cwd: wpsDir,
		})

	}

	logFile2 := wpsDir.Join("metgrid.log.0000")
	vs.Exec(
		vpath.New("simulation", "mpirun"),
		[]string{"-n", conf.Config.Procs.MetgridProcCount, "./metgrid.exe"},
		&connection.RunOptions{
			OutFromLog: &logFile2,
			Cwd:        wpsDir,
			Env:        conf.Config.Env.ToSlice(),
		},
	)

}
