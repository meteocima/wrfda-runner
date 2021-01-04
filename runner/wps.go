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

func buildNamelistForReal(vs *ctx.Context, start, end time.Time, step int) {
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

func runReal(vs *ctx.Context, startDate time.Time, step int, domainCount int) {
	if vs.Err != nil {
		return
	}
	wpsDir := folders.WPSWorkDir(startDate)

	vs.LogF("START REAL\n")

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
		vs.LogF("COMPLETED REAL\n")
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		vs.Copy(
			wpsDir.Join("wrfinput_d%02d", domain),
			indir.Join("wrfinput_d%02d", domain),
		)
	}

	vs.LogF("COMPLETED REAL\n")

}

func buildWPSDir(vs *ctx.Context, start, end time.Time, ds conf.InputDataset) {
	if vs.Err != nil {
		return
	}

	wpsDir := folders.WPSWorkDir(start)
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

func runWPS(vs *ctx.Context, start, end time.Time) {
	if vs.Err != nil {
		return
	}

	vs.LogF("START WPS\n")
	wpsDir := folders.WPSWorkDir(start)

	vs.LogF("Running geogrid")
	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", common.GeogridProcCount, "./geogrid.exe"},
		connection.RunOptions{
			OutFromLog: wpsDir.Join("geogrid.log.0000"),
			Cwd:        wpsDir,
		},
	)
	if vs.Err != nil {
		return
	}

	vs.LogF("Running linkgrib ../gfs/*")
	vs.Exec(wpsDir.Join("./link_grib.csh"), []string{"../gfs/*"}, connection.RunOptions{

		Cwd: wpsDir,
	})
	if vs.Err != nil {
		return
	}

	vs.LogF("Running ungrib")
	vs.Exec(wpsDir.Join("./ungrib.exe"), []string{}, connection.RunOptions{

		Cwd: wpsDir,
	})
	if vs.Err != nil {
		return
	}

	if end.Sub(start) > 24*time.Hour {
		vs.LogF("Running avg_tsfc")
		vs.Exec(wpsDir.Join("./avg_tsfc.exe"), []string{"../gfs/*"}, connection.RunOptions{

			Cwd: wpsDir,
		})
		if vs.Err != nil {
			return
		}
	}

	vs.LogF("Running metgrid")
	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", common.MetgridProcCount, "./metgrid.exe"},
		connection.RunOptions{
			OutFromLog: wpsDir.Join("metgrid.log.0000"),
			Cwd:        wpsDir,
		},
	)

	if vs.Err != nil {
		return
	}

	vs.LogF("COMPLETED WPS\n")

}
