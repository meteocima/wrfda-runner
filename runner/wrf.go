package runner

import (
	"time"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/connection"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/common"
	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/folders"
)

func runWRFStep(vs *ctx.Context, start time.Time, step int) {
	if vs.Err != nil {
		return
	}

	defer vs.SetTask("wrf cycle %d", step)()

	wrfDir := folders.WRFWorkDir(start, step)

	vs.Exec(
		vpath.New(wrfDir.Host, "mpirun"),
		[]string{"-n", common.WrfstepProcCount, "./wrf.exe"},
		connection.RunOptions{OutFromLog: wrfDir.Join("rsl.out.0000"),
			Cwd: wrfDir,
		},
	)

}
func buildWRFDir(vs *ctx.Context, start, end time.Time, step int, domainCount int) {
	if vs.Err != nil {
		return
	}
	wrfDir := folders.WRFWorkDir(start, step)
	defer vs.SetTask("build wrf work dir for cycle %d on `%s`", step, wrfDir.String())()

	wrfPrg := folders.Cfg.WRFAssStepPrg
	nameListName := "namelist.step.wrf"

	dtStart := start.Add(3 * time.Duration(step-3) * time.Hour)
	dtEnd := dtStart.Add(3 * time.Hour)

	if step == 3 {
		dtEnd = end
		wrfPrg = folders.Cfg.WRFMainRunPrg
		nameListName = "namelist.run.wrf"
	}

	vs.MkDir(wrfDir)

	// boundary from same cycle da dir for domain 1
	daBdy := folders.DAWorkDir(start, 1, step).Join("wrfbdy_d01")
	vs.Copy(daBdy, wrfDir.Join("wrfbdy_d01"))

	// build namelist for wrf
	conf.RenderNameList(
		vs,
		nameListName,
		wrfDir.Join("namelist.input"),
		namelist.Args{
			Start: dtStart,
			End:   dtEnd,
		},
	)

	wrfvar := ""
	switch step {
	case 1:
		wrfvar = "wrf_var.txt.wrf_01"
	case 2:
		wrfvar = "wrf_var.txt.wrf_02"
	case 3:
		wrfvar = "wrf_var.txt.wrf_03"
	}

	vs.Copy(
		conf.NamelistFile(wrfvar),
		wrfDir.Join("wrf_var.txt"),
	)

	vs.Link(wrfPrg.Join("main/wrf.exe"), wrfDir.Join("wrf.exe"))
	vs.Link(wrfPrg.Join("run/LANDUSE.TBL"), wrfDir.Join("LANDUSE.TBL"))
	vs.Link(wrfPrg.Join("run/ozone_plev.formatted"), wrfDir.Join("ozone_plev.formatted"))
	vs.Link(wrfPrg.Join("run/ozone_lat.formatted"), wrfDir.Join("ozone_lat.formatted"))
	vs.Link(wrfPrg.Join("run/ozone.formatted"), wrfDir.Join("ozone.formatted"))
	vs.Link(wrfPrg.Join("run/RRTMG_LW_DATA"), wrfDir.Join("RRTMG_LW_DATA"))
	vs.Link(wrfPrg.Join("run/RRTMG_SW_DATA"), wrfDir.Join("RRTMG_SW_DATA"))
	vs.Link(wrfPrg.Join("run/VEGPARM.TBL"), wrfDir.Join("VEGPARM.TBL"))
	vs.Link(wrfPrg.Join("run/SOILPARM.TBL"), wrfDir.Join("SOILPARM.TBL"))
	vs.Link(wrfPrg.Join("run/GENPARM.TBL"), wrfDir.Join("GENPARM.TBL"))

	// prev da results
	for domain := 1; domain <= domainCount; domain++ {
		daDir := folders.DAWorkDir(start, domain, step)
		vs.Link(daDir.Join("wrfvar_output"), wrfDir.Join("wrfinput_d%02d", domain))
	}
}
