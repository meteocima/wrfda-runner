package runner

import (
	"sync"
	"time"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/connection"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"

	"github.com/meteocima/wrfda-runner/v2/conf"
	"github.com/meteocima/wrfda-runner/v2/folders"
)

// RunWRFStep ...
func RunWRFStep(vs *ctx.Context, start time.Time, step int) {
	if vs.Err != nil {
		return
	}

	vs.LogInfo("wrf cycle %d", step)

	wrfDir := folders.WRFWorkDir(start, step)

	logFile := wrfDir.Join("rsl.out.0000")
	vs.Exec(
		vpath.New(wrfDir.Host, "mpirun"),
		[]string{"-n", conf.Config.Procs.WrfstepProcCount, "./wrf.exe"},
		&connection.RunOptions{
			OutFromLog: &logFile,
			Cwd:        wrfDir,
			Env:        conf.Config.Env.ToSlice(),
		},
	)

}

// BuildWRFDir ...
func BuildWRFDir(vs *ctx.Context, start, end time.Time, step int, host string) {
	if vs.Err != nil {
		return
	}
	wrfDir := folders.WRFWorkDir(start, step)
	wrfDir.Host = host
	vs.LogInfo("build wrf work dir for cycle %d on `%s`", step, wrfDir.String())

	wrfPrg := folders.Cfg.WRFAssStepPrg
	wrfPrg.Host = host
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

	vs.LogInfo("Copy wrfbdy_d01 to %s\n", host)
	vs.Copy(daBdy, wrfDir.Join("wrfbdy_d01"))
	vs.LogInfo("Copy done\n")

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

	domainCount := ReadDomainCount(vs, conf.DAPhase)

	alldone := sync.WaitGroup{}
	alldone.Add(domainCount)

	// prev da results
	for domain := 1; domain <= domainCount; domain++ {
		go func(domain int) {
			daDir := folders.DAWorkDir(start, domain, step)
			vs.LogInfo("Copy wrfinput_d%02d to %s\n", domain, host)
			vs.Copy(daDir.Join("wrfvar_output"), wrfDir.Join("wrfinput_d%02d", domain))
			vs.LogInfo("Copy done\n")
			alldone.Done()
		}(domain)
	}
	alldone.Wait()
}
