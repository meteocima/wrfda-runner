package runner

import (
	"fmt"
	"sync"
	"time"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/connection"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/v2/conf"
	"github.com/meteocima/wrfda-runner/v2/folders"
)

func buildDADirInDomain(vs *ctx.Context, start, end time.Time, step, domain int, host string, mainHost bool) {
	if vs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	// prepare da dir
	daDir := folders.DAWorkDir(start, domain, step)
	daDir.Host = host
	vs.LogInfo("build wrfda work dir for cycle %d, domain %d on `%s`", step, domain, daDir.String())

	vs.MkDir(daDir)
	if mainHost {
		if domain == 1 {
			// domain 1 in every step of assimilation receives boundaries from WPS or from 'inputs' directory.
			vs.LogInfo("Copy wrfbdy_d01_da%02d to %s", step, host)
			vs.Copy(
				folders.InputsDir(start).Join("wrfbdy_d01_da%02d", step),
				daDir.Join("wrfbdy_d01"),
			)
			vs.LogInfo("Copy done")
		}

		if step == 1 {
			vs.LogInfo("Copy wrfbdy_d01_da%02d to %s", domain, host)

			// first step of assimilation receives fg input from WPS or from 'inputs' directory.
			vs.Copy(
				folders.InputsDir(start).Join("wrfinput_d%02d", domain),
				daDir.Join("fg"),
			)
			vs.LogInfo("Copy done")
		} else {
			// the others steps receives input from the WRF run
			// of previous step.
			prevHour := assimDate.Hour() - 3
			if prevHour < 0 {
				prevHour += 24
			}

			previousStep := folders.WRFWorkDir(start, step-1)
			vs.LogInfo("Copy wrfvar_input_d%02d to %s", domain, host)
			vs.Copy(
				previousStep.Join("wrfvar_input_d%02d", domain),
				daDir.Join("fg"),
			)
			vs.LogInfo("Copy done")

		}
	}
	// build namelist for wrfda
	conf.RenderNameList(
		vs,
		fmt.Sprintf("namelist.d%02d.wrfda", domain),
		daDir.Join("namelist.input"),
		namelist.Args{
			Start: assimDate,
			End:   end,
		},
	)

	conf.RenderNameList(
		vs,
		"parame.in",
		daDir.Join("parame.in"),
		namelist.Args{
			Start: start,
			End:   end,
		},
	)

	wrfdaPrg := folders.Cfg.WRFDAPrg
	wrfdaPrg.Host = host
	matrixDir := folders.Cfg.CovarMatrixesDir
	matrixDir.Host = host

	// link files from WRFDA build directory
	vs.Link(wrfdaPrg.Join("var/build/da_wrfvar.exe"), daDir.Join("da_wrfvar.exe"))
	vs.Link(wrfdaPrg.Join("var/run/VARBC.in"), daDir.Join("VARBC.in"))
	vs.Link(wrfdaPrg.Join("run/LANDUSE.TBL"), daDir.Join("LANDUSE.TBL"))
	vs.Link(wrfdaPrg.Join("var/build/da_update_bc.exe"), daDir.Join("da_update_bc.exe"))

	var season string
	// we use an approximation
	// to calculate season
	switch start.Month() {
	case 12, 1, 2:
		season = "winter"
	case 3, 4, 5:
		season = "spring"
	case 6, 7, 8:
		season = "summer"
	case 9, 10, 11:
		season = "fall"
	}

	// link covariance matrixes
	vs.Link(matrixDir.Join("%s/be_d%02d", season, domain), daDir.Join("be.dat"))

	// link observations
	vs.Link(folders.RadarObsForDate(start, step, host), daDir.Join("ob.radar"))
	vs.Link(folders.StationsObsForDate(start, step, host), daDir.Join("ob.ascii"))
}

func runDAStepInDomain(vs *ctx.Context, start time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}
	vs.LogInfo("run wrfda for cycle %d, domain %d", step, domain)

	daDir := folders.DAWorkDir(start, domain, step)

	logFile := daDir.Join("rsl.out.0000")
	vs.Exec(
		vpath.New("simulation", "mpirun"),
		[]string{"-n", conf.Config.Procs.WrfdaProcCount, "./da_wrfvar.exe"},
		&connection.RunOptions{
			OutFromLog: &logFile,
			Cwd:        daDir,
			Env:        conf.Config.Env.ToSlice(),
		},
	)

	if domain == 1 {
		vs.Exec(daDir.Join("./da_update_bc.exe"), []string{}, &connection.RunOptions{
			Cwd: daDir,
		})
	}
}

// RunDAStep ...
func RunDAStep(vs *ctx.Context, start time.Time, step int) {
	if vs.Err != nil {
		return
	}
	domainCount := ReadDomainCount(vs, conf.DAPhase)
	allSteps := sync.WaitGroup{}
	allSteps.Add(domainCount)
	for domain := 1; domain <= domainCount; domain++ {
		//go func(domain int) {
		runDAStepInDomain(vs, start, step, domain)
		allSteps.Done()
		//}(domain)
	}
	allSteps.Wait()
}

// BuildDAStepDir ...
func BuildDAStepDir(vs *ctx.Context, start, end time.Time, step int, host string, mainHost bool) {
	if vs.Err != nil {
		return
	}
	domainCount := ReadDomainCount(vs, conf.DAPhase)

	alldone := sync.WaitGroup{}
	alldone.Add(domainCount)

	for domain := 1; domain <= domainCount; domain++ {
		go func(domain int) {
			buildDADirInDomain(vs, start, end, step, domain, host, mainHost)
			alldone.Done()
		}(domain)
	}
	alldone.Wait()
}
