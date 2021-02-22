package runner

import (
	"fmt"
	"sync"
	"time"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/connection"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/common"
	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/folders"
)

func buildDADirInDomain(vs *ctx.Context, start, end time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	// prepare da dir
	daDir := folders.DAWorkDir(start, domain, step)
	vs.LogInfo("build wrfda work dir for cycle %d, domain %d on `%s`", step, domain, daDir.String())

	vs.MkDir(daDir)

	if domain == 1 {
		// domain 1 in every step of assimilation receives boundaries from WPS or from 'inputs' directory.
		vs.Copy(
			folders.InputsDir(start).Join("wrfbdy_d01_da%02d", step),
			daDir.Join("wrfbdy_d01"),
		)
	}

	if step == 1 {
		// first step of assimilation receives fg input from WPS or from 'inputs' directory.
		vs.Copy(
			folders.InputsDir(start).Join("wrfinput_d%02d", domain),
			daDir.Join("fg"),
		)
	} else {
		// the others steps receives input from the WRF run
		// of previous step.
		prevHour := assimDate.Hour() - 3
		if prevHour < 0 {
			prevHour += 24
		}

		previousStep := folders.WRFWorkDir(start, step-1)

		vs.Copy(
			previousStep.Join("wrfvar_input_d%02d", domain),
			daDir.Join("fg"),
		)
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
	matrixDir := folders.Cfg.CovarMatrixesDir

	// link files from WRFDA build directory
	vs.Link(wrfdaPrg.Join("var/build/da_wrfvar.exe"), daDir.Join("da_wrfvar.exe"))
	vs.Link(wrfdaPrg.Join("var/run/VARBC.in"), daDir.Join("VARBC.in"))
	vs.Link(wrfdaPrg.Join("run/LANDUSE.TBL"), daDir.Join("LANDUSE.TBL"))
	vs.Link(wrfdaPrg.Join("var/build/da_update_bc.exe"), daDir.Join("da_update_bc.exe"))

	// link covariance matrixes
	vs.Link(matrixDir.Join("summer/be_2.5km_d%02d", domain), daDir.Join("be.dat"))

	// link observations

	vs.Link(folders.RadarObsForDate(start, step), daDir.Join("ob.radar"))
	vs.Link(folders.StationsObsForDate(start, step), daDir.Join("ob.ascii"))
}

func runDAStepInDomain(vs *ctx.Context, start time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}
	vs.LogInfo("run wrfda for cycle %d, domain %d", step, domain)

	daDir := folders.DAWorkDir(start, domain, step)

	logFile := daDir.Join("rsl.out.0000")
	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", common.WrfdaProcCount, "./da_wrfvar.exe"},
		&connection.RunOptions{
			OutFromLog: &logFile,
			Cwd:        daDir,
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
		go func(domain int) {
			runDAStepInDomain(vs, start, step, domain)
			allSteps.Done()
		}(domain)
	}
	allSteps.Wait()
}

// BuildDAStepDir ...
func BuildDAStepDir(vs *ctx.Context, start, end time.Time, step int) {
	if vs.Err != nil {
		return
	}
	domainCount := ReadDomainCount(vs, conf.DAPhase)

	for domain := 1; domain <= domainCount; domain++ {
		buildDADirInDomain(vs, start, end, step, domain)
	}

}
