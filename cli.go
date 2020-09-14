package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	namelist "github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/wrfassim/fsutil"
)

// Namelists ...
var Namelists *FileSystem

func renderNameList(fs *fsutil.Transaction, source string, target fsutil.Path, args namelist.Args) {
	if fs.Err != nil {
		return
	}

	tmplFile, err := Namelists.Open("/" + source)
	if err != nil {
		log.Fatalf("open template: %s", err.Error())
	}

	targetNamelistFile, err := os.OpenFile(
		path.Join(fs.Root.String(), target.String()),
		os.O_CREATE|os.O_WRONLY,
		os.FileMode(0644),
	)
	defer targetNamelistFile.Close()

	if err != nil {
		log.Fatalf("open namelist.real: %s", err.Error())
	}

	tmpl := namelist.Tmpl{}
	tmpl.ReadTemplateFrom(tmplFile)
	tmpl.RenderTo(args, targetNamelistFile)
}

var wrfdaPrg = fsutil.Path("wrfdaprg")
var wrfPrgStep = fsutil.Path("wrfprgstep")
var wrfPrgMainRun = fsutil.Path("wrfprgrun")
var matrixDir = fsutil.Path("matrix")
var wpsDir = fsutil.Path("wps")
var observationsDir = fsutil.Path("observations")

func buildWRFDir(fs *fsutil.Transaction, start, end time.Time, step int) {
	if fs.Err != nil {
		return
	}

	var wrfPrg fsutil.Path

	assimDateStart := start.Add(3 * time.Duration(step-3) * time.Hour)
	assimDateEnd := assimDateStart.Add(3 * time.Hour)
	if step == 3 {
		assimDateEnd = end
		wrfPrg = wrfPrgMainRun
	} else {
		wrfPrg = wrfPrgStep
	}

	wrfDir := fsutil.PathF("wrf%02d", assimDateStart.Hour())

	fs.MkDir(wrfDir)

	// boundary from WPS
	fs.Copy(wpsDir.Join("wrfbdy_d01"), wrfDir.Join("wrfbdy_d01"))

	// build namelist for wrf
	renderNameList(
		fs,
		"namelist.wrf",
		wrfDir.Join("namelist.input"),
		namelist.Args{
			Start: assimDateStart,
			End:   assimDateEnd,
		},
	)

	fs.Link(wrfPrg.Join("main/wrf.exe"), wrfDir.Join("wrf.exe"))
	fs.Link(wrfPrg.Join("run/LANDUSE.TBL"), wrfDir.Join("LANDUSE.TBL"))
	fs.Link(wrfPrg.Join("run/ozone_plev.formatted"), wrfDir.Join("ozone_plev.formatted"))
	fs.Link(wrfPrg.Join("run/ozone_lat.formatted"), wrfDir.Join("ozone_lat.formatted"))
	fs.Link(wrfPrg.Join("run/ozone.formatted"), wrfDir.Join("ozone.formatted"))
	fs.Link(wrfPrg.Join("run/RRTMG_LW_DATA"), wrfDir.Join("RRTMG_LW_DATA"))
	fs.Link(wrfPrg.Join("run/RRTMG_SW_DATA"), wrfDir.Join("RRTMG_SW_DATA"))
	fs.Link(wrfPrg.Join("run/VEGPARM.TBL"), wrfDir.Join("VEGPARM.TBL"))
	fs.Link(wrfPrg.Join("run/SOILPARM.TBL"), wrfDir.Join("SOILPARM.TBL"))
	fs.Link(wrfPrg.Join("run/GENPARM.TBL"), wrfDir.Join("GENPARM.TBL"))

	// prev da results

	daDir1 := fsutil.PathF("da%02d_d%02d", assimDateStart.Hour(), 1)
	daDir2 := fsutil.PathF("da%02d_d%02d", assimDateStart.Hour(), 2)
	daDir3 := fsutil.PathF("da%02d_d%02d", assimDateStart.Hour(), 3)
	fs.Link(daDir1.Join("wrfvar_output"), wrfDir.Join("wrfinput_d01"))
	fs.Link(daDir2.Join("wrfvar_output"), wrfDir.Join("wrfinput_d02"))
	fs.Link(daDir3.Join("wrfvar_output"), wrfDir.Join("wrfinput_d03"))
}

func buildDADirInDomain(fs *fsutil.Transaction, start, end time.Time, step, domain int) {
	if fs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// prepare da dir
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)

	fs.MkDir(daDir)

	fs.Copy(wpsDir.Join("wrfbdy_d01"), daDir.Join("wrfbdy_d01"))

	if step == 1 {
		// first step of assimilation receives input from WPS
		fs.Copy(wpsDir.JoinF("wrfinput_d%02d", domain), daDir.Join("fg"))
	} else {
		// the others steps receives input from the WRF run
		// of previous step.
		previousStep := fsutil.PathF("wrf%02d", assimDate.Hour()-3)
		fs.Copy(previousStep.JoinF("wrfvar_input_d%02d", domain), daDir.Join("fg"))
	}

	// build namelist for wrfda
	renderNameList(
		fs,
		fmt.Sprintf("namelist.d%02d.wrfda", domain),
		daDir.Join("namelist.input"),
		namelist.Args{
			Start: assimDate,
			End:   end,
		},
	)

	renderNameList(
		fs,
		"parame.in",
		daDir.Join("parame.in"),
		namelist.Args{
			Start: start,
			End:   end,
		},
	)

	// link files from WRFDA build directory
	fs.Link(wrfdaPrg.Join("var/build/da_wrfvar.exe"), daDir.Join("da_wrfvar.exe"))
	fs.Link(wrfdaPrg.Join("var/run/VARBC.in"), daDir.Join("VARBC.in"))
	fs.Link(wrfdaPrg.Join("run/LANDUSE.TBL"), daDir.Join("LANDUSE.TBL"))
	fs.Link(wrfdaPrg.Join("var/build/da_update_bc.exe"), daDir.Join("da_update_bc.exe"))

	// link covariance matrixes
	fs.Link(matrixDir.JoinF("summer/be_2.5km_d%02d", domain), daDir.Join("be.dat"))

	// link observations
	assimDateS := assimDate.Format("2006010215")
	fs.Link(observationsDir.JoinF("ob.radar.%s", assimDateS), daDir.Join("ob.radar"))
	fs.Link(observationsDir.JoinF("ob.ascii.%s.err", assimDateS), daDir.Join("ob.ascii"))

}

func runDAStep(fs *fsutil.Transaction, start time.Time, step int) {
	runDAStepInDomain(fs, start, step, 1)
	runDAStepInDomain(fs, start, step, 2)
	runDAStepInDomain(fs, start, step, 3)
}

func runWRFStep(fs *fsutil.Transaction, start time.Time, step int) {
	if fs.Err != nil {
		return
	}

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wrfDir := fsutil.PathF("wrf%02d", assimDate.Hour())

	fs.Run(wrfDir, wrfDir.Join("rsl.out.0000"), "mpirun", "-n", "84", "./wrf.exe")
}

func runDAStepInDomain(fs *fsutil.Transaction, start time.Time, step, domain int) {
	if fs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)

	fs.Run(daDir, daDir.Join("rsl.out.0000"), "mpirun", "-n", "50", "./da_wrfvar.exe")

	if domain == 1 {
		fs.Run(daDir, "", "./da_update_bc.exe")
	}
}

func buildDAStepDir(fs *fsutil.Transaction, start, end time.Time, step int) {
	buildDADirInDomain(fs, start, end, step, 1)
	buildDADirInDomain(fs, start, end, step, 2)
	buildDADirInDomain(fs, start, end, step, 3)
}

func main() {

	rootDir := fsutil.Path(os.Args[1])
	fs := fsutil.Transaction{Root: rootDir}
	if !fs.Exists(".") {
		log.Fatalf("Directory not found: %s", rootDir)
	}

	startDate := time.Date(2020, 07, 15, 12, 0, 0, 0, time.UTC)
	endDate := time.Date(2020, 07, 17, 12, 0, 0, 0, time.UTC)
	_ = endDate

	for step := 2; step <= 3; step++ {
		assimStartDate := startDate.Add(3 * time.Duration(step-3) * time.Hour)
		_ = assimStartDate
		/*
				// build namelist for real
				renderNameList(
					&fs,
					"namelist.real",
					wpsDir.Join("namelist.input"),
					namelist.Args{
						Start: assimStartDate,
						End:   endDate,
					},
				)

				// execute real
				fs.Run(wpsDir, wpsDir.Join("rsl.out.0000"), "mpirun", "-n", "36", "./real.exe")

			// first step of assimilation
			buildDAStepDir(&fs, startDate, endDate, step)

		*/

		runDAStep(&fs, startDate, step)

		buildWRFDir(&fs, startDate, endDate, step)

		runWRFStep(&fs, startDate, step)

		if fs.Err != nil {
			log.Fatal(fs.Err)
		}
		os.Exit(0)

	}

	if fs.Err != nil {
		log.Fatal(fs.Err)
	}

}
