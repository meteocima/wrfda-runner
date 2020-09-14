package main

import (
	"log"
	"os"
	"time"

	namelist "github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/wrfassim/fsutil"
)

var Namelists *FileSystem

func renderNameList(source string, target fsutil.Path, args namelist.Args) {
	tmplFile, err := Namelists.Open("/" + source)
	if err != nil {
		log.Fatalf("open template: %s", err.Error())
	}

	targetNamelistFile, err := os.OpenFile(
		target.String(),
		os.O_CREATE|os.O_WRONLY,
		os.FileMode(0644),
	)

	if err != nil {
		log.Fatalf("open namelist.real: %s", err.Error())
	}

	tmpl := namelist.Tmpl{}
	tmpl.ReadTemplateFrom(tmplFile)
	tmpl.RenderTo(args, targetNamelistFile)
}

var namelists = fsutil.Path("namelists")
var wrfdaPrg = fsutil.Path("wrfda")
var wrfPrg = fsutil.Path("wrf")
var matrixDir = fsutil.Path("matrix")

func buildWRFDir(fs *fsutil.Transaction, start time.Time, step int) {
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wrfDir := fsutil.PathF("wrf%02d", assimDate.Hour())
	wpsDir := fsutil.Path("wps")

	fs.MkDir(wrfDir)

	// boundary from WPS
	fs.Copy(wpsDir.Join("wrfbdy_d01"), wrfDir.Join("wrfbdy_d01"))

	// build namelist for wrf
	renderNameList(
		"namelist.wrf",
		wrfDir.Join("namelist.input"),
		namelist.Args{
			Start: start,
		},
	)

	fs.Link(wrfdaPrg.Join("main/wrf.exe"), wrfDir.Join("wrf.exe"))
	fs.Link(wrfdaPrg.Join("run/LANDUSE.TBL"), wrfDir.Join("LANDUSE.TBL"))
	fs.Link(wrfdaPrg.Join("run/ozone_plev.formatted"), wrfDir.Join("ozone_plev.formatted"))
	fs.Link(wrfdaPrg.Join("run/ozone_lat.formatted"), wrfDir.Join("ozone_lat.formatted"))
	fs.Link(wrfdaPrg.Join("run/ozone.formatted"), wrfDir.Join("ozone.formatted"))
	fs.Link(wrfdaPrg.Join("run/RRTMG_LW_DATA"), wrfDir.Join("RRTMG_LW_DATA"))
	fs.Link(wrfdaPrg.Join("run/RRTMG_SW_DATA"), wrfDir.Join("RRTMG_SW_DATA"))
	fs.Link(wrfdaPrg.Join("run/VEGPARM.TBL"), wrfDir.Join("VEGPARM.TBL"))
	fs.Link(wrfdaPrg.Join("run/SOILPARM.TBL"), wrfDir.Join("SOILPARM.TBL"))
	fs.Link(wrfdaPrg.Join("run/GENPARM.TBL"), wrfDir.Join("GENPARM.TBL"))

	// prev da results

	daDir1 := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), 1)
	daDir2 := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), 2)
	daDir3 := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), 3)
	fs.Link(daDir1.Join("wrfvar_output"), wrfDir.Join("wrfinput_d01"))
	fs.Link(daDir2.Join("wrfvar_output"), wrfDir.Join("wrfinput_d02"))
	fs.Link(daDir3.Join("wrfvar_output"), wrfDir.Join("wrfinput_d03"))
}

func buildDADirInDomain(fs *fsutil.Transaction, start time.Time, step, domain int) {
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// prepare da dir
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)
	wpsDir := fsutil.Path("wps")

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
		"namelist.wrfda",
		daDir.Join("namelist.input"),
		namelist.Args{
			Start: start,
		},
	)

	// namelist for bc update
	fs.Copy(namelists.Join("parame.in"), daDir.Join("parame.in"))

	// link files from WRFDA build directory
	fs.Link(wrfdaPrg.Join("var/build/da_wrfvar.exe"), daDir.Join("da_wrfvar.exe"))
	fs.Link(wrfdaPrg.Join("var/run/VARBC.in"), daDir.Join("VARBC.in"))
	fs.Link(wrfdaPrg.Join("run/LANDUSE.TBL"), daDir.Join("LANDUSE.TBL"))
	fs.Link(wrfdaPrg.Join("var/build/da_update_bc.exe"), daDir.Join("da_update_bc.exe"))

	// link covariance matrixes
	fs.Link(matrixDir.Join("summer/be_2.5km_d01"), daDir.Join("be.dat"))

	// link observations
	assimDateS := assimDate.Format("200602011504")
	fs.Link(fsutil.PathF("ob.radar.%s", assimDateS), daDir.Join("ob.radar"))
	fs.Link(fsutil.PathF("ob.stations.%s", assimDateS), daDir.Join("ob.ascii"))

	if fs.Err != nil {
		log.Fatal(fs.Err)
	}
}

func main() {
	rootDir := fsutil.Path(os.Args[1])
	fs := fsutil.Transaction{Root: rootDir}
	if !fs.Exists(".") {
		log.Fatalf("Directory not found: %s", rootDir)
	}

	wpsDir := fsutil.Path("wps")
	if !fs.Exists(wpsDir) {
		log.Fatalf("Directory not found: %s", wpsDir)
	}

	startDate := time.Date(2020, 07, 15, 12, 0, 0, 0, time.UTC)
	endDate := time.Date(2020, 07, 17, 12, 0, 0, 0, time.UTC)
	_ = endDate

	for step := 1; step <= 3; step++ {
		// build namelist for wrfda
		renderNameList(
			"namelist.real",
			wpsDir.Join("namelist.input"),
			namelist.Args{
				Start: startDate,
			},
		)

		// execute real
		fs.Run(wpsDir, "mpirun -n 36 ./real.exe")

		// first step of assimilation
		buildDAStepDir(&fs, startDate, step)
		runDAStep(&fs, startDate, step)
		buildWRFDir(&fs, startDate, step)
		runWRFStep(&fs, startDate, step)
	}

}

func runDAStep(fs *fsutil.Transaction, start time.Time, step int) {
	runDAStepInDomain(fs, start, step, 1)
	runDAStepInDomain(fs, start, step, 1)
	runDAStepInDomain(fs, start, step, 1)
}

func runWRFStep(fs *fsutil.Transaction, start time.Time, step int) {
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wrfDir := fsutil.PathF("wrf%02d", assimDate.Hour())

	fs.Run(wrfDir, "mpirun -n 84 ./wrf.exe")
}

func runDAStepInDomain(fs *fsutil.Transaction, start time.Time, step, domain int) {
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)

	fs.Run(daDir, "mpirun -n 50 ./da_wrfvar.exe")

	if domain == 1 {
		fs.Run(daDir, "./da_update_bc.exe")
	}
}

func buildDAStepDir(fs *fsutil.Transaction, start time.Time, step int) {
	buildDADirInDomain(fs, start, step, 1)
	buildDADirInDomain(fs, start, step, 2)
	buildDADirInDomain(fs, start, step, 3)
}
