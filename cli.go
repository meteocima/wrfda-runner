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

var wrfdaPrg = fsutil.Path("wrfdaprg")
var wrfPrgStep = fsutil.Path("wrfprgstep")
var wrfPrgMainRun = fsutil.Path("wrfprgrun")
var wpsPrg = fsutil.Path("wpsprg")
var matrixDir = fsutil.Path("matrix")
var wpsDir = fsutil.Path("wps")
var observationsDir = fsutil.Path("observations")

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
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		os.FileMode(0644),
	)
	defer targetNamelistFile.Close()

	if err != nil {
		log.Fatalf("open namelist.real: %s", err.Error())
	}

	args.Hours = int(args.End.Sub(args.Start).Hours())

	tmpl := namelist.Tmpl{}
	tmpl.ReadTemplateFrom(tmplFile)
	tmpl.RenderTo(args, targetNamelistFile)
}

func buildWPSDir(fs *fsutil.Transaction, start, end time.Time) {
	if fs.Err != nil {
		return
	}
	fs.MkDir(wpsDir)

	// build namelist for wrf
	renderNameList(
		fs,
		"namelist.wps",
		wpsDir.Join("namelist.wps"),
		namelist.Args{
			Start: start.Add(-6 * time.Hour),
			End:   end,
		},
	)

	fs.Link(wpsPrg.Join("link_grib.csh"), wpsDir.Join("link_grib.csh"))
	fs.Link(wpsPrg.Join("ungrib.exe"), wpsDir.Join("ungrib.exe"))
	fs.Link(wpsPrg.Join("metgrid.exe"), wpsDir.Join("metgrid.exe"))
	fs.Link(wpsPrg.Join("util/avg_tsfc.exe"), wpsDir.Join("avg_tsfc.exe"))
	fs.Link(wrfPrgStep.Join("run/real.exe"), wpsDir.Join("real.exe"))
	fs.Link(wpsPrg.Join("geogrid.exe"), wpsDir.Join("geogrid.exe"))
	fs.Link(wpsPrg.Join("ungrib/Variable_Tables/Vtable.GFS"), wpsDir.Join("Vtable"))

	fs.Link(wrfPrgStep.Join("run/RRTM_DATA"), wpsDir.Join("RRTM_DATA"))

	fs.Link(wrfPrgStep.Join("run/RRTM_DATA_DBL"), wpsDir.Join("RRTM_DATA_DBL"))
	fs.Link(wrfPrgStep.Join("run/RRTMG_LW_DATA"), wpsDir.Join("RRTMG_LW_DATA"))
	fs.Link(wrfPrgStep.Join("run/RRTMG_LW_DATA_DBL"), wpsDir.Join("RRTMG_LW_DATA_DBL"))
	fs.Link(wrfPrgStep.Join("run/RRTMG_SW_DATA"), wpsDir.Join("RRTMG_SW_DATA"))
	fs.Link(wrfPrgStep.Join("run/RRTMG_SW_DATA_DBL"), wpsDir.Join("RRTMG_SW_DATA_DBL"))
	fs.Link(wrfPrgStep.Join("run/URBPARM.TBL"), wpsDir.Join("URBPARM.TBL"))
	fs.Link(wrfPrgStep.Join("run/URBPARM_UZE.TBL"), wpsDir.Join("URBPARM_UZE.TBL"))
	fs.Link(wrfPrgStep.Join("run/VEGPARM.TBL"), wpsDir.Join("VEGPARM.TBL"))
	fs.Link(wrfPrgStep.Join("run/SOILPARM.TBL"), wpsDir.Join("SOILPARM.TBL"))

}

func runWPS(fs *fsutil.Transaction, start, end time.Time) {
	if fs.Err != nil {
		return
	}

	fsutil.Logf("START WPS\n")

	fmt.Println("running geogrid")
	fs.Run(wpsDir, wpsDir.Join("geogrid.log.0000"), "mpirun", "-n", "84", "./geogrid.exe")
	fmt.Println("running linkgrib ../gfs/*")
	fs.Run(wpsDir, "", "./link_grib.csh", "../gfs/*")
	fmt.Println("running ungrib")
	fs.Run(wpsDir, "", "./ungrib.exe")
	if end.Sub(start) > 24*time.Hour {
		fmt.Println("running avg_tsfc")
		fs.Run(wpsDir, "", "./avg_tsfc.exe")
	}
	fmt.Println("running metgrid")
	fs.Run(wpsDir, wpsDir.Join("metgrid.log.0000"), "mpirun", "-n", "84", "./metgrid.exe")

	fsutil.Logf("COMPLETED WPS\n")

}

func buildWRFDir(fs *fsutil.Transaction, start, end time.Time, step int) {
	if fs.Err != nil {
		return
	}

	wrfPrg := wrfPrgStep
	nameListName := "namelist.step.wrf"

	dtStart := start.Add(3 * time.Duration(step-3) * time.Hour)
	dtEnd := dtStart.Add(3 * time.Hour)
	if step == 3 {
		dtEnd = end
		wrfPrg = wrfPrgMainRun
		nameListName = "namelist.run.wrf"
	}

	wrfDir := fsutil.PathF("wrf%02d", dtStart.Hour())

	fs.MkDir(wrfDir)

	// boundary from same cycle da dir for domain 1

	daBdy := fsutil.PathF("da%02d_d01/wrfbdy_d01", dtStart.Hour())
	fs.Copy(daBdy, wrfDir.Join("wrfbdy_d01"))

	// build namelist for wrf
	renderNameList(
		fs,
		nameListName,
		wrfDir.Join("namelist.input"),
		namelist.Args{
			Start: dtStart,
			End:   dtEnd,
		},
	)

	fs.Save(wrfDir.Join("wrf_var.txt"), []byte("\n"))
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
	daDir1 := fsutil.PathF("da%02d_d%02d", dtStart.Hour(), 1)
	daDir2 := fsutil.PathF("da%02d_d%02d", dtStart.Hour(), 2)
	daDir3 := fsutil.PathF("da%02d_d%02d", dtStart.Hour(), 3)
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
		prevHour := assimDate.Hour() - 3
		if prevHour < 0 {
			prevHour += 24
		}

		previousStep := fsutil.PathF("wrf%02d", prevHour)
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

	fsutil.Logf("START WRF STEP %d\n", step)

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wrfDir := fsutil.PathF("wrf%02d", assimDate.Hour())

	fs.Run(wrfDir, wrfDir.Join("rsl.out.0000"), "mpirun", "-n", "84", "./wrf.exe")

	fsutil.Logf("COMPLETED WRF STEP %d\n", step)
}

func runDAStepInDomain(fs *fsutil.Transaction, start time.Time, step, domain int) {
	if fs.Err != nil {
		return
	}

	fsutil.Logf("START DA STEP %d in DOMAIN %d\n", step, domain)

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)

	fs.Run(daDir, daDir.Join("rsl.out.0000"), "mpirun", "-n", "50", "./da_wrfvar.exe")

	if domain == 1 {
		fs.Run(daDir, "", "./da_update_bc.exe")
	}

	fsutil.Logf("COMPLETED DA STEP %d in DOMAIN %d\n", step, domain)
}

func buildDAStepDir(fs *fsutil.Transaction, start, end time.Time, step int) {
	//assimStartDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	//dds.DownloadRadar(assimStartDate)
	//radar.Convert(".", assimStartDate.Format("2006010214"))

	buildDADirInDomain(fs, start, end, step, 1)
	buildDADirInDomain(fs, start, end, step, 2)
	buildDADirInDomain(fs, start, end, step, 3)
}

func buildNamelistForReal(fs *fsutil.Transaction, start, end time.Time, step int) {
	assimStartDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// build namelist for real
	renderNameList(
		fs,
		"namelist.real",
		wpsDir.Join("namelist.input"),
		namelist.Args{
			Start: assimStartDate,
			End:   end,
		},
	)
}

func runReal(fs *fsutil.Transaction) {
	fsutil.Logf("START REAL\n")

	fs.Run(wpsDir, wpsDir.Join("rsl.out.0000"), "mpirun", "-n", "36", "./real.exe")
	fsutil.Logf("COMPLETED REAL\n")
}

func main() {

	startDate := time.Date(2020, 7, 15, 12, 0, 0, 0, time.UTC)
	err := runWRFDA(os.Args[1], startDate)
	if err != nil {
		log.Fatal(err)
	}
}

func runWRFDA(rootPath string, startDate time.Time) error {
	rootDir := fsutil.Path(rootPath)
	endDate := startDate.Add(48 * time.Hour)

	fs := fsutil.Transaction{Root: rootDir}
	if !fs.Exists(".") {
		log.Fatalf("Directory not found: %s", rootDir)
	}

	buildWPSDir(&fs, startDate, endDate)
	runWPS(&fs, startDate, endDate)
	for step := 1; step <= 3; step++ {
		buildNamelistForReal(&fs, startDate, endDate, step)
		runReal(&fs)

		buildDAStepDir(&fs, startDate, endDate, step)
		runDAStep(&fs, startDate, step)

		buildWRFDir(&fs, startDate, endDate, step)
		runWRFStep(&fs, startDate, step)
	}

	return fs.Err

}
