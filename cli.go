package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/meteocima/wrfassim/conf"

	namelist "github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/wrfassim/fsutil"
)

var wrfdaPrg fsutil.Path
var wrfPrgStep fsutil.Path
var wrfPrgMainRun fsutil.Path
var wpsPrg fsutil.Path
var matrixDir fsutil.Path

var wpsDir = fsutil.Path("wps")
var inputsDir = fsutil.Path("../inputs")
var observationsDir = fsutil.Path("../observations")

//var geogridProcCount = "84"
//var metgridProcCount = "84"
//var wrfstepProcCount = "84"
//var wrfdaProcCount = "50"
//var realProcCount = "36"

var geogridProcCount = "10"
var metgridProcCount = "10"
var wrfstepProcCount = "10"
var wrfdaProcCount = "10"
var realProcCount = "10"

func renderNameList(fs *fsutil.Transaction, source string, target fsutil.Path, args namelist.Args) {
	if fs.Err != nil {
		return
	}

	tmplFile, err := os.Open(conf.Config.Folders.NamelistsDir.Join(source).String())
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

func buildWPSDir(fs *fsutil.Transaction, start, end time.Time, ds inputDataset) {
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

	fs.LinkAbs(wpsPrg.Join("link_grib.csh"), wpsDir.Join("link_grib.csh"))
	fs.LinkAbs(wpsPrg.Join("ungrib.exe"), wpsDir.Join("ungrib.exe"))
	fs.LinkAbs(wpsPrg.Join("metgrid.exe"), wpsDir.Join("metgrid.exe"))
	fs.LinkAbs(wpsPrg.Join("util/avg_tsfc.exe"), wpsDir.Join("avg_tsfc.exe"))
	fs.LinkAbs(wrfPrgStep.Join("run/real.exe"), wpsDir.Join("real.exe"))
	fs.LinkAbs(wpsPrg.Join("geogrid.exe"), wpsDir.Join("geogrid.exe"))

	if ds == GFS {
		fs.LinkAbs(wpsPrg.Join("ungrib/Variable_Tables/Vtable.GFS"), wpsDir.Join("Vtable"))
	} else if ds == IFS {
		fs.LinkAbs(wpsPrg.Join("ungrib/Variable_Tables/Vtable.ECMWF"), wpsDir.Join("Vtable"))
	}

}

func runWPS(fs *fsutil.Transaction, start, end time.Time) {
	if fs.Err != nil {
		return
	}

	fsutil.Logf("START WPS\n")

	fmt.Println("running geogrid")
	fs.Run(wpsDir, wpsDir.Join("geogrid.log.0000"), "mpirun", "-n", geogridProcCount, "./geogrid.exe")
	fmt.Println("running linkgrib ../gfs/*")
	fs.Run(wpsDir, "", "./link_grib.csh", "../gfs/*")
	fmt.Println("running ungrib")
	fs.Run(wpsDir, "", "./ungrib.exe")
	if end.Sub(start) > 24*time.Hour {
		fmt.Println("running avg_tsfc")
		fs.Run(wpsDir, "", "./avg_tsfc.exe")
	}
	fmt.Println("running metgrid")
	fs.Run(wpsDir, wpsDir.Join("metgrid.log.0000"), "mpirun", "-n", metgridProcCount, "./metgrid.exe")

	fsutil.Logf("COMPLETED WPS\n")

}

func buildWRFDir(fs *fsutil.Transaction, start, end time.Time, step int, domainCount int) {
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

	wrfvar := ""
	switch step {
	case 1:
		wrfvar = "wrf_var.txt.wrf_01"
	case 2:
		wrfvar = "wrf_var.txt.wrf_02"
	case 3:
		wrfvar = "wrf_var.txt.wrf_03"
	}

	wrfTarget := "wrf_var.txt"

	fs.CopyAbs(conf.Config.Folders.NamelistsDir.Join(wrfvar), wrfDir.Join(wrfTarget))

	fs.LinkAbs(wrfPrg.Join("main/wrf.exe"), wrfDir.Join("wrf.exe"))
	fs.LinkAbs(wrfPrg.Join("run/LANDUSE.TBL"), wrfDir.Join("LANDUSE.TBL"))
	fs.LinkAbs(wrfPrg.Join("run/ozone_plev.formatted"), wrfDir.Join("ozone_plev.formatted"))
	fs.LinkAbs(wrfPrg.Join("run/ozone_lat.formatted"), wrfDir.Join("ozone_lat.formatted"))
	fs.LinkAbs(wrfPrg.Join("run/ozone.formatted"), wrfDir.Join("ozone.formatted"))
	fs.LinkAbs(wrfPrg.Join("run/RRTMG_LW_DATA"), wrfDir.Join("RRTMG_LW_DATA"))
	fs.LinkAbs(wrfPrg.Join("run/RRTMG_SW_DATA"), wrfDir.Join("RRTMG_SW_DATA"))
	fs.LinkAbs(wrfPrg.Join("run/VEGPARM.TBL"), wrfDir.Join("VEGPARM.TBL"))
	fs.LinkAbs(wrfPrg.Join("run/SOILPARM.TBL"), wrfDir.Join("SOILPARM.TBL"))
	fs.LinkAbs(wrfPrg.Join("run/GENPARM.TBL"), wrfDir.Join("GENPARM.TBL"))

	// prev da results
	for domain := 1; domain <= domainCount; domain++ {
		daDir := fsutil.PathF("../da%02d_d%02d", dtStart.Hour(), domain)
		fs.LinkAbs(daDir.Join("wrfvar_output"), wrfDir.JoinF("wrfinput_d%02d", domain))
	}
}

func buildDADirInDomain(fs *fsutil.Transaction, mode runMode, start, end time.Time, step, domain int) {
	if fs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// prepare da dir
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)

	fs.MkDir(daDir)

	if domain == 1 {
		// domain 1 in every step of assimilation receives boundaries from WPS or from 'inputs' directory.
		fs.Copy(inputsDir.Join(start.Format("20060102")).JoinF("wrfbdy_d01_da%02d", step), daDir.Join("wrfbdy_d01"))
	}

	if step == 1 {
		// first step of assimilation receives fg input from WPS or from 'inputs' directory.
		fs.Copy(inputsDir.Join(start.Format("20060102")).JoinF("wrfinput_d%02d", domain), daDir.Join("fg"))
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
	fs.LinkAbs(wrfdaPrg.Join("var/build/da_wrfvar.exe"), daDir.Join("da_wrfvar.exe"))
	fs.LinkAbs(wrfdaPrg.Join("var/run/VARBC.in"), daDir.Join("VARBC.in"))
	fs.LinkAbs(wrfdaPrg.Join("run/LANDUSE.TBL"), daDir.Join("LANDUSE.TBL"))
	fs.LinkAbs(wrfdaPrg.Join("var/build/da_update_bc.exe"), daDir.Join("da_update_bc.exe"))

	// link covariance matrixes
	fs.LinkAbs(matrixDir.JoinF("summer/be_2.5km_d%02d", domain), daDir.Join("be.dat"))

	// link observations
	assimDateS := assimDate.Format("2006010215")
	fs.LinkAbs(observationsDir.JoinF("ob.radar.%s", assimDateS), daDir.Join("ob.radar"))
	fs.LinkAbs(observationsDir.JoinF("ob.ascii.%s", assimDateS), daDir.Join("ob.ascii"))
}

func runWRFStep(fs *fsutil.Transaction, start time.Time, step int) {
	if fs.Err != nil {
		return
	}

	fsutil.Logf("START WRF STEP %d\n", step)

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wrfDir := fsutil.PathF("wrf%02d", assimDate.Hour())

	fs.Run(wrfDir, wrfDir.Join("rsl.out.0000"), "mpirun", "-n", wrfstepProcCount, "./wrf.exe")

	fsutil.Logf("COMPLETED WRF STEP %d\n", step)
}

func runDAStepInDomain(fs *fsutil.Transaction, start time.Time, step, domain int) {
	if fs.Err != nil {
		return
	}

	fsutil.Logf("START DA STEP %d in DOMAIN %d\n", step, domain)

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	daDir := fsutil.PathF("da%02d_d%02d", assimDate.Hour(), domain)

	fs.Run(daDir, daDir.Join("rsl.out.0000"), "mpirun", "-n", wrfdaProcCount, "./da_wrfvar.exe")

	if domain == 1 {
		fs.Run(daDir, "", "./da_update_bc.exe")
	}

	fsutil.Logf("COMPLETED DA STEP %d in DOMAIN %d\n", step, domain)
}

func runDAStep(fs *fsutil.Transaction, start time.Time, step int, domainCount int) {
	if fs.Err != nil {
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		runDAStepInDomain(fs, start, step, domain)
	}
}

func buildDAStepDir(fs *fsutil.Transaction, mode runMode, start, end time.Time, step int, domainCount int) {
	if fs.Err != nil {
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		buildDADirInDomain(fs, mode, start, end, step, domain)
	}

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

func runReal(fs *fsutil.Transaction, startDate time.Time, step int, domainCount int) {
	if fs.Err != nil {
		return
	}

	fsutil.Logf("START REAL\n")

	//fs.Run(wpsDir, wpsDir.Join("rsl.out.0000"), "mpirun", "-n", realProcCount, "./real.exe")

	indir := inputsDir.Join(startDate.Format("20060102"))
	fs.MkDir(indir)

	fs.Copy(wpsDir.JoinF("wrfbdy_d01"), indir.JoinF("wrfbdy_d01_da%02d", step))

	if step != 1 {
		fsutil.Logf("COMPLETED REAL\n")
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		fs.Copy(wpsDir.JoinF("wrfinput_d%02d", domain), indir.JoinF("wrfinput_d%02d", domain))
	}

	fsutil.Logf("COMPLETED REAL\n")

}

func buildWRFDAWorkdir(fs *fsutil.Transaction, mode runMode, startDate time.Time) {
	if fs.Err != nil {
		return
	}

	folders := conf.Config.Folders
	workdir := fsutil.Path(startDate.Format("20060102"))

	fs.MkDir(workdir)

	fs.LinkAbs(conf.Config.Folders.GeodataDir, workdir.Join("geodata"))
	fs.LinkAbs(conf.Config.Folders.CovarMatrixesDir, workdir.Join("matrix"))
	fs.LinkAbs(conf.Config.Folders.WPSPrg, workdir.Join("wpsprg"))
	fs.LinkAbs(conf.Config.Folders.WRFDAPrg, workdir.Join("wrfdaprg"))
	fs.LinkAbs(conf.Config.Folders.WRFMainRunPrg, workdir.Join("wrfprgrun"))
	fs.LinkAbs(conf.Config.Folders.WRFAssStepPrg, workdir.Join("wrfprgstep"))

	observationDir := workdir.Join("observations")
	gfsDir := workdir.Join("gfs")

	fs.MkDir(gfsDir)
	fs.MkDir(observationDir)

	assimStartDate := startDate.Add(2 * time.Duration(-3) * time.Hour)

	if mode == WPSMode || mode == WPSDAMode {
		// GFS
		gfsSources := folders.GFSArchive.Join(assimStartDate.Format("2006/01/02/1504"))
		for _, filename := range fs.ReaddirAbs(gfsSources) {
			gfsFile := gfsSources.Join(filename)
			fs.CopyAbs(gfsFile, gfsDir.Join(filename))
		}
	}

	// Observations - weather stations and radars

	if mode == DAMode || mode == WPSDAMode {
		cpObervations := func(dt time.Time) {
			fs.CopyAbs(
				folders.ObservationsArchive.JoinF("ob.radar.%s", dt.Format("2006010215")),
				observationDir.JoinF("ob.radar.%s", dt.Format("2006010215")),
			)

			fs.CopyAbs(
				folders.ObservationsArchive.JoinF("ob.ascii.%s", dt.Format("2006010215")),
				observationDir.JoinF("ob.ascii.%s", dt.Format("2006010215")),
			)
		}
		cpObervations(assimStartDate)
		cpObervations(assimStartDate.Add(3 * time.Hour))
		cpObervations(assimStartDate.Add(6 * time.Hour))
	}
}

func runWRFDA(fs *fsutil.Transaction, mode runMode, startDate time.Time, ds inputDataset, domainCount int) {
	if fs.Err != nil {
		return
	}

	endDate := startDate.Add(42 * time.Hour)

	if mode == WPSMode || mode == WPSDAMode {
		//buildWPSDir(fs, startDate, endDate, ds)
		//runWPS(fs, startDate, endDate)
		for step := 1; step <= 3; step++ {
			buildNamelistForReal(fs, startDate, endDate, step)
			runReal(fs, startDate, step, domainCount)
		}
	}

	if mode == DAMode || mode == WPSDAMode {
		for step := 1; step <= 3; step++ {
			buildDAStepDir(fs, mode, startDate, endDate, step, domainCount)
			runDAStep(fs, startDate, step, domainCount)

			buildWRFDir(fs, startDate, endDate, step, domainCount)
			runWRFStep(fs, startDate, step)
		}
	}
}

type runMode int

const (
	// WPSMode - run only WPS
	WPSMode runMode = iota
	// DAMode - run only DA
	DAMode
	// WPSDAMode - run WPS followed by DA
	WPSDAMode
)

type inputDataset int

const (
	// GFS ...
	GFS inputDataset = iota
	// IFS ...
	IFS
)

func readDomainCount(mode runMode) (int, error) {
	nmlDir := conf.Config.Folders.NamelistsDir
	namelistToReadMaxDom := "namelist.run.wrf"
	if mode == WPSMode || mode == WPSDAMode {
		namelistToReadMaxDom = "namelist.wps"
	}

	fileName := nmlDir.Join(namelistToReadMaxDom).String()
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("Cannot read %s:%w", fileName, err)
	}

	rows := strings.Split(string(content), "\n")
	fmt.Println(rows)

	for _, line := range rows {
		trimdLine := strings.TrimLeft(line, " \t")
		if strings.HasPrefix(trimdLine, "max_dom") {
			fields := strings.Split(trimdLine, "=")
			if len(fields) < 2 {
				return 0, fmt.Errorf("Malformed max_dom property in `%s`: %s", trimdLine, err)
			}
			valueS := strings.Trim(fields[1], " \t,")
			value, err := strconv.Atoi(valueS)
			if err != nil {
				return 0, fmt.Errorf("Cannot convert max_dom `%s` to integer: %w", valueS, err)
			}
			return value, nil
		}
	}

	return 0, fmt.Errorf("max_dom property not found in %s", fileName)
}

func main() {
	usage := "Usage: wrfassim [-m WPS|DA|WPSDA] [-i GFS|IFS] <workdir> <startdate> <enddate>\nformat for dates: YYYYMMDDHH\ndefault for -m is WPSDA\n"

	modeF := flag.String("m", "WPSDA", "")
	inputF := flag.String("i", "GFS", "")

	flag.Parse()

	var mode runMode
	var input inputDataset

	if *modeF == "WPS" {
		mode = WPSMode
	} else if *modeF == "DA" {
		mode = DAMode
	} else if *modeF == "WPSDA" {
		mode = WPSDAMode
	} else {
		log.Fatalf("%s\nUnknown mode `%s`", usage, *modeF)
	}

	if *inputF == "GFS" {
		input = GFS
	} else if *inputF == "IFS" {
		input = IFS
	} else {
		log.Fatalf("%s\nUnknown input dataset `%s`", usage, *modeF)
	}

	args := flag.Args()
	if len(args) != 3 {
		log.Fatal(usage)
	}

	startDate, err := time.Parse("2006010215", args[1])
	if err != nil {
		log.Fatal(usage + err.Error() + "\n")
	}
	endDate, err := time.Parse("2006010215", args[2])
	if err != nil {
		log.Fatal(usage + err.Error() + "\n")
	}
	workdir := fsutil.Path(args[0])

	conf.Init(workdir.Join("wrfda-runner.cfg").String())

	domainCount, err := readDomainCount(mode)

	wrfdaPrg = conf.Config.Folders.WRFDAPrg
	wrfPrgStep = conf.Config.Folders.WRFAssStepPrg
	wrfPrgMainRun = conf.Config.Folders.WRFMainRunPrg
	wpsPrg = conf.Config.Folders.WPSPrg
	matrixDir = conf.Config.Folders.CovarMatrixesDir

	fsutil.Logf(
		"RUN FOR DATES FROM %s TO %s\n",
		startDate.Format("2006010215"),
		endDate.Format("2006010215"),
	)

	for dt := startDate; dt.Unix() <= endDate.Unix(); dt = dt.Add(time.Hour * 24) {
		fsutil.Logf("STARTING RUN FOR DATE %s\n", dt.Format("2006010215"))
		fs := fsutil.Transaction{Root: workdir}
		if !fs.Exists(".") {
			log.Fatalf("Directory not found: %s", workdir.String())
		}
		buildWRFDAWorkdir(&fs, mode, dt)
		if fs.Err != nil {
			log.Fatal(fs.Err)
		}
		fs = fsutil.Transaction{Root: workdir.Join(dt.Format("20060102"))}
		runWRFDA(&fs, mode, dt, input, domainCount)
		if fs.Err != nil {
			log.Fatal(fs.Err)
		}

		fsutil.Logf("RUN FOR DATE %s COMPLETED\n", dt.Format("2006010215"))
	}

}
