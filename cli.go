package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/meteocima/virtual-server/config"
	"github.com/meteocima/virtual-server/connection"
	"github.com/meteocima/wrfassim/conf"

	namelist "github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
)

var wrfdaPrg vpath.VirtualPath
var wrfPrgStep vpath.VirtualPath
var wrfPrgMainRun vpath.VirtualPath
var wpsPrg vpath.VirtualPath
var matrixDir vpath.VirtualPath

var wpsDir = vpath.New("localhost", "wps")
var inputsDir = vpath.New("localhost", "../inputs")
var observationsDir = vpath.New("localhost", "../observations")

var geogridProcCount = "84"
var metgridProcCount = "84"
var wrfstepProcCount = "84"
var wrfdaProcCount = "50"
var realProcCount = "36"

//var geogridProcCount = "10"
//var metgridProcCount = "10"
//var wrfstepProcCount = "10"
//var wrfdaProcCount = "10"
//var realProcCount = "10"

func renderNameList(vs *ctx.Context, source string, target vpath.VirtualPath, args namelist.Args) {
	if vs.Err != nil {
		return
	}

	tmplFile, err := os.Open(conf.Config.Folders.NamelistsDir.Join(source).String())
	if err != nil {
		log.Fatalf("open template: %s", err.Error())
	}

	targetNamelistFile, err := os.OpenFile(
		target.String(),
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

func buildWPSDir(vs *ctx.Context, start, end time.Time, ds inputDataset) {
	if vs.Err != nil {
		return
	}
	vs.MkDir(wpsDir)

	// build namelist for wrf
	renderNameList(
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

	if ds == GFS {
		vs.Link(wpsPrg.Join("ungrib/Variable_Tables/Vtable.GFS"), wpsDir.Join("Vtable"))
	} else if ds == IFS {
		vs.Link(wpsPrg.Join("ungrib/Variable_Tables/Vtable.ECMWF"), wpsDir.Join("Vtable"))
	}

}

func runWPS(vs *ctx.Context, start, end time.Time) {
	if vs.Err != nil {
		return
	}

	vs.LogF("START WPS\n")

	vs.LogF("Running geogrid")
	vs.Run(
		wpsDir.Join("mpirun"),
		[]string{"-n", geogridProcCount, "./geogrid.exe"},
		connection.RunOptions{OutFromLog: wpsDir.Join("geogrid.log.0000")},
	)
	if vs.Err != nil {
		return
	}

	vs.LogF("Running linkgrib ../gfs/*")
	vs.Run(wpsDir.Join("./link_grib.csh"), []string{"../gfs/*"})
	if vs.Err != nil {
		return
	}

	vs.LogF("Running ungrib")
	vs.Run(wpsDir.Join("./ungrib.exe"), []string{})
	if vs.Err != nil {
		return
	}

	if end.Sub(start) > 24*time.Hour {
		vs.LogF("Running avg_tsfc")
		vs.Run(wpsDir.Join("./avg_tsfc.exe"), []string{"../gfs/*"})
		if vs.Err != nil {
			return
		}
	}

	vs.LogF("Running metgrid")
	vs.Run(
		wpsDir.Join("mpirun"),
		[]string{"-n", metgridProcCount, "./metgrid.exe"},
		connection.RunOptions{OutFromLog: wpsDir.Join("metgrid.log.0000")},
	)

	if vs.Err != nil {
		return
	}

	vs.LogF("COMPLETED WPS\n")

}

func buildWRFDir(vs *ctx.Context, start, end time.Time, step int, domainCount int) {
	if vs.Err != nil {
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

	wrfDir := vpath.New("localhost", "wrf%02d", dtStart.Hour())

	vs.MkDir(wrfDir)

	// boundary from same cycle da dir for domain 1

	daBdy := vpath.New("localhost", "da%02d_d01/wrfbdy_d01", dtStart.Hour())
	vs.Copy(daBdy, wrfDir.Join("wrfbdy_d01"))

	// build namelist for wrf
	renderNameList(
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

	wrfTarget := "wrf_var.txt"

	vs.Copy(conf.Config.Folders.NamelistsDir.Join(wrfvar), wrfDir.Join(wrfTarget))

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
		daDir := vpath.New("localhost", "../da%02d_d%02d", dtStart.Hour(), domain)
		vs.Link(daDir.Join("wrfvar_output"), wrfDir.Join("wrfinput_d%02d", domain))
	}
}

func buildDADirInDomain(vs *ctx.Context, phase runPhase, start, end time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// prepare da dir
	daDir := vpath.New("localhost", "da%02d_d%02d", assimDate.Hour(), domain)

	vs.MkDir(daDir)

	if domain == 1 {
		// domain 1 in every step of assimilation receives boundaries from WPS or from 'inputs' directory.
		vs.Copy(inputsDir.Join(start.Format("20060102")).Join("wrfbdy_d01_da%02d", step), daDir.Join("wrfbdy_d01"))
	}

	if step == 1 {
		// first step of assimilation receives fg input from WPS or from 'inputs' directory.
		vs.Copy(inputsDir.Join(start.Format("20060102")).Join("wrfinput_d%02d", domain), daDir.Join("fg"))
	} else {
		// the others steps receives input from the WRF run
		// of previous step.
		prevHour := assimDate.Hour() - 3
		if prevHour < 0 {
			prevHour += 24
		}

		previousStep := vpath.New("localhost", "wrf%02d", prevHour)
		vs.Copy(previousStep.Join("wrfvar_input_d%02d", domain), daDir.Join("fg"))
	}

	// build namelist for wrfda
	renderNameList(
		vs,
		fmt.Sprintf("namelist.d%02d.wrfda", domain),
		daDir.Join("namelist.input"),
		namelist.Args{
			Start: assimDate,
			End:   end,
		},
	)

	renderNameList(
		vs,
		"parame.in",
		daDir.Join("parame.in"),
		namelist.Args{
			Start: start,
			End:   end,
		},
	)

	// link files from WRFDA build directory
	vs.Link(wrfdaPrg.Join("var/build/da_wrfvar.exe"), daDir.Join("da_wrfvar.exe"))
	vs.Link(wrfdaPrg.Join("var/run/VARBC.in"), daDir.Join("VARBC.in"))
	vs.Link(wrfdaPrg.Join("run/LANDUSE.TBL"), daDir.Join("LANDUSE.TBL"))
	vs.Link(wrfdaPrg.Join("var/build/da_update_bc.exe"), daDir.Join("da_update_bc.exe"))

	// link covariance matrixes
	vs.Link(matrixDir.Join("summer/be_2.5km_d%02d", domain), daDir.Join("be.dat"))

	// link observations
	assimDateS := assimDate.Format("2006010215")
	vs.Link(observationsDir.Join("ob.radar.%s", assimDateS), daDir.Join("ob.radar"))
	vs.Link(observationsDir.Join("ob.ascii.%s", assimDateS), daDir.Join("ob.ascii"))
}

func runWRFStep(vs *ctx.Context, start time.Time, step int) {
	if vs.Err != nil {
		return
	}

	vs.LogF("START WRF STEP %d\n", step)

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wrfDir := vpath.New("localhost", "wrf%02d", assimDate.Hour())

	vs.Run(wrfDir.Join("mpirun"), []string{"-n", wrfstepProcCount, "./wrf.exe"},
		connection.RunOptions{OutFromLog: wrfDir.Join("rsl.out.0000")})

	vs.LogF("COMPLETED WRF STEP %d\n", step)
}

func runDAStepInDomain(vs *ctx.Context, start time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}

	vs.LogF("START DA STEP %d in DOMAIN %d\n", step, domain)

	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	daDir := vpath.New("localhost", "da%02d_d%02d", assimDate.Hour(), domain)

	vs.Run(
		daDir.Join("mpirun"),
		[]string{"-n", wrfdaProcCount, "./da_wrfvar.exe"},
		connection.RunOptions{OutFromLog: daDir.Join("rsl.out.0000")},
	)

	if domain == 1 {
		vs.Run(daDir.Join("./da_update_bc.exe"), []string{})
	}

	vs.LogF("COMPLETED DA STEP %d in DOMAIN %d\n", step, domain)
}

func runDAStep(vs *ctx.Context, start time.Time, step int, domainCount int) {
	if vs.Err != nil {
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		runDAStepInDomain(vs, start, step, domain)
	}
}

func buildDAStepDir(vs *ctx.Context, phase runPhase, start, end time.Time, step int, domainCount int) {
	if vs.Err != nil {
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		buildDADirInDomain(vs, phase, start, end, step, domain)
	}

}

func buildNamelistForReal(vs *ctx.Context, start, end time.Time, step int) {
	assimStartDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// build namelist for real
	renderNameList(
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

	vs.LogF("START REAL\n")

	vs.Run(
		wpsDir.Join("mpirun"),
		[]string{"-n", realProcCount, "./real.exe"},
		connection.RunOptions{OutFromLog: wpsDir.Join("rsl.out.0000")},
	)

	indir := inputsDir.Join(startDate.Format("20060102"))
	vs.MkDir(indir)

	vs.Copy(wpsDir.Join("wrfbdy_d01"), indir.Join("wrfbdy_d01_da%02d", step))

	if step != 1 {
		vs.LogF("COMPLETED REAL\n")
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		vs.Copy(wpsDir.Join("wrfinput_d%02d", domain), indir.Join("wrfinput_d%02d", domain))
	}

	vs.LogF("COMPLETED REAL\n")

}

func buildWRFDAWorkdir(vs *ctx.Context, phase runPhase, startDate time.Time) {
	if vs.Err != nil {
		return
	}

	folders := conf.Config.Folders
	workdir := vpath.New("localhost", startDate.Format("20060102"))

	vs.MkDir(workdir)

	vs.Link(conf.Config.Folders.GeodataDir, workdir.Join("geodata"))
	//vs.Link(conf.Config.Folders.CovarMatrixesDir, workdir.Join("matrix"))
	vs.Link(conf.Config.Folders.WPSPrg, workdir.Join("wpsprg"))
	vs.Link(conf.Config.Folders.WRFDAPrg, workdir.Join("wrfdaprg"))
	vs.Link(conf.Config.Folders.WRFMainRunPrg, workdir.Join("wrfprgrun"))
	vs.Link(conf.Config.Folders.WRFAssStepPrg, workdir.Join("wrfprgstep"))

	observationDir := workdir.Join("observations")
	gfsDir := workdir.Join("gfs")

	vs.MkDir(gfsDir)
	vs.MkDir(observationDir)

	assimStartDate := startDate.Add(2 * time.Duration(-3) * time.Hour)

	if phase == WPSPhase || phase == WPSThenDAPhase {
		// GFS
		gfsSources := folders.GFSArchive.Join(assimStartDate.Format("2006/01/02/1504"))
		for _, filename := range vs.ReadDir(gfsSources) {
			gfsFile := gfsSources.JoinP(filename)
			vs.Copy(gfsFile, gfsDir.JoinP(filename))
		}
	}

	// Observations - weather stations and radars

	if phase == DAPhase || phase == WPSThenDAPhase {
		cpObervations := func(dt time.Time) {
			vs.Copy(
				folders.ObservationsArchive.Join("ob.radar.%s", dt.Format("2006010215")),
				observationDir.Join("ob.radar.%s", dt.Format("2006010215")),
			)

			vs.Copy(
				folders.ObservationsArchive.Join("ob.ascii.%s", dt.Format("2006010215")),
				observationDir.Join("ob.ascii.%s", dt.Format("2006010215")),
			)
		}
		cpObervations(assimStartDate)
		cpObervations(assimStartDate.Add(3 * time.Hour))
		cpObervations(assimStartDate.Add(6 * time.Hour))
	}
}

func runWRFDA(vs *ctx.Context, phase runPhase, startDate time.Time, ds inputDataset, domainCount int) {
	if vs.Err != nil {
		return
	}

	endDate := startDate.Add(42 * time.Hour)

	if phase == WPSPhase || phase == WPSThenDAPhase {
		buildWPSDir(vs, startDate, endDate, ds)
		runWPS(vs, startDate, endDate)
		for step := 1; step <= 3; step++ {
			buildNamelistForReal(vs, startDate, endDate, step)
			runReal(vs, startDate, step, domainCount)
		}
	}

	if phase == DAPhase || phase == WPSThenDAPhase {
		for step := 1; step <= 3; step++ {
			buildDAStepDir(vs, phase, startDate, endDate, step, domainCount)
			runDAStep(vs, startDate, step, domainCount)

			buildWRFDir(vs, startDate, endDate, step, domainCount)
			runWRFStep(vs, startDate, step)
		}
	}
}

type runPhase int

const (
	// WPSPhase - run only WPS
	WPSPhase runPhase = iota
	// DAPhase - run only DA
	DAPhase
	// WPSThenDAPhase - run WPS followed by DA
	WPSThenDAPhase
)

type inputDataset int

const (
	// GFS ...
	GFS inputDataset = iota
	// IFS ...
	IFS
)

func readDomainCount(vs *ctx.Context, phase runPhase) (int, error) {
	nmlDir := conf.Config.Folders.NamelistsDir
	namelistToReadMaxDom := "namelist.run.wrf"
	if phase == WPSPhase || phase == WPSThenDAPhase {
		namelistToReadMaxDom = "namelist.wps"
	}

	fileName := nmlDir.Join(namelistToReadMaxDom).String()
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		return 0, fmt.Errorf("Cannot read %s:%w", fileName, err)
	}

	rows := strings.Split(string(content), "\n")
	//fsutil.LogF(rows)

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
	usage := "Usage: wrfda-run [-p WPS|DA|WPSDA] [-i GFS|IFS] <workdir> <startdate> <enddate>\nformat for dates: YYYYMMDDHH\ndefault for -p is WPSDA\ndefault for -i is GFS\n"

	phaseF := flag.String("p", "WPSDA", "")
	inputF := flag.String("i", "GFS", "")

	flag.Parse()

	var phase runPhase
	var input inputDataset

	if *phaseF == "WPS" {
		phase = WPSPhase
	} else if *phaseF == "DA" {
		phase = DAPhase
	} else if *phaseF == "WPSDA" {
		phase = WPSThenDAPhase
	} else {
		log.Fatalf("%s\nUnknown phase `%s`", usage, *phaseF)
	}

	if *inputF == "GFS" {
		input = GFS
	} else if *inputF == "IFS" {
		input = IFS
	} else {
		log.Fatalf("%s\nUnknown input dataset `%s`", usage, *phaseF)
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

	absWd, err := filepath.Abs(args[0])
	if err != nil {
		log.Fatal(err.Error())
	}
	workdir := vpath.New("localhost", absWd)
	conf.Init(workdir.Join("wrfda-runner.cfg"))

	wrfdaPrg = conf.Config.Folders.WRFDAPrg
	wrfPrgStep = conf.Config.Folders.WRFAssStepPrg
	wrfPrgMainRun = conf.Config.Folders.WRFMainRunPrg
	wpsPrg = conf.Config.Folders.WPSPrg
	matrixDir = conf.Config.Folders.CovarMatrixesDir

	/*vs.LogF(
		"RUN FOR DATES FROM %s TO %s\n",
		startDate.Format("2006010215"),
		endDate.Format("2006010215"),
	)
	*/

	for dt := startDate; dt.Unix() <= endDate.Unix(); dt = dt.Add(time.Hour * 24) {
		vs := ctx.Context{}
		domainCount, err := readDomainCount(&vs, phase)
		if err != nil {
			log.Fatal(err.Error())
		}

		vs.LogF("STARTING RUN FOR DATE %s\n", dt.Format("2006010215"))
		if !vs.Exists(vpath.New("localhost", ".")) {
			log.Fatalf("Directory not found: %s", workdir.String())
		}
		buildWRFDAWorkdir(&vs, phase, dt)
		if vs.Err != nil {
			log.Fatal(vs.Err)
		}
		vs = ctx.Context{}
		runWRFDA(&vs, phase, dt, input, domainCount)
		if vs.Err != nil {
			log.Fatal(vs.Err)
		}

		vs.LogF("RUN FOR DATE %s COMPLETED\n", dt.Format("2006010215"))
	}

}
