package main

import (
	"flag"
	"fmt"
	"log"

	"path/filepath"
	"strconv"
	"strings"
	"time"

	vsConfig "github.com/meteocima/virtual-server/config"
	"github.com/meteocima/wrfassim/conf"
	"github.com/meteocima/wrfassim/folders"

	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"

	"github.com/meteocima/virtual-server/connection"
)

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

	tmplFile := vs.ReadString(folders.NamelistFile(source))

	args.Hours = int(args.End.Sub(args.Start).Hours())

	tmpl := namelist.Tmpl{}
	tmpl.ReadTemplateFrom(strings.NewReader(tmplFile))

	var renderedNamelist strings.Builder
	tmpl.RenderTo(args, &renderedNamelist)
	vs.WriteString(target, renderedNamelist.String())
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
		[]string{"-n", geogridProcCount, "./geogrid.exe"},
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
		[]string{"-n", metgridProcCount, "./metgrid.exe"},
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

func buildWRFDir(vs *ctx.Context, start, end time.Time, step int, domainCount int) {
	if vs.Err != nil {
		return
	}

	wrfPrg := folders.Cfg.WRFAssStepPrg
	nameListName := "namelist.step.wrf"

	dtStart := start.Add(3 * time.Duration(step-3) * time.Hour)
	dtEnd := dtStart.Add(3 * time.Hour)

	if step == 3 {
		dtEnd = end
		wrfPrg = folders.Cfg.WRFMainRunPrg
		nameListName = "namelist.run.wrf"
	}

	wrfDir := folders.WRFWorkDir(start, step)

	vs.MkDir(wrfDir)

	// boundary from same cycle da dir for domain 1
	daBdy := folders.DAWorkDir(start, 1, step).Join("wrfbdy_d01")
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

	vs.Copy(
		folders.NamelistFile(wrfvar),
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

func buildDADirInDomain(vs *ctx.Context, phase conf.RunPhase, start, end time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}
	assimDate := start.Add(3 * time.Duration(step-3) * time.Hour)

	// prepare da dir
	daDir := folders.DAWorkDir(start, domain, step)

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

	vs.Link(folders.RadarObsForDate(assimDate, step), daDir.Join("ob.radar"))
	vs.Link(folders.StationsObsForDate(assimDate, step), daDir.Join("ob.ascii"))
}

func runWRFStep(vs *ctx.Context, start time.Time, step int) {
	if vs.Err != nil {
		return
	}

	vs.LogF("START WRF STEP %d\n", step)

	wrfDir := folders.WRFWorkDir(start, step)

	vs.Exec(
		vpath.New(wrfDir.Host, "mpirun"),
		[]string{"-n", wrfstepProcCount, "./wrf.exe"},
		connection.RunOptions{OutFromLog: wrfDir.Join("rsl.out.0000"),
			Cwd: wrfDir,
		},
	)

	vs.LogF("COMPLETED WRF STEP %d\n", step)
}

func runDAStepInDomain(vs *ctx.Context, start time.Time, step, domain int) {
	if vs.Err != nil {
		return
	}

	vs.LogF("START DA STEP %d in DOMAIN %d\n", step, domain)

	daDir := folders.DAWorkDir(start, domain, step)

	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", wrfdaProcCount, "./da_wrfvar.exe"},
		connection.RunOptions{
			OutFromLog: daDir.Join("rsl.out.0000"),
			Cwd:        daDir,
		},
	)

	if domain == 1 {
		vs.Exec(daDir.Join("./da_update_bc.exe"), []string{}, connection.RunOptions{
			Cwd: daDir,
		})
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

func buildDAStepDir(vs *ctx.Context, phase conf.RunPhase, start, end time.Time, step int, domainCount int) {
	if vs.Err != nil {
		return
	}

	for domain := 1; domain <= domainCount; domain++ {
		buildDADirInDomain(vs, phase, start, end, step, domain)
	}

}

func buildNamelistForReal(vs *ctx.Context, start, end time.Time, step int) {
	assimStartDate := start.Add(3 * time.Duration(step-3) * time.Hour)
	wpsDir := folders.WPSWorkDir(start)

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
	wpsDir := folders.WPSWorkDir(startDate)

	vs.LogF("START REAL\n")

	vs.Exec(
		vpath.New("localhost", "mpirun"),
		[]string{"-n", realProcCount, "./real.exe"},
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

func buildWorkdirForDate(vs *ctx.Context, phase conf.RunPhase, startDate time.Time) {
	if vs.Err != nil {
		return
	}

	//folders := conf.Config.Folders
	workdir := folders.WorkdirForDate(startDate)

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

	if phase == conf.WPSPhase || phase == conf.WPSThenDAPhase {
		// GFS
		gfsSources := folders.GFSSources(startDate)
		for _, gfsFile := range vs.ReadDir(gfsSources) {
			vs.Copy(gfsFile, gfsDir.Join(gfsFile.Filename()))
		}
	}

	// Observations - weather stations and radars
	if phase == conf.DAPhase || phase == conf.WPSThenDAPhase {
		cpObservations(vs, 1, startDate)
		cpObservations(vs, 2, startDate)
		cpObservations(vs, 3, startDate)
	}
}

func cpObservations(vs *ctx.Context, cycle int, startDate time.Time) {
	vs.Copy(
		folders.RadarObsArchive(startDate, cycle),
		folders.RadarObsForDate(startDate, cycle),
	)

	vs.Copy(
		folders.StationsObsArchive(startDate, cycle),
		folders.StationsObsForDate(startDate, cycle),
	)
}

func runWRFDA(vs *ctx.Context, phase conf.RunPhase, startDate time.Time, ds conf.InputDataset, domainCount int) {
	if vs.Err != nil {
		return
	}

	endDate := startDate.Add(42 * time.Hour)

	if phase == conf.WPSPhase || phase == conf.WPSThenDAPhase {
		buildWPSDir(vs, startDate, endDate, ds)
		runWPS(vs, startDate, endDate)
		for step := 1; step <= 3; step++ {
			buildNamelistForReal(vs, startDate, endDate, step)
			runReal(vs, startDate, step, domainCount)
		}
	}

	if phase == conf.DAPhase || phase == conf.WPSThenDAPhase {
		for step := 1; step <= 3; step++ {
			buildDAStepDir(vs, phase, startDate, endDate, step, domainCount)
			runDAStep(vs, startDate, step, domainCount)

			buildWRFDir(vs, startDate, endDate, step, domainCount)
			runWRFStep(vs, startDate, step)
		}
	}
}

func readDomainCount(vs *ctx.Context, phase conf.RunPhase) (int, error) {
	nmlDir := conf.Config.Folders.NamelistsDir
	namelistToReadMaxDom := "namelist.run.wrf"
	if phase == conf.WPSPhase || phase == conf.WPSThenDAPhase {
		namelistToReadMaxDom = "namelist.wps"
	}

	fileName := nmlDir.Join(namelistToReadMaxDom)
	content := vs.ReadString(fileName)

	rows := strings.Split(string(content), "\n")
	//fsutil.LogF(rows)

	for _, line := range rows {
		trimdLine := strings.TrimLeft(line, " \t")
		if strings.HasPrefix(trimdLine, "max_dom") {
			fields := strings.Split(trimdLine, "=")
			if len(fields) < 2 {
				return 0, fmt.Errorf("Malformed max_dom property in `%s`: %s", fileName.String(), trimdLine)
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

	var phase conf.RunPhase
	var input conf.InputDataset

	if *phaseF == "WPS" {
		phase = conf.WPSPhase
	} else if *phaseF == "DA" {
		phase = conf.DAPhase
	} else if *phaseF == "WPSDA" {
		phase = conf.WPSThenDAPhase
	} else {
		log.Fatalf("%s\nUnknown phase `%s`", usage, *phaseF)
	}

	if *inputF == "GFS" {
		input = conf.GFS
	} else if *inputF == "IFS" {
		input = conf.IFS
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
	folders.Root = workdir
	cfgFile := workdir.Join("wrfda-runner.cfg")

	err = vsConfig.Init(cfgFile.Path)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = conf.Init(cfgFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	folders.Cfg = conf.Config.Folders

	/*vs.LogF(
		"RUN FOR DATES FROM %s TO %s\n",
		startDate.Format("2006010215"),
		endDate.Format("2006010215"),
	)
	*/

	vs := ctx.Context{}
	domainCount, err := readDomainCount(&vs, phase)

	for dt := startDate; dt.Unix() <= endDate.Unix(); dt = dt.Add(time.Hour * 24) {
		if err != nil {
			log.Fatal(err.Error())
		}

		vs.LogF("STARTING RUN FOR DATE %s\n", dt.Format("2006010215"))
		if !vs.Exists(workdir) {
			log.Fatalf("Directory not found: %s", workdir.String())
		}
		buildWorkdirForDate(&vs, phase, dt)
		if vs.Err != nil {
			log.Fatal(vs.Err)
		}
		runWRFDA(&vs, phase, dt, input, domainCount)
		if vs.Err != nil {
			log.Fatal(vs.Err)
		}

		vs.LogF("RUN FOR DATE %s COMPLETED\n", dt.Format("2006010215"))
	}

}
