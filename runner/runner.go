package runner

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	vsConfig "github.com/meteocima/virtual-server/config"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/folders"
	/*

		"github.com/meteocima/namelist-prepare/namelist"
		"github.com/meteocima/virtual-server/ctx"
		"github.com/meteocima/virtual-server/vpath"

		"github.com/meteocima/virtual-server/connection"
	*/)

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

func Init(workdir vpath.VirtualPath) {
	folders.Root = workdir
	cfgFile := workdir.Join("wrfda-runner.cfg")

	err := vsConfig.Init(cfgFile.Path)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = conf.Init(cfgFile)
	if err != nil {
		log.Fatal(err.Error())
	}

	folders.Cfg = conf.Config.Folders

}

func Run(startDate, endDate time.Time, workdir vpath.VirtualPath, phase conf.RunPhase, input conf.InputDataset) {

	vs := ctx.Context{}
	domainCount, err := readDomainCount(&vs, phase)
	if err != nil {
		log.Fatal(err.Error())
	}

	for dt := startDate; dt.Unix() <= endDate.Unix(); dt = dt.Add(time.Hour * 24) {
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

func buildWorkdirForDate(vs *ctx.Context, phase conf.RunPhase, startDate time.Time) {
	if vs.Err != nil {
		return
	}

	//folders := conf.Config.Folders
	workdir := folders.WorkdirForDate(startDate)

	vs.MkDir(workdir)

	vs.Link(conf.Config.Folders.GeodataDir, workdir.Join("geodata"))
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
