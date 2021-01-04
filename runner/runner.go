package runner

import (
	"fmt"
	"io"
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

func readDomainCount(vs *ctx.Context, phase conf.RunPhase) int {
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
				vs.Err = fmt.Errorf("Malformed max_dom property in `%s`: %s", fileName.String(), trimdLine)
				return 0
			}
			valueS := strings.Trim(fields[1], " \t,")
			value, err := strconv.Atoi(valueS)
			if err != nil {
				vs.Err = fmt.Errorf("Cannot convert max_dom `%s` to integer: %w", valueS, err)
				return 0
			}
			return value
		}
	}

	vs.Err = fmt.Errorf("max_dom property not found in %s", fileName)
	return 0
}

// Init ...
func Init(cfgFile, workdir vpath.VirtualPath) error {
	folders.Root = workdir

	err := vsConfig.Init(cfgFile.Path)
	if err != nil {
		return err
	}

	err = conf.Init(cfgFile)
	if err != nil {
		return err
	}

	folders.Cfg = conf.Config.Folders
	return nil
}

// RemoveRunFolder ...
func RemoveRunFolder(startDate time.Time, workdir vpath.VirtualPath, logWriter io.Writer, detailLogWriter io.Writer) error {
	vs := ctx.Context{
		Log:       logWriter,
		DetailLog: detailLogWriter,
	}

	dtWorkdir := folders.WorkdirForDate(startDate)

	if vs.Exists(dtWorkdir) {
		vs.RmDir(dtWorkdir)
	}

	return vs.Err
}

// Run ...
func Run(startDate, endDate time.Time, workdir vpath.VirtualPath, phase conf.RunPhase, input conf.InputDataset,
	logWriter io.Writer, detailLogWriter io.Writer,
) error {
	vs := ctx.Context{
		Log:       logWriter,
		DetailLog: detailLogWriter,
	}

	if !vs.Exists(workdir) {
		return fmt.Errorf("Directory not found: %s", workdir.String())
	}

	domainCount := readDomainCount(&vs, phase)

	for dt := startDate; dt.Unix() <= endDate.Unix(); dt = dt.Add(time.Hour * 24) {
		vs.LogF("STARTING RUN FOR DATE %s\n", dt.Format("2006010215"))

		buildWorkdirForDate(&vs, phase, dt)
		runWRFDA(&vs, phase, dt, input, domainCount)
		if vs.Err == nil {
			vs.LogF("RUN FOR DATE %s COMPLETED\n", dt.Format("2006010215"))
		}
	}

	return vs.Err
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
