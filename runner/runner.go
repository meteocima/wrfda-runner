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

// ReadDomainCount ...
func ReadDomainCount(vs *ctx.Context, phase conf.RunPhase) int {
	if vs.Err != nil {
		return 0
	}
	nmlDir := conf.Config.Folders.NamelistsDir
	namelistToReadMaxDom := "namelist.step.wrf"
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
	vs := ctx.New(logWriter, detailLogWriter)

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
	vs := ctx.New(logWriter, detailLogWriter)

	if !vs.Exists(workdir) {
		return fmt.Errorf("Directory not found: %s", workdir.String())
	}

	domainCount := ReadDomainCount(vs, phase)

	for dt := startDate; dt.Unix() <= endDate.Unix(); dt = dt.Add(time.Hour * 24) {
		vs.LogInfo("STARTING RUN FOR DATE %s\n", dt.Format("2006010215"))

		workdir := folders.WorkdirForDate(startDate)
		BuildWorkdirForDate(vs, workdir, phase, dt)
		runWRFDA(vs, phase, dt, input, domainCount)
		if vs.Err == nil {
			vs.LogInfo("RUN FOR DATE %s COMPLETED\n", dt.Format("2006010215"))
		}
	}

	return vs.Err
}

func runWRFDA(vs *ctx.Context, phase conf.RunPhase, startDate time.Time, ds conf.InputDataset, domainCount int) {
	if vs.Err != nil {
		return
	}

	endDate := startDate.Add(48 * time.Hour)

	if phase == conf.WPSPhase || phase == conf.WPSThenDAPhase {
		BuildWPSDir(vs, startDate, endDate, ds)
		RunWPS(vs, startDate, endDate)
		for cycle := 1; cycle <= 3; cycle++ {
			BuildNamelistForReal(vs, startDate, endDate, cycle)
			RunReal(vs, startDate, cycle, phase)
		}
	}

	if phase == conf.DAPhase || phase == conf.WPSThenDAPhase {
		for cycle := 1; cycle <= 3; cycle++ {
			BuildDAStepDir(vs, startDate, endDate, cycle)
			RunDAStep(vs, startDate, cycle)

			BuildWRFDir(vs, startDate, endDate, cycle)
			RunWRFStep(vs, startDate, cycle)
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

// BuildWorkdirForDate ...
func BuildWorkdirForDate(vs *ctx.Context, workdir vpath.VirtualPath, phase conf.RunPhase, startDate time.Time) {
	if vs.Err != nil {
		return
	}

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
			if vs.IsFile(gfsFile) {
				vs.Copy(gfsFile, gfsDir.Join(gfsFile.Filename()))
			}
		}
	}

	// Observations - weather stations and radars
	if phase == conf.DAPhase || phase == conf.WPSThenDAPhase {
		cpObservations(vs, 1, startDate)
		cpObservations(vs, 2, startDate)
		cpObservations(vs, 3, startDate)
	}
}
