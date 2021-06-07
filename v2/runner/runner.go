package runner

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	vsConfig "github.com/meteocima/virtual-server/config"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/v2/conf"
	"github.com/meteocima/wrfda-runner/v2/folders"
)

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
				vs.Err = fmt.Errorf("malformed max_dom property in `%s`: %s", fileName.String(), trimdLine)
				return 0
			}
			valueS := strings.Trim(fields[1], " \t,")
			value, err := strconv.Atoi(valueS)
			if err != nil {
				vs.Err = fmt.Errorf("cannot convert max_dom `%s` to integer: %w", valueS, err)
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
	vs := ctx.New(os.Stdin, logWriter, detailLogWriter)

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
	vs := ctx.New(os.Stdin, logWriter, detailLogWriter)

	if !vs.Exists(workdir) {
		return fmt.Errorf("directory not found: %s", workdir.String())
	}

	domainCount := ReadDomainCount(vs, phase)

	//for dt := startDate; dt.Unix() < endDate.Unix(); dt = dt.Add(time.Hour * 24) {
	vs.LogInfo("STARTING RUN FOR DATE %s\n", startDate.Format("2006010215"))
	dir := folders.WorkdirForDate(startDate)
	BuildWorkdirForDate(vs, dir, phase, startDate, endDate)
	runWRFDA(vs, phase, startDate, endDate, input, domainCount)
	if vs.Err == nil {
		vs.LogInfo("RUN FOR DATE %s COMPLETED\n", startDate.Format("2006010215"))
	}
	//}

	return vs.Err
}

func runWRFDA(vs *ctx.Context, phase conf.RunPhase, startDate, endDate time.Time, ds conf.InputDataset, domainCount int) {
	if vs.Err != nil {
		return
	}

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
			BuildDAStepDir(vs, startDate, endDate, cycle, "simulation")
			RunDAStep(vs, startDate, cycle)

			BuildWRFDir(vs, startDate, endDate, cycle, "simulation")
			RunWRFStep(vs, startDate, cycle)
		}
	}
}

// StepType ...
type StepType int

const (
	// BuildDA ...
	BuildDA StepType = iota
	// BuildWRF ...
	BuildWRF
	// RunDA ...
	RunDA
	// RunWRF ...
	RunWRF
)

// RunSingleStep ...
func RunSingleStep(startDate time.Time, ds conf.InputDataset, cycle int, stepType StepType, logWriter io.Writer, detailLogWriter io.Writer) {
	endDate := startDate.Add(48 * time.Hour)
	vs := ctx.New(os.Stdin, logWriter, detailLogWriter)
	//domainCount := ReadDomainCount(vs, phase)

	switch stepType {
	case BuildDA:
		BuildDAStepDir(vs, startDate, endDate, cycle, "simulation")

	case BuildWRF:
		BuildWRFDir(vs, startDate, endDate, cycle, "simulation")

	case RunDA:
		RunDAStep(vs, startDate, cycle)

	case RunWRF:
		RunWRFStep(vs, startDate, cycle)
	default:
		panic("unknown step type")
	}
}

func cpObservations(vs *ctx.Context, cycle int, startDate time.Time, host string) {
	vs.LogInfo("Copy radar for date %s\n", startDate.Format("200601021504"))

	src := folders.RadarObsArchive(startDate, cycle)
	dst := folders.RadarObsForDate(startDate, cycle, host)

	vs.LogInfo("Copy radar for cycle %d to %s: %s -> %s\n", cycle, host, src, dst)
	vs.Copy(src, dst)
	vs.LogInfo("Copy done\n")

	src = folders.StationsObsArchive(startDate, cycle)
	if vs.Exists(src) {
		dst = folders.StationsObsForDate(startDate, cycle, host)
		vs.LogInfo("Copy observations for cycle %d to %s: %s -> %s\n", cycle, host, src, dst)
		vs.Copy(src, dst)
		vs.LogInfo("Copy done\n")
	}
}

// BuildWorkdirForDate ...
func BuildWorkdirForDate(vs *ctx.Context, workdir vpath.VirtualPath, phase conf.RunPhase, startDate, endDate time.Time) {
	if vs.Err != nil {
		return
	}

	h := workdir.Host
	geodataDir := vpath.New(h, conf.Config.Folders.GeodataDir.Path)
	wpsPrg := vpath.New(h, conf.Config.Folders.WPSPrg.Path)
	wrfdaPrg := vpath.New(h, conf.Config.Folders.WRFDAPrg.Path)
	wrfMainRunPrg := vpath.New(h, conf.Config.Folders.WRFMainRunPrg.Path)
	wrfAssStepPrg := vpath.New(h, conf.Config.Folders.WRFAssStepPrg.Path)

	vs.MkDir(workdir)

	vs.Link(geodataDir, workdir.Join("geodata"))
	vs.Link(wpsPrg, workdir.Join("wpsprg"))
	vs.Link(wrfdaPrg, workdir.Join("wrfdaprg"))
	vs.Link(wrfMainRunPrg, workdir.Join("wrfprgrun"))
	vs.Link(wrfAssStepPrg, workdir.Join("wrfprgstep"))

	observationDir := workdir.Join("observations")
	gfsDir := workdir.Join("gfs")

	vs.MkDir(gfsDir)
	vs.MkDir(observationDir)

	vs.LogInfo("Copy GFS files to %s\n", h)

	files := make(chan vpath.VirtualPath)
	var alldone sync.WaitGroup

	if phase == conf.WPSPhase || phase == conf.WPSThenDAPhase {
		// GFS

		alldone = sync.WaitGroup{}
		alldone.Add(20)

		for i := 0; i < 20; i++ {
			go func() {
				for f := range files {
					vs.LogInfo("Copy GFS file %s", gfsDir.Join(f.Filename()).String())
					vs.Copy(f, gfsDir.Join(f.Filename()))
				}
				alldone.Done()
			}()
		}

		gfsSources := folders.GFSSources(startDate)
		for _, gfsFile := range vs.ReadDir(gfsSources) {
			if vs.IsFile(gfsFile) {
				files <- gfsFile
			}
		}
		close(files)
		alldone.Wait()
	}

	vs.LogInfo("Copy done\n")

	// Observations - weather stations and radars
	if phase == conf.DAPhase || phase == conf.WPSThenDAPhase {
		alldone = sync.WaitGroup{}
		alldone.Add(3)

		for i := 0; i < 3; i++ {
			go func(i int) {
				cpObservations(vs, i+1, startDate, h)
				alldone.Done()
			}(i)
		}
		alldone.Wait()
	}
}
