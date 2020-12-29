package folders

import (
	"time"

	"github.com/meteocima/virtual-server/vpath"
	"github.com/meteocima/wrfda-runner/conf"
)

var Root vpath.VirtualPath
var Cfg conf.FoldersConf

func InputsDir(startDate time.Time) vpath.VirtualPath {
	return Root.Join("inputs/%s", startDate.Format("20060102"))
}

func WPSWorkDir(startDate time.Time) vpath.VirtualPath {
	return WorkdirForDate(startDate).Join("wps")
}

func WRFWorkDir(start time.Time, cycle int) vpath.VirtualPath {
	dtStart := start.Add(3 * time.Duration(cycle-3) * time.Hour)
	return WorkdirForDate(start).Join("wrf%02d", dtStart.Hour())
}

func DAWorkDir(startDate time.Time, domain, cycle int) vpath.VirtualPath {
	assimDate := startDate.Add(3 * time.Duration(cycle-3) * time.Hour)

	return WorkdirForDate(startDate).Join("da%02d_d%02d", assimDate.Hour(), domain)
}

func NamelistFile(source string) vpath.VirtualPath {
	return Cfg.NamelistsDir.Join(source)
}

func DAWorkdir(phase conf.RunPhase, startDate time.Time) vpath.VirtualPath {
	return vpath.FromS("")
}

func WorkdirForDate(startDate time.Time) vpath.VirtualPath {
	return Root.Join(startDate.Format("20060102"))
}

func GFSSources(startDate time.Time) vpath.VirtualPath {
	// assimStartDate is the date of the first cycle assimilation
	assimStartDate := startDate.Add(-6 * time.Hour)
	gfsSources := Cfg.GFSArchive.Join(
		assimStartDate.Format("2006/01/02/1504"),
	)
	return gfsSources
}

func RadarObsForDate(startDate time.Time, cycle int) vpath.VirtualPath {
	workdir := WorkdirForDate(startDate)
	observationDir := workdir.Join("observations")

	// dt is the date of the first cycle assimilation
	dt := startDate.Add(time.Duration(-6+3*(cycle-1)) * time.Hour)
	return observationDir.Join("ob.radar.%s", dt.Format("2006010215"))
}

func StationsObsForDate(startDate time.Time, cycle int) vpath.VirtualPath {
	workdir := WorkdirForDate(startDate)
	observationDir := workdir.Join("observations")

	// dt is the date of the first cycle assimilation
	dt := startDate.Add(time.Duration(-6+3*(cycle-1)) * time.Hour)
	return observationDir.Join("ob.ascii.%s", dt.Format("2006010215"))
}

func RadarObsArchive(startDate time.Time, cycle int) vpath.VirtualPath {
	// dt is the date of the first cycle assimilation
	dt := startDate.Add(time.Duration(-6+3*(cycle-1)) * time.Hour)
	return Cfg.ObservationsArchive.Join("ob.radar.%s", dt.Format("2006010215"))
}

func StationsObsArchive(startDate time.Time, cycle int) vpath.VirtualPath {
	// dt is the date of the first cycle assimilation
	dt := startDate.Add(time.Duration(-6+3*(cycle-1)) * time.Hour)
	return Cfg.ObservationsArchive.Join("ob.ascii.%s", dt.Format("2006010215"))
}
