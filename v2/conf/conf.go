package conf

// This module contains data structures
// used to keep configuration variables
// for the command.

import (
	"fmt"
	"path"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/meteocima/namelist-prepare/namelist"
	"github.com/meteocima/virtual-server/ctx"
	"github.com/meteocima/virtual-server/vpath"
)

// FoldersConf contains path of all
// files and directories somehow needed by the command
type FoldersConf struct {
	GeodataDir          vpath.VirtualPath
	CovarMatrixesDir    vpath.VirtualPath
	WPSPrg              vpath.VirtualPath
	WRFDAPrg            vpath.VirtualPath
	WRFMainRunPrg       vpath.VirtualPath
	WRFAssStepPrg       vpath.VirtualPath
	GFSArchive          vpath.VirtualPath
	ObservationsArchive vpath.VirtualPath
	NamelistsDir        vpath.VirtualPath
}

// ProcsConf ...
type ProcsConf struct {
	// GeogridProcCount ...
	GeogridProcCount string

	// MetgridProcCount ...
	MetgridProcCount string

	// WrfstepProcCount ...
	WrfstepProcCount string

	// WrfdaProcCount ...
	WrfdaProcCount string

	// RealProcCount ...
	RealProcCount string
}

// EnvVars is a set of environment variables
// that will be passed to every command executed
type EnvVars map[string]string

// ToSlice converts variables to a slice of string, each one
// in the format NAME=VALUE
func (vars EnvVars) ToSlice() []string {
	res := make([]string, len(vars))
	i := 0
	for name, val := range vars {
		res[i] = fmt.Sprintf("%s=%s", name, val)
		i++
	}
	return res
}

// Configuration contains all configuration
// sub structures
type Configuration struct {
	Folders FoldersConf
	Procs   ProcsConf
	Env     EnvVars
}

// Config is the runtime configuration readed from file.
var Config Configuration

// Init initializes the system by reading configuration
// from `confPath` file.
func Init(confFile vpath.VirtualPath) error {

	_, err := toml.DecodeFile(confFile.Path, &Config)
	confDir := confFile.Dir()

	if !path.IsAbs(Config.Folders.GeodataDir.Path) {
		Config.Folders.GeodataDir = confDir.JoinP(Config.Folders.GeodataDir)
	}

	if !path.IsAbs(Config.Folders.CovarMatrixesDir.Path) {
		Config.Folders.CovarMatrixesDir = confDir.JoinP(Config.Folders.CovarMatrixesDir)
	}

	if !path.IsAbs(Config.Folders.WPSPrg.Path) {
		Config.Folders.WPSPrg = confDir.JoinP(Config.Folders.WPSPrg)
	}

	if !path.IsAbs(Config.Folders.WRFDAPrg.Path) {
		Config.Folders.WRFDAPrg = confDir.JoinP(Config.Folders.WRFDAPrg)
	}

	if !path.IsAbs(Config.Folders.WRFMainRunPrg.Path) {
		Config.Folders.WRFMainRunPrg = confDir.JoinP(Config.Folders.WRFMainRunPrg)
	}

	if !path.IsAbs(Config.Folders.WRFAssStepPrg.Path) {
		Config.Folders.WRFAssStepPrg = confDir.JoinP(Config.Folders.WRFAssStepPrg)
	}

	if !path.IsAbs(Config.Folders.GFSArchive.Path) {
		Config.Folders.GFSArchive = confDir.JoinP(Config.Folders.GFSArchive)
	}

	if !path.IsAbs(Config.Folders.ObservationsArchive.Path) {
		Config.Folders.ObservationsArchive = confDir.JoinP(Config.Folders.ObservationsArchive)
	}

	if !path.IsAbs(Config.Folders.NamelistsDir.Path) {
		Config.Folders.NamelistsDir = confDir.JoinP(Config.Folders.NamelistsDir)
	}
	//fmt.Println(Config.Folders)
	return err
}

// NamelistFile ...
func NamelistFile(source string) vpath.VirtualPath {
	return Config.Folders.NamelistsDir.Join(source)
}

// RenderNameList ...
func RenderNameList(vs *ctx.Context, source string, target vpath.VirtualPath, args namelist.Args) {
	if vs.Err != nil {
		return
	}

	tmplFile := vs.ReadString(NamelistFile(source))

	//args.Hours = int(args.End.Sub(args.Start).Hours())

	tmpl := namelist.Tmpl{}
	tmpl.ReadTemplateFrom(strings.NewReader(tmplFile))

	var renderedNamelist strings.Builder
	tmpl.RenderTo(args, &renderedNamelist)
	vs.WriteString(target, renderedNamelist.String())
}
