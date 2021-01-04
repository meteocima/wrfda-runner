package conf

// This module contains data structures
// used to keep configuration variables
// for the command.

import (
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

// Configuration contains all configuration
// sub structure (at the moment, only a FoldersConf struct.)
type Configuration struct {
	Folders FoldersConf
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

func NamelistFile(source string) vpath.VirtualPath {
	return Config.Folders.NamelistsDir.Join(source)
}

func RenderNameList(vs *ctx.Context, source string, target vpath.VirtualPath, args namelist.Args) {
	if vs.Err != nil {
		return
	}

	tmplFile := vs.ReadString(NamelistFile(source))

	args.Hours = int(args.End.Sub(args.Start).Hours())

	tmpl := namelist.Tmpl{}
	tmpl.ReadTemplateFrom(strings.NewReader(tmplFile))

	var renderedNamelist strings.Builder
	tmpl.RenderTo(args, &renderedNamelist)
	vs.WriteString(target, renderedNamelist.String())
}
