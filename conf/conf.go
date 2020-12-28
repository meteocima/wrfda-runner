package conf

// This module contains data structures
// used to keep configuration variables
// for the command.

import (
	"fmt"
	"path"

	"github.com/meteocima/virtual-server/vpath"

	"github.com/BurntSushi/toml"
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
	fmt.Println(Config.Folders)
	return err
}
