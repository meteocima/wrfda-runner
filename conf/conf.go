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
func Init(confPath string) error {
	_, err := toml.DecodeFile(confPath, &Config)
	confDir := path.Dir(confPath)

	confFile := vpath.New("timoteo", confDir)
	if !path.IsAbs(Config.Folders.GeodataDir.String()) {
		Config.Folders.GeodataDir = confFile.JoinP(Config.Folders.GeodataDir)
	}

	if !path.IsAbs(Config.Folders.CovarMatrixesDir.String()) {
		Config.Folders.CovarMatrixesDir = confFile.JoinP(Config.Folders.CovarMatrixesDir)
	}

	if !path.IsAbs(Config.Folders.WPSPrg.String()) {
		Config.Folders.WPSPrg = confFile.JoinP(Config.Folders.WPSPrg)
	}

	if !path.IsAbs(Config.Folders.WRFDAPrg.String()) {
		Config.Folders.WRFDAPrg = confFile.JoinP(Config.Folders.WRFDAPrg)
	}

	if !path.IsAbs(Config.Folders.WRFMainRunPrg.String()) {
		Config.Folders.WRFMainRunPrg = confFile.JoinP(Config.Folders.WRFMainRunPrg)
	}

	if !path.IsAbs(Config.Folders.WRFAssStepPrg.String()) {
		Config.Folders.WRFAssStepPrg = confFile.JoinP(Config.Folders.WRFAssStepPrg)
	}

	if !path.IsAbs(Config.Folders.GFSArchive.String()) {
		Config.Folders.GFSArchive = confFile.JoinP(Config.Folders.GFSArchive)
	}

	if !path.IsAbs(Config.Folders.ObservationsArchive.String()) {
		Config.Folders.ObservationsArchive = confFile.JoinP(Config.Folders.ObservationsArchive)
	}

	if !path.IsAbs(Config.Folders.NamelistsDir.String()) {
		Config.Folders.NamelistsDir = confFile.JoinP(Config.Folders.NamelistsDir)
	}
	fmt.Println(Config.Folders)
	return err
}
