package conf

// This module contains data structures
// used to keep configuration variables
// for the command.

import (
	"github.com/BurntSushi/toml"
	"github.com/meteocima/wrfassim/fsutil"
)

// FoldersConf contains path of all
// files and directories somehow needed by the command
type FoldersConf struct {
	GeodataDir          fsutil.Path
	CovarMatrixesDir    fsutil.Path
	WPSPrg              fsutil.Path
	WRFDAPrg            fsutil.Path
	WRFMainRunPrg       fsutil.Path
	WRFAssStepPrg       fsutil.Path
	GFSArchive          fsutil.Path
	ObservationsArchive fsutil.Path
	NamelistsDir        fsutil.Path
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
	return err
}
