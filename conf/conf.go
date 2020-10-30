package conf

import (
	"github.com/BurntSushi/toml"
	"github.com/meteocima/wrfassim/fsutil"
)

/*
type SmtpConf struct {
	SmtpHost string
	Port     uint64
	MailFrom string
	Username string
	Password string
}
*/

// FoldersConf ...
type FoldersConf struct {
	GeodataDir          fsutil.Path
	CovarMatrixesDir    fsutil.Path
	WPSPrg              fsutil.Path
	WRFDAPrg            fsutil.Path
	WRFMainRunPrg       fsutil.Path
	WRFAssStepPrg       fsutil.Path
	GFSArchive          fsutil.Path
	ObservationsArchive fsutil.Path
}

// Configuration ...
type Configuration struct {
	//Smtp    SmtpConf
	Folders FoldersConf
}

// Config ...
var Config Configuration

// Init ...
func Init(confPath string) error {
	_, err := toml.DecodeFile(confPath, &Config)
	return err

}
