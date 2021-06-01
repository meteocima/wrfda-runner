package runner

import (
	"os"

	"github.com/parro-it/fileargs"
)

// ReadTimes ...
func ReadTimes(file string) (*fileargs.FileArguments, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	fsys := os.DirFS(cwd)
	return fileargs.ReadFile(fsys, file)
}
