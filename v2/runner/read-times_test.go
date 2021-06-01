package runner

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/parro-it/fileargs"
	"github.com/stretchr/testify/assert"
)

func fixture(filePath string) string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot retrieve the source file path")
	} else {
		file = filepath.Dir(filepath.Dir(file))
	}

	return path.Join(file, "fixtures", filePath)
}

func TestMatchDownloadedData(t *testing.T) {

	fsys := os.DirFS(fixture("."))
	args, err := fileargs.ReadFile(fsys, "dates.txt")
	if !assert.NoError(t, err) {
		return
	}

	assert.Equal(t, 2, len(args.Periods))
	assert.Equal(t, "2020112600", args.Periods[0].Start.Format("2006010215"))
	assert.Equal(t, "2020112700", args.Periods[1].Start.Format("2006010215"))
	assert.Equal(t, time.Hour*24, args.Periods[0].Duration)
	assert.Equal(t, time.Hour*48, args.Periods[1].Duration)
	assert.Equal(t, "wrfda-runner.cfg", args.CfgPath)

}
