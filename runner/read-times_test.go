package runner

import (
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

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
	dateFile := fixture("dates.txt")
	dates, err := ReadTimes(dateFile)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(dates))
	assert.Equal(t, "2020112600", dates[0].Start.Format("2006010215"))
	assert.Equal(t, "2020112700", dates[1].Start.Format("2006010215"))
	assert.Equal(t, time.Hour*24, dates[0].Duration)
	assert.Equal(t, time.Hour*48, dates[1].Duration)

}
