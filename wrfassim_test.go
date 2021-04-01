package main

import (
	"path"
	"path/filepath"
	"runtime"
)

func fixtures() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot retrieve the source file path")
	} else {
		file = filepath.Dir(file)
	}

	return path.Join(file, "fixtures")
}

/*
func TestMatchDownloadedData(t *testing.T) {
	err := os.Chdir(fixtures())
	assert.NoError(t, err)

	err = conf.Init(fixtures() + "/testrun/wrfda-runner.cfg")
	assert.NoError(t, err)

	domains, err := ReadDomainCount(conf.WPSPhase)
	assert.NoError(t, err)
	assert.Equal(t, 3, domains)

	domains2, err := ReadDomainCount(conf.DAPhase)
	assert.NoError(t, err)
	assert.Equal(t, 3, domains2)

	domainsDA, err := ReadDomainCount(conf.WPSThenDAPhase)
	assert.NoError(t, err)
	assert.Equal(t, 3, domainsDA)
}
*/
