package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/meteocima/wrfassim/conf"
)

func TestMatchDownloadedData(t *testing.T) {
	err := conf.Init("./fixtures/wrfda-runner.cfg")
	assert.NoError(t, err)

	domains, err := readDomainCount(WPSPhase)
	assert.NoError(t, err)
	assert.Equal(t, 42, domains)

	domains2, err := readDomainCount(WPSPhase)
	assert.NoError(t, err)
	assert.Equal(t, 42, domains2)

	domainsDA, err := readDomainCount(DAPhase)
	assert.NoError(t, err)
	assert.Equal(t, 13, domainsDA)
}
