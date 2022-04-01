package conf

import (
	"fmt"
)

// RunPhase ...
type RunPhase int

const (
	// WPSPhase - run only WPS
	WPSPhase RunPhase = iota
	// DAPhase - run only DA
	DAPhase
	// WPSThenDAPhase - run WPS followed by DA
	WPSThenDAPhase
)

// InputDataset ...
type InputDataset int

const (
	Unspecified InputDataset = iota
	GFS
	IFS
)

func (phase *RunPhase) FromString(phaseS string) error {
	if phaseS == "WPS" {
		*phase = WPSPhase
		return nil
	}

	if phaseS == "DA" {
		*phase = DAPhase
		return nil
	}

	if phaseS == "WPSDA" {
		*phase = WPSThenDAPhase
		return nil
	}

	return fmt.Errorf("Unknown phase `%s`", phaseS)
}

func (input *InputDataset) FromString(inputS string) {
	if inputS == "GFS" {
		*input = GFS
		return
	}

	if inputS == "IFS" {
		*input = IFS
		return
	}

	*input = Unspecified
}
