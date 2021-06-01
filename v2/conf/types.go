package conf

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
	// GFS ...
	GFS InputDataset = iota
	// IFS ...
	IFS
)
