package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"time"

	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/runner"

	"github.com/meteocima/virtual-server/vpath"
)

func main() {
	usage := "Usage: wrfda-run [-p WPS|DA|WPSDA] [-i GFS|IFS] <workdir> <startdate> <enddate>\nformat for dates: YYYYMMDDHH\ndefault for -p is WPSDA\ndefault for -i is GFS\n"

	phaseF := flag.String("p", "WPSDA", "")
	inputF := flag.String("i", "GFS", "")

	flag.Parse()

	var phase conf.RunPhase
	var input conf.InputDataset

	if *phaseF == "WPS" {
		phase = conf.WPSPhase
	} else if *phaseF == "DA" {
		phase = conf.DAPhase
	} else if *phaseF == "WPSDA" {
		phase = conf.WPSThenDAPhase
	} else {
		log.Fatalf("%s\nUnknown phase `%s`", usage, *phaseF)
	}

	if *inputF == "GFS" {
		input = conf.GFS
	} else if *inputF == "IFS" {
		input = conf.IFS
	} else {
		log.Fatalf("%s\nUnknown input dataset `%s`", usage, *phaseF)
	}

	args := flag.Args()
	if len(args) != 3 {
		log.Fatal(usage)
	}

	startDate, err := time.Parse("2006010215", args[1])
	if err != nil {
		log.Fatal(usage + err.Error() + "\n")
	}
	endDate, err := time.Parse("2006010215", args[2])
	if err != nil {
		log.Fatal(usage + err.Error() + "\n")
	}

	absWd, err := filepath.Abs(args[0])
	if err != nil {
		log.Fatal(err.Error())
	}

	wd := vpath.Local(absWd)
	cfgFile := wd.Join("wrfda-runner.cfg")

	err = runner.Init(cfgFile, wd)
	if err != nil {
		log.Fatal(err.Error())
	}
	err = runner.Run(startDate, endDate, wd, phase, input, os.Stdout, os.Stderr)
	if err != nil {
		log.Fatal(err.Error())
	}
}
