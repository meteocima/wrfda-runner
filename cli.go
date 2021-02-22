package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"time"

	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/runner"

	"github.com/meteocima/virtual-server/vpath"
)

func main() {
	usage := "Usage: wrfda-run [-p WPS|DA|WPSDA] [-i GFS|IFS] <workdir> <startdate> <enddate>\nformat for dates: YYYYMMDDHH\ndefault for -p is WPSDA\ndefault for -i is GFS\n"

	phaseF := flag.String("p", "WPSDA", "")
	stepF := flag.String("s", "", "")
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
	if len(args) < 1 {
		log.Fatal(usage)
	}

	var err error
	var dates []runner.TimePeriod
	if len(args) == 1 {
		dates, err = runner.ReadTimes("dates.txt")
		if err != nil {
			log.Fatal(err.Error() + "\n")
		}

	} else {
		startDate, err := time.Parse("2006010215", args[1])
		if err != nil {
			log.Fatal(usage + err.Error() + "\n")
		}
		endDate, err := time.Parse("2006010215", args[2])
		if err != nil {
			log.Fatal(usage + err.Error() + "\n")
		}
		for dt := startDate; dt.Before(endDate) || dt.Equal(endDate); dt = dt.Add(24 * time.Hour) {
			dates = append(dates, runner.TimePeriod{
				Start:    dt,
				Duration: 48,
			})
		}
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

	if *stepF == "" {
		err = runner.Run(dates[0].Start, dates[0].Start.Add(24*time.Hour), wd, phase, input, os.Stdout, os.Stderr)
		if err != nil {
			log.Fatal(err.Error())
		}
	} else {
		parts := strings.Split(*stepF, "-")
		cycleS := parts[0]
		cycle, err := strconv.ParseInt(cycleS, 10, 64)
		if err != nil {
			panic(err)
		}
		var stepType runner.StepType
		switch parts[1] {
		case "BuildDA":
			stepType = runner.BuildDA
		case "BuildWRF":
			stepType = runner.BuildWRF
		case "RunDA":
			stepType = runner.RunDA
		case "RunWRF":
			stepType = runner.RunWRF
		default:
			log.Fatalf("Unknown step type %s", parts[1])
		}
		runner.RunSingleStep(dates[0].Start, input, int(cycle), stepType, os.Stdout, os.Stderr)
	}
}
