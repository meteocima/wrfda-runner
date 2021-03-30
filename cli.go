package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"time"

	"github.com/meteocima/wrfda-runner/conf"
	"github.com/meteocima/wrfda-runner/runner"
	"github.com/parro-it/fileargs"

	"github.com/meteocima/virtual-server/vpath"
)

func main() {
	usage := `
Usage: wrfda-run [-p WPS|DA|WPSDA] [-i GFS|IFS] [-outargs <argsfile>] <workdir> [startdate enddate]
format for dates: YYYYMMDDHH
Note: if you omit startdate and enddate, they are read from an arguments.txt
files that should be put in the workdir.
default for -p is WPSDA
default for -i is GFS
`

	phaseF := flag.String("p", "WPSDA", "")
	stepF := flag.String("s", "", "")
	inputF := flag.String("i", "GFS", "")
	outArgsFileF := flag.String("outargs", "", "")

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
	var dates *fileargs.FileArguments
	var cfgFile vpath.VirtualPath

	absWd, err := filepath.Abs(args[0])
	if err != nil {
		log.Fatal(err.Error())
	}

	wd := vpath.Local(absWd)

	if len(args) == 1 {
		dates, err = runner.ReadTimes("arguments.txt")
		if err != nil {
			log.Fatal(err.Error() + "\n")
		}
		cfgFile = vpath.Local(dates.CfgPath)
	} else {
		dates = &fileargs.FileArguments{
			Periods: []*fileargs.Period{},
			CfgPath: "",
		}
		startDate, err := time.Parse("2006010215", args[1])
		if err != nil {
			log.Fatal(usage + err.Error() + "\n")
		}
		endDate, err := time.Parse("2006010215", args[2])
		if err != nil {
			log.Fatal(usage + err.Error() + "\n")
		}
		for dt := startDate; dt.Before(endDate) || dt.Equal(endDate); dt = dt.Add(24 * time.Hour) {
			dates.Periods = append(dates.Periods, &fileargs.Period{
				Start:    dt,
				Duration: 48 * time.Hour,
			})
		}

		cfgFile = wd.Join("wrfda-runner.cfg")
	}

	if outArgsFileF != nil {
		outargs := *outArgsFileF
		var buf lineBuf
		if input == conf.GFS {
			buf.AddLine("wrfda-runner.it.cfg")
		} else {
			buf.AddLine("wrfda-runner.fr.cfg")
		}

		for _, p := range dates.Periods {
			buf.AddLine(p.String())
		}

		err := buf.WriteTo(outargs)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	err = runner.Init(cfgFile, wd)
	if err != nil {
		log.Fatal(err.Error())
	}

	if *stepF == "" {
		err = runner.Run(dates.Periods[0].Start, dates.Periods[0].Start.Add(24*time.Hour), wd, phase, input, os.Stdout, os.Stderr)
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
		runner.RunSingleStep(dates.Periods[0].Start, input, int(cycle), stepType, os.Stdout, os.Stderr)
	}
}

type lineBuf struct {
	buf bytes.Buffer
}

func (lines *lineBuf) AddLine(lineFormat string, arguments ...interface{}) {
	line := fmt.Sprintf(lineFormat, arguments...)
	lines.buf.WriteString(line)
	lines.buf.WriteRune('\n')
}

func (lines *lineBuf) WriteTo(filepath string) error {
	f, err := os.OpenFile(filepath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fs.FileMode(0644))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(lines.buf.Bytes())
	lines.buf.Truncate(0)

	return err
}
