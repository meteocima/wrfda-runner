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

// Version of the command
var Version string = "development"

func main() {
	usage := `
Usage: wrfda-run [-p WPS|DA|WPSDA] [-i GFS|IFS] [-outargs <argsfile>] <workdir> [startdate enddate]
format for dates: YYYYMMDDHH
Note: if you omit startdate and enddate, they are read from an arguments.txt
files that should be put in a subdirectory of workdir named "inputs"
default for -p is WPSDA
default for -i is GFS (you can omit this argument if you're using an arguments.txt file.)

Show version: wrfda-run -v
`

	showver := flag.Bool("v", false, "print version to stdout")
	phaseF := flag.String("p", "WPSDA", "")
	stepF := flag.String("s", "", "")
	inputF := flag.String("i", "GFS", "")
	outArgsFileF := flag.String("outargs", "", "")

	flag.Parse()

	if showver != nil && *showver {
		fmt.Printf("wrfda-run ver. %s\n", Version)
		return
	}

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
	} else if outArgsFileF == nil || *outArgsFileF == "" {
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
		dates, err = runner.ReadTimes("inputs/arguments.txt")
		if err != nil {
			log.Fatal(err.Error() + "\n")
		}

		if inputF == nil {
			if strings.HasSuffix(dates.CfgPath, ".gfs.cfg") {
				input = conf.GFS
			}

			if strings.HasSuffix(dates.CfgPath, ".ifs.cfg") {
				input = conf.IFS
			}

			log.Fatalf("%s\nUnknown input dataset `%s` must end in .gfs.cfg or .ifs.cfg", usage, dates.CfgPath)
		}
		dates.CfgPath = wd.Join(dates.CfgPath).Path
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

		duration := endDate.Sub(startDate)
		dates.Periods = append(dates.Periods, &fileargs.Period{
			Start:    startDate,
			Duration: duration,
		})
		/*
			for dt := startDate; dt.Before(endDate) || dt.Equal(endDate); dt = dt.Add(24 * time.Hour) {
				dates.Periods = append(dates.Periods, &fileargs.Period{
					Start:    dt,
					Duration: 48 * time.Hour,
				})
			}
		*/

		dates.CfgPath = wd.Join("wrfda-runner.cfg").Path
	}
	cfgFile = vpath.Local(dates.CfgPath)

	if outArgsFileF != nil && *outArgsFileF != "" {
		outargs := *outArgsFileF

		_, err := os.Stat(outargs)
		fileargsExists := err == nil

		var buf lineBuf
		if !fileargsExists {
			if input == conf.GFS {
				buf.AddLine("italy-config.gfs.cfg")
			} else {
				buf.AddLine("france-config.ifs.cfg")
			}
		}

		for _, p := range dates.Periods {
			buf.AddLine(p.String())
		}

		if fileargsExists {
			err = buf.AppendTo(outargs)
		} else {
			err = buf.WriteTo(outargs)
		}

		if err != nil {
			log.Fatal(err.Error())
		}
	}

	err = runner.Init(cfgFile, wd)
	if err != nil {
		log.Fatal(err.Error())
	}

	if *stepF == "" {
		err = runner.Run(
			dates.Periods, wd, phase, input, os.Stdout, os.Stderr,
		)

		if err != nil {
			log.Fatal(err.Error())
		}
		return
	}

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

func (lines *lineBuf) AppendTo(filepath string) error {
	f, err := os.OpenFile(filepath, os.O_APPEND|os.O_WRONLY, fs.FileMode(0644))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(lines.buf.Bytes())
	lines.buf.Truncate(0)

	return err
}
