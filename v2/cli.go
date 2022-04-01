package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"time"

	"github.com/meteocima/wrfda-runner/v2/conf"
	"github.com/meteocima/wrfda-runner/v2/runner"
	"github.com/parro-it/fileargs"

	"github.com/meteocima/virtual-server/vpath"
)

// Version of the command
var Version string = "development"

const usage = `
Usage: wrfda-runner [-v ][-p WPS|DA|WPSDA] [-i GFS|IFS] [-outargs <argsfile>] <workdir> [startdate enddate]

-v print version to stdout
-p specifiy the phase to execute. Default is WPSDA
-i specify which kind of dataset to use as guide. Default for -i is GFS. If you omit the option and 
you are using an arguments.txt file, the command will use GFS if the configuration file name specified 
there ends in .gfs.cfg, and IFS if it ends in .ifs.cfg
-outargs if specified, the command write an inputs/arguments.txt file suitable to be used as an input 
arguments.txt file. You cannot use this option if you omit startdate and enddate arguments.

To choose which dates to elaborate you can use startdate and enddate arguments if you need a single date.
Otherwise, you omit this two arguments, and an inputs/arguments.txt will be read that contains al lthe dates 
to run. Format for dates is YYYYMMDDHH.

workdir must be set to the path of a directory containing a prepared environment.

-v show version of the executable
`

func failed(err error) {
	log.Fatalf("%s\n\n%s\n", err, usage)
}

func syntaxInvalid() {
	failed(errors.New("Invalid arguments provided"))
}

func main() {

	showver := flag.Bool("v", false, "")
	phaseF := flag.String("p", "WPSDA", "")
	inputF := flag.String("i", "GFS", "")
	outArgsFileF := flag.String("outargs", "", "")

	flag.Parse()

	if showver != nil && *showver {
		fmt.Printf("wrfda-run ver. %s\n", Version)
		return
	}

	var phase conf.RunPhase
	var input conf.InputDataset

	if err := phase.FromString(*phaseF); err != nil {
		failed(err)
	}

	input.FromString(*inputF)

	args := flag.Args()
	if len(args) < 1 {
		syntaxInvalid()
	}

	var err error
	var dates *fileargs.FileArguments
	var cfgFile vpath.VirtualPath
	var wd vpath.VirtualPath

	absWd, err := filepath.Abs(args[0])
	if err != nil {
		log.Fatal(err.Error())
	}

	wd = vpath.Local(absWd)

	if len(args) == 1 {
		dates = readInputArgs(&input, wd)
	} else {
		dates = datesFromArgs(args, wd)
	}

	cfgFile = vpath.Local(dates.CfgPath)

	if outArgsFileF != nil && *outArgsFileF != "" {
		writeOutargs(outArgsFileF, input, dates)
	}

	err = runner.Init(cfgFile, wd)
	if err != nil {
		log.Fatal(err.Error())
	}

	err = runner.Run(dates.Periods,
		wd, phase, input, os.Stdout, os.Stderr,
	)

	if err != nil {
		log.Fatal(err.Error())
	}

}

func datesFromArgs(args []string, wd vpath.VirtualPath) *fileargs.FileArguments {
	dates := &fileargs.FileArguments{
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

	dates.CfgPath = wd.Join("wrfda-runner.cfg").Path
	return dates
}

func readInputArgs(input *conf.InputDataset, wd vpath.VirtualPath) *fileargs.FileArguments {
	dates, err := runner.ReadTimes("inputs/arguments.txt")
	if err != nil {
		log.Fatal(err.Error() + "\n")
	}

	if *input == conf.Unspecified {
		if strings.HasSuffix(dates.CfgPath, ".gfs.cfg") {
			*input = conf.GFS
		}

		if strings.HasSuffix(dates.CfgPath, ".ifs.cfg") {
			*input = conf.IFS
		}

		failed(fmt.Errorf("Unknown input dataset `%s` must end in .gfs.cfg or .ifs.cfg", dates.CfgPath))
	}
	dates.CfgPath = wd.Join(dates.CfgPath).Path
	return dates
}

func writeOutargs(outArgsFileF *string, input conf.InputDataset, dates *fileargs.FileArguments) {
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
		failed(err)
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
