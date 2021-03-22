package runner

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type KnownDomain int

const (
	Italy KnownDomain = iota
	France
)

type TimePeriod struct {
	Start    time.Time
	Duration time.Duration
	Domain   KnownDomain
}

func ReadTimes(file string) ([]TimePeriod, error) {
	content, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	contentS := strings.Split(string(content), "\n")
	results := make([]TimePeriod, len(contentS))
	for idx, line := range contentS {
		parts := strings.Split(line, " ")
		if len(parts) != 3 {
			return nil, fmt.Errorf(`
Expected format for arguments.txt:  
YYYYMMDDHH HOURS DOMAIN
Cannot parse line
%s`, line)
		}
		date, err := time.Parse("2006010215", parts[0])
		if err != nil {
			return nil, err
		}
		var duration time.Duration
		dur, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return nil, err
		}
		duration = time.Hour * time.Duration(dur)
		tp := TimePeriod{
			Start:    date,
			Duration: duration,
		}

		domain := parts[2]
		switch domain {
		case "IT":
			tp.Domain = Italy
		case "FR":
			tp.Domain = France
		default:
			return nil, fmt.Errorf("wrong domain code %s: expecting one of IT, FR", domain)
		}

		results[idx] = tp
	}

	return results, nil
}
