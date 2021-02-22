package runner

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type TimePeriod struct {
	Start    time.Time
	Duration time.Duration
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
		date, err := time.Parse("2006010215", parts[0])
		if err != nil {
			return nil, err
		}
		var duration time.Duration
		if len(parts) > 1 {
			dur, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, err
			}
			duration = time.Hour * time.Duration(dur)
		} else {
			duration = time.Hour * 24
		}
		results[idx] = TimePeriod{
			Start:    date,
			Duration: duration,
		}
	}

	return results, nil
}
