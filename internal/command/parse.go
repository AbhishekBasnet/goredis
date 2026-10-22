package command

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

var errNotInteger = errors.New("ERR value is not an integer or out of range")

func invalidExpire(cmd string) string {
	return fmt.Sprintf("ERR invalid expire time in '%s' command", cmd)
}

// parseInt accepts only canonical integers ("10", "-3"; not "+1" or "01"),
// matching how Redis parses numeric arguments.
//
// The store has the same five lines for values. Duplicating them keeps the
// store free of command-parsing concerns: a little copying is better than a
// little dependency.
func parseInt(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, errNotInteger
	}
	return n, nil
}

// parseTTL parses a count of unit (seconds or milliseconds) into a Duration.
// The result may be zero or negative; each command decides what that means.
// cmd names the command in the error message.
func parseTTL(s string, unit time.Duration, cmd string) (time.Duration, error) {
	n, err := parseInt(s)
	if err != nil {
		return 0, err
	}
	// time.Duration is int64 nanoseconds, about 292 years. Reject anything
	// that would silently wrap around when multiplied.
	if n > math.MaxInt64/int64(unit) || n < math.MinInt64/int64(unit) {
		return 0, errors.New(invalidExpire(cmd))
	}
	return time.Duration(n) * unit, nil
}
