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

func parseInt(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, errNotInteger
	}
	return n, nil
}

func parseTTL(s string, unit time.Duration, cmd string) (time.Duration, error) {
	n, err := parseInt(s)
	if err != nil {
		return 0, err
	}
	if n > math.MaxInt64/int64(unit) || n < math.MinInt64/int64(unit) {
		return 0, errors.New(invalidExpire(cmd))
	}
	return time.Duration(n) * unit, nil
}

func parseTimeout(s string) (time.Duration, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, errors.New("ERR timeout is not a float or out of range")
	}
	if f < 0 {
		return 0, errors.New("ERR timeout is negative")
	}
	if f > float64(math.MaxInt64/int64(time.Second)) {
		return 0, errors.New("ERR timeout is out of range")
	}
	return time.Duration(f * float64(time.Second)), nil
}
