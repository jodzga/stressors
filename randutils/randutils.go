// Package randutils provides utility functions for common rand operations.
package randutils

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// SetRandSeed simply wraps setting a unique random seed. It should be called before calling any other randutils functions.
func SetRandSeed() {
	rand.Seed(time.Now().UnixNano())
}

// GetRandInRange returns a nonnegative integer in the range up to but not including limit parameter.
func GetRandInRange(limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("expected a positive integer but got %d", limit)
	}

	return rand.Intn(limit), nil
}

// GetRandElems returns a string slice of count number of elements randomly selected from the source string slice.
func GetRandElems(src []string, count int) ([]string, error) {
	max := len(src)
	if count > max {
		return nil, fmt.Errorf("requested %d random elems from the slice but there are only %d elems available", count, max)
	}

	dest := make([]string, count)

	perm := rand.Perm(len(src))
	for i := 0; i < count; i++ {
		dest[i] = src[perm[i]]
	}

	return dest, nil
}

// https://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
// If lambda is N requests per nanosecond, this would return the number of nanoseconds before the next request should be made.
// time.Duration is int64 so need nsec to not lose decimal places
func GetNextTimePoisson(lambdaInPerNsec float64) (time.Duration, error) {
	if lambdaInPerNsec <= 0 {
		return 0, fmt.Errorf("expected a positive integer but got %f", lambdaInPerNsec)
	}

	return time.Duration(-math.Log(1.0-rand.Float64()) / lambdaInPerNsec), nil
}
