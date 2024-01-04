package randutils

import (
	"math"
	"math/rand"
	"testing"
)

func init() {
	rand.Seed(0)
}

func TestGetRandInRange_GetsRandInRange(t *testing.T) {
	const Iters = 10000
	const Limit = 88

	for i := 0; i < Iters; i++ {
		num, err := GetRandInRange(Limit)
		if err != nil {
			t.Fatalf("failed with error: %s", err)
		}

		if num < 0 || num >= Limit {
			t.Fatalf("expected a nonnegative integer less than %d but got %d", Limit, num)
		}
	}
}

func TestGetRandInRange_ErrorsOnOutOfBounds(t *testing.T) {
	if _, err := GetRandInRange(0); err == nil {
		t.Fatalf("expected an error on passing 0, which is out-of-bounds")
	}

	if _, err := GetRandInRange(-3); err == nil {
		t.Fatalf("expected an error on passing a negative number, which is out-of-bounds")
	}
}

func TestGetRandInRange_HasUniformDistribution(t *testing.T) {
	const Iters = 100000
	const Limit = 20
	const ExpectedCount = Iters / Limit

	countMap := make(map[int]int)
	for i := 0; i < Iters; i++ {
		num, err := GetRandInRange(Limit)
		if err != nil {
			t.Fatalf("failed with error: %s", err)
		}

		countMap[num]++
	}

	for _, count := range countMap {
		if count < 0.97*ExpectedCount || count > 1.03*ExpectedCount {
			t.Fatalf("expected a uniform distribution %d", count)
		}
	}
}

func TestGetRandElems_PermutesElemsFromSrc(t *testing.T) {
	src := []string{"hello", "this", "is", "a", "test", "!"}

	randElems, err := GetRandElems(src, len(src))
	if err != nil {
		t.Fatalf("failed with error: %s", err)
	}

	for i := range randElems {
		if src[i] != randElems[i] {
			return
		}
	}

	// With the set rand seed, the permutation is not the same.
	t.Fatalf("expected a permutation of elements but got the same")
}

func TestGetRandElems_ReturnsElemsFromSrc(t *testing.T) {
	src := []string{"hello", "this", "is", "a", "test", "!"}
	srcSet := make(map[string]bool)
	for _, val := range src {
		srcSet[val] = true
	}

	randElems, err := GetRandElems(src, len(src)-2)
	if err != nil {
		t.Fatalf("failed with error: %s", err)
	}

	for _, randElem := range randElems {
		if _, found := srcSet[randElem]; !found {
			t.Fatalf("got %s which is not in the source slice %+v", randElem, src)
		}
	}
}

func TestGetRandElems_ReturnsSliceOfExpectedSize(t *testing.T) {
	src := []string{"hello", "this", "is", "a", "GetRandElems", "test", "!"}

	for i := 0; i <= len(src); i++ {
		rand, err := GetRandElems(src, i)
		if err != nil {
			t.Fatalf("failed with error: %s", err)
		}

		if i != len(rand) {
			t.Fatalf("expected a slice of size %d but got one of size %d", i, len(rand))
		}
	}
}

func TestGetRandElems_DoesNotRepeatSelection(t *testing.T) {
	uniqueElems := []string{"hello", "this", "is", "a", "test", "with", "only", "unique", "strings", "in", "the", "string", "slice", "!"}

	selected, err := GetRandElems(uniqueElems, len(uniqueElems)-2)
	if err != nil {
		t.Fatalf("failed with error: %s", err)
	}

	foundElems := make(map[string]bool)
	for _, selectedElem := range selected {
		if _, found := foundElems[selectedElem]; found {
			t.Fatalf("expected unique elems but %s was repeated", selectedElem)
		}

		foundElems[selectedElem] = true
	}
}

func TestGetRandElems_ErrorsOnTooManyRequested(t *testing.T) {
	elems := []string{"hello", "this", "is", "a", "test", "!"}

	if _, err := GetRandElems(elems, len(elems)+1); err == nil {
		t.Fatalf("expected an error on requesting more elems than available")
	}
}

func TestGetNextTimePoisson_HasReasonableMean(t *testing.T) {
	// If lambda is 30 requests per second, then over many samples, the mean should be 1/30 ~= 0.033 seconds per request
	const Iters = 100000
	lambdaInPerNsec := 30.0 / math.Pow(10, 9)
	expectedMean := 1 / lambdaInPerNsec

	sum := 0.0
	for i := 0; i < Iters; i++ {
		val, err := GetNextTimePoisson(lambdaInPerNsec)
		if err != nil {
			t.Fatalf("failed with error: %s", err)
		}

		sum += float64(val.Nanoseconds())
	}
	actualMean := sum / Iters

	if actualMean < 0.97*expectedMean || actualMean > 1.03*expectedMean {
		t.Fatalf("expected %f but got %f", expectedMean, actualMean)
	}
}

func TestGetNextTimePoisson_ErrorsOnOutOfBounds(t *testing.T) {
	if _, err := GetNextTimePoisson(0); err == nil {
		t.Fatalf("expected an error on passing 0, which is out-of-bounds")
	}

	if _, err := GetNextTimePoisson(-3.82); err == nil {
		t.Fatalf("expected an error on passing a negative number, which is out-of-bounds")
	}
}
