package histutils

import (
	"encoding/csv"
	"fmt"
	"os"
)

type Histogram struct {
	BucketKeys []float64
	Counts     map[float64]int
	BucketSize int
}

func NewHistogram(bucketType string, bucketSize float64, maxVal float64) *Histogram {
	bucketUpper := bucketSize

	bucketKeys := make([]float64, 0)
	counts := make(map[float64]int)

	// Pre-creating the buckets
	if bucketType == "equal" {
		for bucketUpper < maxVal {
			bucketKeys = append(bucketKeys, bucketUpper)
			counts[bucketUpper] = 0
			bucketUpper += bucketSize
		}
	} else if bucketType == "exponential" {
		for bucketUpper < maxVal {
			bucketKeys = append(bucketKeys, bucketUpper)
			counts[bucketUpper] = 0
			bucketUpper *= 2
		}
	}

	// Adding a final bucket that's twice the size of the previous one
	bucketKeys = append(bucketKeys, bucketUpper)
	counts[bucketUpper] = 0

	return &Histogram{
		BucketKeys: bucketKeys,
		Counts:     counts,
		BucketSize: len(bucketKeys),
	}
}

func (h *Histogram) RecordValue(value float64) {
	var previousBucket float64
	for _, bucket := range h.BucketKeys {
		if value > previousBucket && value <= bucket {
			h.Counts[bucket]++
			return
		}
		previousBucket = bucket
	}

	// If value exceeds upper limit of the last bucket
	if value > h.BucketKeys[h.BucketSize-1] {
		h.Counts[h.BucketKeys[h.BucketSize-1]]++
	}
}

func (h *Histogram) ClearCounts() {
	for _, bucket := range h.BucketKeys {
		h.Counts[bucket] = 0
	}
}

func (h *Histogram) GetBucketKeys() []float64 {
	return h.BucketKeys
}

func (h *Histogram) GetCount(bucket float64) (int, bool) {
	count, ok := h.Counts[bucket]
	return count, ok
}

func (h *Histogram) Add(other *Histogram) {
	for _, key := range other.BucketKeys {
		h.Counts[key] += other.Counts[key]
	}
}

func (h *Histogram) WriteToCSV(fileName string) {
	// Write results to csv
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"bucket_upper", "count"})

	// Write results in ascending order of bucket bounds
	for _, bucket := range h.BucketKeys {
		writer.Write([]string{fmt.Sprintf("%.0f", bucket), fmt.Sprintf("%d", h.Counts[bucket])})
	}
}
