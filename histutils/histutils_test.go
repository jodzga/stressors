package histutils

import (
	"testing"
)

func TestNewHistogram(t *testing.T) {
	h := NewHistogram("exponential", 10.0, 1e6)

	if h.BucketSize != len(h.BucketKeys) {
		t.Errorf("Incorrect bucket size: got %v want %v", h.BucketSize, len(h.BucketKeys))
	}

	h = NewHistogram("equal", 1.0, 100.0)

	if h.BucketSize != len(h.BucketKeys) {
		t.Errorf("Incorrect bucket size: got %v want %v", h.BucketSize, len(h.BucketKeys))
	}
}

func TestPercentHistogram(t *testing.T) {
	h := NewHistogram("equal", 1.0, 100.0)

	h.RecordValue(10)
	h.RecordValue(20)
	h.RecordValue(100)
	h.RecordValue(1000)

	// verify that the counts are correct
	if h.Counts[10] != 1 || h.Counts[20] != 1 || h.Counts[100] != 2 {
		t.Errorf("Incorrect bucket counts: got %v, %v, %v want %v, %v, %v", h.Counts[10], h.Counts[20], h.Counts[100], 1, 1, 2)
	}

}

func TestRecordValue(t *testing.T) {
	h := NewHistogram("exponential", 10.0, 1e6)

	h.RecordValue(15)
	if h.Counts[20] != 1 {
		t.Errorf("Incorrect bucket count for value 15: got %v want %v", h.Counts[20], 1)
	}

	h.RecordValue(10000000) // 10 seconds, should go to the last bucket
	if h.Counts[h.BucketKeys[h.BucketSize-1]] != 1 {
		t.Errorf("Incorrect bucket count for value 10000000: got %v want %v", h.Counts[h.BucketKeys[h.BucketSize-1]], 1)
	}
}

func TestAdd(t *testing.T) {
	h1 := NewHistogram("exponential", 10.0, 1e6)
	h2 := NewHistogram("exponential", 10.0, 1e6)

	h1.RecordValue(15)
	h2.RecordValue(25)

	h1.Add(h2)

	if h1.Counts[20] != 1 || h1.Counts[40] != 1 {
		t.Errorf("Incorrect counts after addition: got %v and %v, want 1 and 1", h1.Counts[20], h1.Counts[40])
	}
}

func TestClearCounts(t *testing.T) {
	h := NewHistogram("equal", 1.0, 100.0)

	h.RecordValue(15)
	h.ClearCounts()

	count, _ := h.GetCount(15)
	if count != 0 {
		t.Errorf("Counts not cleared: got %v expected 0", count)
	}
}
