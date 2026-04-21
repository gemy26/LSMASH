package sstable

import (
	"testing"
)

func TestBloomFilter(t *testing.T) {
	// Estimations from the blog post with 100,000 elements and an error rate
	// of 0.01
	m, k := calculateParams(100000, 0.01)
	bf := NewBloomFilter(m, k)
	items := [][]byte{
		make([]byte, 10),
		make([]byte, 13),
		make([]byte, 123),
		make([]byte, 55),
		make([]byte, 70),
		make([]byte, 145),
		make([]byte, 32),
		make([]byte, 435),
		make([]byte, 9),
		make([]byte, 22),
		make([]byte, 69),
		make([]byte, 45),
	}
	for _, item := range items {
		bf.Add(item)
	}
	for _, item := range items {
		if !bf.Contains(item) {
			t.Fatalf("expected to be found")
		}
	}

	if bf.Contains(make([]byte, 77)) {
		t.Fatalf("expected to not be found")
	}
	if bf.Contains(make([]byte, 37)) {
		t.Fatalf("expected to not be found")
	}
}
