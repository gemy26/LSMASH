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
		intToByte(10),
		intToByte(13),
		intToByte(123),
		intToByte(55),
		intToByte(70),
		intToByte(145),
		intToByte(32),
		intToByte(435),
		intToByte(9),
		intToByte(22),
		intToByte(69),
		intToByte(45),
	}
	for _, item := range items {
		bf.Add(item)
	}
	for _, item := range items {
		if !bf.Contains(item) {
			t.Fatalf("expected to be found")
		}
	}

	if bf.Contains(intToByte(77)) {
		t.Fatalf("expected to not be found")
	}
	if bf.Contains(intToByte(37)) {
		t.Fatalf("expected to not be found")
	}
}
