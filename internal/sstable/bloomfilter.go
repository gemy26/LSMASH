package sstable

import (
	"hash/maphash"
	"math"
)

type BloomFilter struct {
	m            uint64 //expected size
	k            uint64
	bitset       []byte
	Count        int //number of elements added
	seed1, seed2 maphash.Seed
}

func NewBloomFilter(m uint64, k uint64) *BloomFilter {
	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, m/8), //each element is byte = 8bit and we can use each bit of those bits
		seed1:  maphash.MakeSeed(),
		seed2:  maphash.MakeSeed(),
	}
}

func calculateParams(n uint64, p float64) (m uint64, k uint64) {
	// The formulae we derived are:
	// (m/n) = -ln(eps)/(ln(2)*ln(2))
	// k = (m/n)ln(2)
	ln2 := math.Log(2)
	mdivn := -math.Log(p) / (ln2 * ln2)
	m = uint64(math.Ceil(float64(n) * mdivn))
	k = uint64(math.Ceil(mdivn * ln2))
	return
}
func (bf *BloomFilter) Add(item []byte) {
	h1 := maphash.Bytes(bf.seed1, item)
	h2 := maphash.Bytes(bf.seed2, item)

	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		byteIndex := pos / 8
		bitIndex := pos % 8
		bf.bitset[byteIndex] |= 1 << bitIndex
	}
}

func (bf *BloomFilter) Contains(item []byte) bool {
	h1 := maphash.Bytes(bf.seed1, item)
	h2 := maphash.Bytes(bf.seed2, item)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		if !bf.isSet(pos) {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) isSet(pos uint64) bool {
	byteIndex := pos / 8
	bitIndex := pos % 8
	return (bf.bitset[byteIndex] & (1 << bitIndex)) != 0
}
