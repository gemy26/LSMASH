package sstable

import (
	"hash/fnv"
	memTable "lsmash/internal/memtable"
	"math"
)

type BloomFilter struct {
	m      uint64 // number of bits
	k      uint64 // number of hash functions
	bitset []byte
	Count  int64 // number of elements added
}

func NewBloomFilter(m uint64, k uint64) *BloomFilter {
	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, (m+7)/8), // each element is byte = 8bit
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

func (bf *BloomFilter) hash1(item []byte) uint64 {
	h := fnv.New64()
	h.Write(item)
	return h.Sum64()
}

func (bf *BloomFilter) hash2(item []byte) uint64 {
	h := fnv.New64a()
	h.Write(item)
	return h.Sum64()
}

func (bf *BloomFilter) Add(item []byte) {
	h1 := bf.hash1(item)
	h2 := bf.hash2(item)

	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		byteIndex := pos / 8
		bitIndex := pos % 8
		bf.bitset[byteIndex] |= 1 << bitIndex
	}
	bf.Count++
}

func (bf *BloomFilter) Contains(item []byte) bool {
	h1 := bf.hash1(item)
	h2 := bf.hash2(item)
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

func BuildBloomFilter(item []memTable.Entry) *BloomFilter {
	m, k := calculateParams(uint64(len(item)), .1)
	filter := NewBloomFilter(m, k)
	for i := 0; i < len(item); i++ {
		filter.Add(intToByte(uint64(item[i].Key)))
	}
	return filter
}
