package sstable

import (
	"encoding/binary"
	"fmt"
	"lsmash/config"
	memTable "lsmash/internal/memtable"
	"os"
	"path/filepath"
	"strings"
)

type SSTableHeader struct {
	MinKey      int64
	MaxKey      int64
	EntryCount  uint32
	BloomSize   uint32
	BloomOffset uint32
}

type SSTable struct {
	header   SSTableHeader
	filePath string
	fileName string
	bloom    *BloomFilter
	file     *os.File
}

func NewSSTable(header SSTableHeader, bloomFilter *BloomFilter, level int8) (*SSTable, error) {
	fileName := newSSTableFileName(level)
	cfg := config.DefaultConfig()
	fullPath := filepath.Join(cfg.WorkingDir, fileName)

	file, err := os.Create(fullPath)

	if err != nil {
		return nil, err
	}
	return &SSTable{
		header:   header,
		filePath: cfg.WorkingDir,
		fileName: fileName,
		bloom:    bloomFilter,
		file:     file,
	}, nil
}

func newSSTableFileName(level int8) string {
	cfg := config.DefaultConfig()
	dir := cfg.WorkingDir
	prefix := fmt.Sprintf("l%d_", level)
	count := 0
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ".lsm") {
			count++
		}
	}
	return fmt.Sprintf("l%d_%d.lsm", level, count)
}

func buildHeader(entries []memTable.Entry, bf *BloomFilter) SSTableHeader {
	minKey := entries[0].Key
	maxKey := entries[len(entries)-1].Key
	bloomBytes := uint32(len(bf.bitset))
	dataSize := uint32(len(entries)) * uint32(entrySize)
	bloomOffset := uint32(headerSize) + dataSize

	header := SSTableHeader{
		EntryCount:  uint32(len(entries)),
		MinKey:      int64(minKey),
		MaxKey:      int64(maxKey),
		BloomSize:   bloomBytes,
		BloomOffset: bloomOffset,
	}
	return header
}

// TODO: Seek-based Binary Search
func (s *SSTable) Get(key int64) (int64, bool) {
	if key < s.header.MinKey || key > s.header.MaxKey {
		return 0, false
	}
	if !s.bloom.Contains(intToByte(uint64(key))) {
		return 0, false
	}

	data, err := s.readEntry()
	if err != nil {
		return 0, false
	}
	l, r := 0, len(data)-1
	idx := -1
	for r >= l {
		mid := (l + r) / 2
		if data[mid].Key == key {
			idx = mid
			break
		} else if data[mid].Key < key {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	if idx == -1 {
		return 0, false
	}

	if data[idx].Tombstoned {
		return 0, false
	}

	return data[idx].Val, true
}

func FlushToSSTable(memtable *memTable.MemTable) (*SSTable, error) {
	entries := memtable.SkipList.ScanAll()
	if len(entries) == 0 {
		return nil, fmt.Errorf("cannot flush an empty memtable")
	}
	minKey := entries[0].Key
	maxKey := entries[len(entries)-1].Key
	m, k := calculateParams(uint64(len(entries)), .1)
	bf := NewBloomFilter(m, k)

	for _, e := range entries {
		bf.Add(intToByte(uint64(e.Key)))
	}

	bloomBytes := uint32(len(bf.bitset))
	dataSize := uint32(len(entries)) * uint32(entrySize)
	bloomOffset := uint32(headerSize) + dataSize
	//totalSize := bloomOffset + bloomBytes

	//limit := config.DefaultConfig().SstableFileSizeLimit
	//if int64(totalSize) > limit {
	//	return nil, fmt.Errorf("sstable size %d exceeds file size limit %d", totalSize, limit)
	//}

	header := SSTableHeader{
		EntryCount:  uint32(len(entries)),
		MinKey:      int64(minKey),
		MaxKey:      int64(maxKey),
		BloomSize:   bloomBytes,
		BloomOffset: bloomOffset,
	}
	sst, err := NewSSTable(header, bf, 0)
	if err != nil {
		return nil, err
	}

	if err := sst.writeHeader(); err != nil {
		return nil, fmt.Errorf("writeHeader: %w", err)
	}
	if err := sst.writeEntry(entries); err != nil {
		return nil, fmt.Errorf("writeEntry: %w", err)
	}
	if err := sst.writeBloom(); err != nil {
		return nil, fmt.Errorf("writeBloom: %w", err)
	}

	return sst, nil
}

func intToByte(i uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, i)
	return b
}

func Compaction(it *MergeIterator, level int8) ([]*SSTable, error) {
	var entries []memTable.Entry
	var tables []*SSTable
	for it.Next() {
		if it.currEntry.Tombstoned {
			continue
		}
		entries = append(entries, *it.Value())
		if len(entries) == 5 { //TODO: change static number with config one
			sstable, _ := sealSSTable(entries, level)
			tables = append(tables, sstable)
			entries = nil
		}
	}
	if len(entries) != 0 {
		sstable, _ := sealSSTable(entries, level)
		tables = append(tables, sstable)
	}
	return tables, nil
}
func sealSSTable(entries []memTable.Entry, level int8) (*SSTable, error) {
	bf := BuildBloomFilter(entries)
	header := buildHeader(entries, bf)
	sstable, err := NewSSTable(header, bf, level)
	if err != nil {
		return nil, err
	}
	if err := sstable.writeHeader(); err != nil {
		return nil, err
	}
	if err := sstable.writeEntry(entries); err != nil {
		return nil, err
	}
	if err := sstable.writeBloom(); err != nil {
		return nil, err
	}
	return sstable, nil
}
