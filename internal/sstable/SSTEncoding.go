package sstable

import (
	"encoding/binary"
	"lsmash/internal/memtable"
)

const headerSize = 28 // 2×int64 + 3×uint32 (MinKey, MaxKey, EntryCount, BloomSize, BloomOffset)
const entrySize = 17  // int64 key + int64 val + 1 byte tombstone

func (s *SSTable) writeHeader() error {
	if err := binary.Write(s.file, binary.LittleEndian, s.header.MinKey); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.LittleEndian, s.header.MaxKey); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.LittleEndian, s.header.EntryCount); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.LittleEndian, s.header.BloomSize); err != nil {
		return err
	}
	if err := binary.Write(s.file, binary.LittleEndian, s.header.BloomOffset); err != nil {
		return err
	}
	return nil
}
func (s *SSTable) writeEntry(entry []memTable.Entry) error {
	s.file.Seek(headerSize, 0)
	for _, entryItem := range entry {
		if err := binary.Write(s.file, binary.LittleEndian, entryItem.Key); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.LittleEndian, entryItem.Val); err != nil {
			return err
		}
		if err := binary.Write(s.file, binary.LittleEndian, entryItem.Tombstoned); err != nil {
			return err
		}
	}
	return nil
}
func (s *SSTable) writeBloom() error {
	s.file.Seek(int64(s.header.BloomOffset), 0)
	if err := binary.Write(s.file, binary.LittleEndian, s.bloom.bitset); err != nil {
		return err
	}
	return nil
}
func (s *SSTable) readHeader() error {
	if err := binary.Read(s.file, binary.LittleEndian, &s.header); err != nil {
		return err
	}
	return nil
}
func (s *SSTable) readBloom() error {
	s.file.Seek(int64(s.header.BloomOffset), 0)
	bitset := make([]byte, s.header.BloomSize)
	if err := binary.Read(s.file, binary.LittleEndian, &bitset); err != nil {
		return err
	}
	_, k := calculateParams(uint64(s.header.EntryCount), 0.1)
	s.bloom = &BloomFilter{
		m:      uint64(s.header.BloomSize) * 8,
		k:      k,
		bitset: bitset,
	}
	return nil
}
func (s *SSTable) readEntry() []memTable.Entry {
	data := make([]memTable.Entry, s.header.EntryCount)
	s.file.Seek(headerSize, 0)
	if err := binary.Read(s.file, binary.LittleEndian, &data); err != nil {
		return nil
	}
	return data
}
