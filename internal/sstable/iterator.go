package sstable

import (
	"io"
	memTable "lsmash/internal/memtable"
)

type Iterator struct {
	currEntry *memTable.Entry
	sstable   *SSTable
	readCount uint32
	index     uint32
}

func NewIterator(sstable *SSTable) *Iterator {
	newIterator := &Iterator{
		currEntry: nil,
		sstable:   sstable,
		readCount: 0,
	}
	newIterator.sstable.file.Seek(headerSize, 0)
	return newIterator
}
func (it *Iterator) Next() error {
	if it.readCount >= it.sstable.header.EntryCount {
		return io.EOF
	}
	entry, err := it.sstable.readNextEntry()
	if err != nil {
		return err
	}
	it.currEntry = &entry
	it.readCount++
	return nil
}
func (it *Iterator) Value() *memTable.Entry {
	if it.currEntry != nil {
		return it.currEntry
	}
	return nil
}
func (it *Iterator) Key() (int64, bool) {
	if it.currEntry != nil {
		return it.currEntry.Key, true
	}
	return -1, false
}

func (it *Iterator) Valid() bool {
	return it.currEntry != nil
}
