package sstable

import (
	"testing"

	memTable "lsmash/internal/memtable"
)

func TestFlushAndGet(t *testing.T) {
	mt := memTable.NewMemTable()
	mt.SkipList.Insert(1, 100)
	mt.SkipList.Insert(2, 200)
	mt.SkipList.Insert(3, 300)
	mt.SkipList.Delete(2) // tombstone

	sst, err := FlushToSSTable(mt)
	if err != nil {
		t.Fatal(err)
	}

	if v, ok := sst.Get(1); !ok || v != 100 {
		t.Errorf("key 1: want 100, got %d %v", v, ok)
	}
	if _, ok := sst.Get(2); ok {
		t.Error("key 2 is tombstoned, should not be found")
	}
	if v, ok := sst.Get(3); !ok || v != 300 {
		t.Errorf("key 3: want 300, got %d %v", v, ok)
	}
	if _, ok := sst.Get(99); ok {
		t.Error("key 99 never inserted, should not be found")
	}
}
