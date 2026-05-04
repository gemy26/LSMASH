package sstable

import (
	"lsmash/config"
	memTable "lsmash/internal/memtable"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestFlushAndGet(t *testing.T) {
	mt := memTable.NewMemTable()
	mt.SkipList.Insert(1, 100)
	mt.SkipList.Insert(2, 200)
	mt.SkipList.Insert(3, 300)
	mt.SkipList.Insert(4, 400)
	mt.SkipList.Insert(5, 500)
	mt.SkipList.Delete(2) // tombstone

	sst, err := FlushToSSTable(mt)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cfg := config.DefaultConfig()
		os.Remove(filepath.Join(cfg.WorkingDir, sst.FileName))
	}()

	if sst.header.MinKey != 1 {
		t.Errorf("MinKey: want 1, got %d", sst.header.MinKey)
	}
	if sst.header.MaxKey != 5 {
		t.Errorf("MaxKey: want 5, got %d", sst.header.MaxKey)
	}
	if sst.header.EntryCount != 5 {
		t.Errorf("EntryCount: want 5, got %d", sst.header.EntryCount)
	}

	if v, ok := sst.Get(1); !ok || v != 100 {
		t.Errorf("key 1: want 100, got %d ok=%v", v, ok)
	}
	if v, ok := sst.Get(3); !ok || v != 300 {
		t.Errorf("key 3: want 300, got %d ok=%v", v, ok)
	}

	if v, ok := sst.Get(1); !ok || v != 100 {
		t.Errorf("min key 1: want 100, got %d ok=%v", v, ok)
	}
	if v, ok := sst.Get(5); !ok || v != 500 {
		t.Errorf("max key 5: want 500, got %d ok=%v", v, ok)
	}

	if _, ok := sst.Get(2); ok {
		t.Error("key 2 is tombstoned, should not be found")
	}

	if _, ok := sst.Get(0); ok {
		t.Error("key 0 is below MinKey, should not be found")
	}
	if _, ok := sst.Get(99); ok {
		t.Error("key 99 is above MaxKey, should not be found")
	}

	for i := 0; i < 3; i++ {
		if v, ok := sst.Get(4); !ok || v != 400 {
			t.Errorf("repeated Get key 4 (iter %d): want 400, got %d ok=%v", i, v, ok)
		}
	}
}

func TestFlushEmptyMemtable(t *testing.T) {
	mt := memTable.NewMemTable()
	_, err := FlushToSSTable(mt)
	if err == nil {
		t.Error("expected error flushing empty memtable, got nil")
	}
}

func BenchmarkGet(b *testing.B) {
	mt := memTable.NewMemTable()

	const N = 100000
	for i := int64(0); i < N; i++ {
		mt.SkipList.Insert(i, i)
	}

	sst, err := FlushToSSTable(mt)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		cfg := config.DefaultConfig()
		os.Remove(filepath.Join(cfg.WorkingDir, sst.FileName))
	}()

	keys := make([]int64, 10000)
	for i := 0; i < len(keys); i++ {
		keys[i] = rand.Int63n(N)
	}

	var result int64
	var ok bool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[i%len(keys)]
		result, ok = sst.Get(k)
	}

	_ = result
	_ = ok
}
