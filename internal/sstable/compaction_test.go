package sstable

import (
	"os"
	"testing"

	memTable "lsmash/internal/memtable"
)

func makeSSTableForCompaction(t *testing.T, kvs []struct{ k, v int64 }, tombstones []int64) *SSTable {
	t.Helper()
	mt := memTable.NewMemTable()
	for _, kv := range kvs {
		mt.SkipList.Insert(kv.k, kv.v)
	}
	for _, k := range tombstones {
		mt.SkipList.Delete(k)
	}
	sst, err := FlushToSSTable(mt)
	if err != nil {
		t.Fatalf("FlushToSSTable: %v", err)
	}
	path := sst.filePath + "/" + sst.FileName
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	sst.file = f
	sst.readHeader()
	t.Cleanup(func() { f.Close(); os.Remove(path) })
	return sst
}

func runCompaction(t *testing.T, files []*SSTable, level int8) []*SSTable {
	t.Helper()
	var iterators []*Iterator
	for _, sst := range files {
		iterators = append(iterators, NewIterator(sst))
	}
	mit := NewMergeIterator(iterators)
	tables, err := Compaction(mit, level)
	if err != nil {
		t.Fatalf("Compaction: %v", err)
	}
	for _, tbl := range tables {
		path := tbl.filePath + "/" + tbl.FileName
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("reopen compaction output: %v", err)
		}
		tbl.file = f
		if err := tbl.readHeader(); err != nil {
			t.Fatalf("readHeader on compaction output: %v", err)
		}
		t.Cleanup(func() {
			f.Close()
			os.Remove(path)
		})
	}
	return tables
}

func TestCompaction_OutputIsSorted(t *testing.T) {
	f1 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{1, 1}, {3, 3}, {5, 5}}, nil)
	f2 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{2, 2}, {4, 4}, {6, 6}}, nil)
	tables := runCompaction(t, []*SSTable{f1, f2}, 1)

	prev := int64(-1)
	for _, tbl := range tables {
		entries, err := tbl.readEntry()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.Key <= prev {
				t.Errorf("output not sorted: %d after %d", e.Key, prev)
			}
			prev = e.Key
		}
	}
}

func TestCompaction_NoDuplicateKeys(t *testing.T) {
	f1 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{1, 1}, {3, 999}}, nil) // newer
	f2 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{3, 100}, {5, 5}}, nil) // older
	tables := runCompaction(t, []*SSTable{f1, f2}, 1)

	seen := map[int64]int{}
	for _, tbl := range tables {
		entries, err := tbl.readEntry()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			seen[e.Key]++
		}
	}
	for k, count := range seen {
		if count > 1 {
			t.Errorf("key %d appears %d times in compaction output", k, count)
		}
	}
}

func TestCompaction_NewerValueWins(t *testing.T) {
	f1 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{5, 999}}, nil) // newer
	f2 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{5, 111}}, nil) // older
	tables := runCompaction(t, []*SSTable{f1, f2}, 1)

	for _, tbl := range tables {
		entries, _ := tbl.readEntry()
		for _, e := range entries {
			if e.Key == 5 && e.Val != 999 {
				t.Errorf("key 5: want 999 (newer wins), got %d", e.Val)
			}
		}
	}
}

func TestCompaction_TombstonePreserved(t *testing.T) {
	mt := memTable.NewMemTable()
	mt.SkipList.Insert(1, 100)
	mt.SkipList.Insert(2, 200)
	mt.SkipList.Delete(2)
	sst, _ := FlushToSSTable(mt)
	path := sst.filePath + "/" + sst.FileName
	f, _ := os.Open(path)
	sst.file = f
	sst.readHeader()
	t.Cleanup(func() { f.Close(); os.Remove(path) })

	tables := runCompaction(t, []*SSTable{sst}, 1)
	for _, tbl := range tables {
		entries, _ := tbl.readEntry()
		for _, e := range entries {
			if e.Key == 2 {
				t.Error("key 2 is tombstoned, should not appear in compaction output")
			}
		}
	}
}

func TestCompaction_SplitsAtSizeLimit(t *testing.T) {
	var kvs []struct{ k, v int64 }
	for i := int64(1); i <= 10; i++ {
		kvs = append(kvs, struct{ k, v int64 }{i, i * 10})
	}
	f1 := makeSSTableForCompaction(t, kvs, nil)
	tables := runCompaction(t, []*SSTable{f1}, 1)
	if len(tables) != 2 {
		t.Errorf("expected 2 output tables for 10 entries at chunk=5, got %d", len(tables))
	}
}

func TestCompaction_TotalEntryCountPreserved(t *testing.T) {
	f1 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{1, 1}, {2, 2}, {3, 3}}, nil)
	f2 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{4, 4}, {5, 5}, {6, 6}}, nil)
	tables := runCompaction(t, []*SSTable{f1, f2}, 1)

	total := 0
	for _, tbl := range tables {
		entries, err := tbl.readEntry()
		if err != nil {
			t.Fatal(err)
		}
		total += len(entries)
	}
	if total != 6 {
		t.Errorf("expected 6 total entries, got %d", total)
	}
}

func TestCompaction_EmptyRemainder(t *testing.T) {
	var kvs []struct{ k, v int64 }
	for i := int64(1); i <= 5; i++ {
		kvs = append(kvs, struct{ k, v int64 }{i, i})
	}
	f1 := makeSSTableForCompaction(t, kvs, nil)
	tables := runCompaction(t, []*SSTable{f1}, 1)
	if len(tables) != 1 {
		t.Errorf("expected 1 output table, got %d", len(tables))
	}
}

func TestCompaction_OutputLevel(t *testing.T) {
	f1 := makeSSTableForCompaction(t, []struct{ k, v int64 }{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}, nil)
	tables := runCompaction(t, []*SSTable{f1}, 2)
	for _, tbl := range tables {
		if len(tbl.FileName) < 3 || tbl.FileName[:3] != "l2_" {
			t.Errorf("output file %q should be at level 2", tbl.FileName)
		}
	}
}
