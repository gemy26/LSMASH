package sstable

import (
	"io"
	"os"
	"testing"

	memTable "lsmash/internal/memtable"
)

func flushAndReopen(t *testing.T, kvs []struct{ k, v int64 }, tombstones []int64) *SSTable {
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
	path := sst.filePath + "/" + sst.fileName
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	sst.file = f
	if err := sst.readHeader(); err != nil {
		t.Fatalf("readHeader: %v", err)
	}
	t.Cleanup(func() {
		f.Close()
		os.Remove(path)
	})
	return sst
}

func TestIterator_TraversesInOrder(t *testing.T) {
	sst := flushAndReopen(t, []struct{ k, v int64 }{{1, 10}, {2, 20}, {3, 30}}, nil)
	it := NewIterator(sst)

	expected := []int64{1, 2, 3}
	for _, want := range expected {
		if err := it.Next(); err != nil {
			t.Fatalf("Next() error: %v", err)
		}
		k, ok := it.Key()
		if !ok || k != want {
			t.Errorf("Key() = (%d, %v), want (%d, true)", k, ok, want)
		}
	}
	if err := it.Next(); err != io.EOF {
		t.Errorf("expected EOF after last entry, got %v", err)
	}
}

func TestIterator_StopsAtEntryCount(t *testing.T) {
	sst := flushAndReopen(t, []struct{ k, v int64 }{{1, 1}, {2, 2}}, nil)
	it := NewIterator(sst)
	count := 0
	for it.Next() == nil {
		count++
	}
	if count != 2 {
		t.Errorf("iterated %d entries, want 2", count)
	}
}

func TestIterator_TombstoneVisible(t *testing.T) {
	sst := flushAndReopen(t, []struct{ k, v int64 }{{1, 1}, {2, 2}}, []int64{2})
	it := NewIterator(sst)
	var entries []*memTable.Entry
	for it.Next() == nil {
		e := it.Value()
		entries = append(entries, &memTable.Entry{Key: e.Key, Val: e.Val, Tombstoned: e.Tombstoned})
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if !entries[1].Tombstoned {
		t.Error("second entry should be tombstoned")
	}
}

func TestIterator_ValidBeforeAndAfter(t *testing.T) {
	sst := flushAndReopen(t, []struct{ k, v int64 }{{5, 50}}, nil)
	it := NewIterator(sst)
	if it.Valid() {
		t.Error("Valid() should be false before first Next()")
	}
	it.Next()
	if !it.Valid() {
		t.Error("Valid() should be true after successful Next()")
	}
}

func TestIterator_SingleEntry(t *testing.T) {
	sst := flushAndReopen(t, []struct{ k, v int64 }{{42, 999}}, nil)
	it := NewIterator(sst)
	if err := it.Next(); err != nil {
		t.Fatalf("Next(): %v", err)
	}
	k, ok := it.Key()
	if !ok || k != 42 {
		t.Errorf("Key() = (%d, %v), want (42, true)", k, ok)
	}
	if err := it.Next(); err != io.EOF {
		t.Error("expected EOF on second Next()")
	}
}

func makeMergeIterator(t *testing.T, files [][]struct{ k, v int64 }) *MergeIterator {
	t.Helper()
	var iterators []*Iterator
	for _, kvs := range files {
		sst := flushAndReopen(t, kvs, nil)
		iterators = append(iterators, NewIterator(sst))
	}
	return NewMergeIterator(iterators)
}

func TestMergeIterator_NonOverlappingFiles(t *testing.T) {
	mit := makeMergeIterator(t, [][]struct{ k, v int64 }{
		{{1, 10}, {3, 30}},
		{{2, 20}, {4, 40}},
	})
	expected := []int64{1, 2, 3, 4}
	for _, want := range expected {
		if !mit.Next() {
			t.Fatalf("Next() returned false, want key %d", want)
		}
		k, _ := mit.Key()
		if k != want {
			t.Errorf("Key() = %d, want %d", k, want)
		}
	}
	if mit.Next() {
		t.Error("expected merge iterator to be exhausted")
	}
}

func TestMergeIterator_OverlappingKeys_NewerWins(t *testing.T) {
	sst1 := flushAndReopen(t, []struct{ k, v int64 }{{1, 100}, {2, 999}}, nil) // newer
	sst2 := flushAndReopen(t, []struct{ k, v int64 }{{2, 200}, {3, 300}}, nil) // older
	mit := NewMergeIterator([]*Iterator{NewIterator(sst1), NewIterator(sst2)})

	results := map[int64]int64{}
	for mit.Next() {
		k, _ := mit.Key()
		results[k] = mit.Value().Val
	}
	if results[2] != 999 {
		t.Errorf("key 2: want 999 (newer wins), got %d", results[2])
	}
}

func TestMergeIterator_SingleFile(t *testing.T) {
	mit := makeMergeIterator(t, [][]struct{ k, v int64 }{
		{{1, 1}, {2, 2}, {3, 3}},
	})
	count := 0
	for mit.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}
}

func TestMergeIterator_AllSameKey(t *testing.T) {
	sst1 := flushAndReopen(t, []struct{ k, v int64 }{{5, 1}}, nil)
	sst2 := flushAndReopen(t, []struct{ k, v int64 }{{5, 2}}, nil)
	sst3 := flushAndReopen(t, []struct{ k, v int64 }{{5, 3}}, nil)
	mit := NewMergeIterator([]*Iterator{NewIterator(sst1), NewIterator(sst2), NewIterator(sst3)})

	count := 0
	for mit.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 deduplicated entry, got %d", count)
	}
}

func TestMergeIterator_TombstonePropagated(t *testing.T) {
	mt := memTable.NewMemTable()
	mt.SkipList.Insert(1, 100)
	mt.SkipList.Insert(2, 200)
	mt.SkipList.Delete(2)
	sst, err := FlushToSSTable(mt)
	if err != nil {
		t.Fatal(err)
	}
	path := sst.filePath + "/" + sst.fileName
	f, _ := os.Open(path)
	sst.file = f
	sst.readHeader()
	t.Cleanup(func() { f.Close(); os.Remove(path) })

	mit := NewMergeIterator([]*Iterator{NewIterator(sst)})
	tombstoneFound := false
	for mit.Next() {
		if mit.Value().Tombstoned {
			tombstoneFound = true
		}
	}
	if !tombstoneFound {
		t.Error("merge iterator should propagate tombstones for compaction")
	}
}

func TestMergeIterator_ThreeFilesFullySorted(t *testing.T) {
	mit := makeMergeIterator(t, [][]struct{ k, v int64 }{
		{{1, 1}, {4, 4}, {7, 7}},
		{{2, 2}, {5, 5}, {8, 8}},
		{{3, 3}, {6, 6}, {9, 9}},
	})
	prev := int64(-1)
	count := 0
	for mit.Next() {
		k, _ := mit.Key()
		if k <= prev {
			t.Errorf("out of order: got %d after %d", k, prev)
		}
		prev = k
		count++
	}
	if count != 9 {
		t.Errorf("expected 9 entries, got %d", count)
	}
}
