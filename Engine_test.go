package lsmash

import (
	"lsmash/config"
	"lsmash/internal/mainfest"
	"lsmash/internal/wal"
	"os"
	"path/filepath"
	"testing"
)

func cleanDataDir(t *testing.T) {
	t.Helper()
	cfg := config.DefaultConfig()
	entries, _ := os.ReadDir(cfg.WorkingDir)
	for _, e := range entries {
		os.Remove(filepath.Join(cfg.WorkingDir, e.Name()))
	}
}

func createTestEngine(t *testing.T) *Engine {
	t.Helper()
	cleanDataDir(t)
	t.Cleanup(func() { cleanDataDir(t) })

	cfg := config.DefaultConfig()
	e, err := CreateEngine(cfg)
	if err != nil {
		t.Fatalf("CreateEngine: %v", err)
	}
	return e
}

func TestEngineInsertGet(t *testing.T) {
	e := createTestEngine(t)
	e.Insert(1, 10)
	e.Insert(2, 20)
	e.Insert(3, 30)
	if got := e.Get(1); got != 10 {
		t.Fatalf("expected key 1: 10, got %d", got)
	}
	if got := e.Get(2); got != 20 {
		t.Fatalf("expected key 2: 20, got %d", got)
	}
	if got := e.Get(3); got != 30 {
		t.Fatalf("expected key 3: 30, got %d", got)
	}
}

func TestEngineOverwrite(t *testing.T) {
	e := createTestEngine(t)
	e.Insert(1, 10)
	e.Insert(1, 99)
	if got := e.Get(1); got != 99 {
		t.Fatalf("expected key 1: 99 after overwrite, got %d", got)
	}
}

func TestEngineGetMissingKey(t *testing.T) {
	e := createTestEngine(t)
	e.Insert(1, 10)
	if got := e.Get(999); got != -1 {
		t.Fatalf("expected missing key -1, got %d", got)
	}
}

func TestEngineGetFromImmutable(t *testing.T) {
	e := createTestEngine(t)

	for i := int64(1); i <= 6; i++ {
		e.Insert(i, i*10)
	}

	if len(e.immutable) != 1 {
		t.Fatalf("expected 1 immutable memtable, got %d", len(e.immutable))
	}
	if got := e.Get(1); got != 10 {
		t.Fatalf("expected key 1: 10 from immutable, got %d", got)
	}
	if got := e.Get(6); got != 60 {
		t.Fatalf("expected key 6: 60 from active memtable, got %d", got)
	}
}

func TestEngineMultipleImmutables(t *testing.T) {
	e := createTestEngine(t)
	for i := int64(1); i <= 24; i++ {
		e.Insert(i, i*10)
	}

	if len(e.immutable) != 4 {
		t.Fatalf("expected 4 immutable memtables before flush, got %d", len(e.immutable))
	}
	if got := e.Get(1); got != 10 {
		t.Fatalf("expected key 1: 10, got %d", got)
	}
	if got := e.Get(24); got != 240 {
		t.Fatalf("expected key 24: 240, got %d", got)
	}
}

// So after insert 26: L0=0, L1=0, L2=compacted SSTables Until i set a larger size for L2
func TestEngineFlushAndCompaction(t *testing.T) {
	e := createTestEngine(t)
	for i := int64(1); i <= 26; i++ {
		e.Insert(i, i*10)
	}
	if len(e.immutable) != 0 {
		t.Fatalf("expected 0 immutable memtables after flush, got %d", len(e.immutable))
	}

	if len(e.sstable[0]) != 0 {
		t.Fatalf("expected L0 empty after compaction, got %d SSTables", len(e.sstable[0]))
	}
	if len(e.sstable[1]) != 0 {
		t.Fatalf("expected L1 empty after cascading compaction, got %d SSTables", len(e.sstable[1]))
	}
	if len(e.sstable[2]) == 0 {
		t.Fatal("expected L2 to have SSTables after cascading compaction, got 0")
	}

	if e.memtable.Size != 1 {
		t.Fatalf("expected active memtable size 1, got %d", e.memtable.Size)
	}
}

func TestEngineGetFromSSTableAfterCompaction(t *testing.T) {
	e := createTestEngine(t)
	for i := int64(1); i <= 26; i++ {
		e.Insert(i, i*10)
	}
	if got := e.Get(1); got != 10 {
		t.Fatalf("expected key 1: 10 from SSTable, got %d", got)
	}
	if got := e.Get(15); got != 150 {
		t.Fatalf("expected key 15: 150 from SSTable, got %d", got)
	}
	if got := e.Get(25); got != 250 {
		t.Fatalf("expected key 25: 250 from SSTable, got %d", got)
	}
	if got := e.Get(26); got != 260 {
		t.Fatalf("expected key 26: 260 from active memtable, got %d", got)
	}
	if got := e.Get(9999); got != -1 {
		t.Fatalf("expected missing key: -1, got %d", got)
	}
}

func TestEngineManifestRecordsOnCompaction(t *testing.T) {
	e := createTestEngine(t)

	// Trigger flush + cascading compaction (L0→L1→L2)
	for i := int64(1); i <= 26; i++ {
		e.Insert(i, i*10)
	}

	records := e.mainfest.Reply()
	if len(records) == 0 {
		t.Fatal("expected manifest records after compaction, got none")
	}

	// Cascading compaction produces 4 records:
	//   L0→L1: record for L0 (removed old), record for L1 (removed old + added new)
	//   L1→L2: record for L1 (removed), record for L2 (added)
	compactionCount := 0
	for _, r := range records {
		if r.Type == "Compaction" {
			compactionCount++
		}
	}
	if compactionCount < 4 {
		t.Fatalf("expected at least 4 Compaction manifest records for cascading compaction, got %d", compactionCount)
	}

	// Verify records span L0, L1, and L2
	levelsPresent := map[int8]bool{}
	for _, r := range records {
		levelsPresent[r.Level] = true
	}
	for _, level := range []int8{0, 1, 2} {
		if !levelsPresent[level] {
			t.Errorf("expected manifest record for level %d, but none found", level)
		}
	}
}

func TestEngineManifestReplay(t *testing.T) {
	cleanDataDir(t)
	t.Cleanup(func() { cleanDataDir(t) })

	cfg := config.DefaultConfig()

	e1, err := CreateEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 26; i++ {
		e1.Insert(i, i*10)
	}

	origRecords := e1.mainfest.Reply()
	if len(origRecords) == 0 {
		t.Fatal("expected manifest records after compaction")
	}

	mf, err := mainfest.NewMainfest()
	if err != nil {
		t.Fatal(err)
	}
	replayedRecords := mf.Reply()

	if len(replayedRecords) != len(origRecords) {
		t.Fatalf("manifest replay: expected %d records, got %d", len(origRecords), len(replayedRecords))
	}

	for i, orig := range origRecords {
		got := replayedRecords[i]
		if orig.Type != got.Type {
			t.Errorf("record %d: type mismatch: expected %q, got %q", i, orig.Type, got.Type)
		}
		if orig.Level != got.Level {
			t.Errorf("record %d: level mismatch: expected %d, got %d", i, orig.Level, got.Level)
		}
		if len(orig.Added) != len(got.Added) {
			t.Errorf("record %d: added count mismatch: expected %d, got %d", i, len(orig.Added), len(got.Added))
		}
		if len(orig.Removed) != len(got.Removed) {
			t.Errorf("record %d: removed count mismatch: expected %d, got %d", i, len(orig.Removed), len(got.Removed))
		}
	}
}

func TestEngineRecovery(t *testing.T) {
	cleanDataDir(t)
	t.Cleanup(func() { cleanDataDir(t) })
	cfg := config.DefaultConfig()
	e1, err := CreateEngine(cfg)
	if err != nil {
		t.Fatalf("CreateEngine 1: %v", err)
	}
	e1.Insert(1, 10)
	e1.Insert(2, 20)
	e1.Insert(3, 30)

	e2, err := CreateEngine(cfg)
	if err != nil {
		t.Fatalf("CreateEngine 2: %v", err)
	}

	if got := e2.Get(1); got != 10 {
		t.Fatalf("after recovery: expected key 1: 10, got %d", got)
	}
	if got := e2.Get(2); got != 20 {
		t.Fatalf("after recovery: expected key 2: 20, got %d", got)
	}
	if got := e2.Get(3); got != 30 {
		t.Fatalf("after recovery: expected key 3: 30, got %d", got)
	}
}

func TestEngineWALReplay(t *testing.T) {
	cleanDataDir(t)
	t.Cleanup(func() { cleanDataDir(t) })

	cfg := config.DefaultConfig()
	e, err := CreateEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.Insert(10, 100)
	e.Insert(20, 200)

	// WAL should contain 2 records
	records, err := wal.Reply()
	if err != nil {
		t.Fatalf("wal.Reply: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 WAL records, got %d", len(records))
	}
}

func TestEngineDelete(t *testing.T) {
	e := createTestEngine(t)
	e.Insert(1, 10)
	e.Insert(2, 20)
	e.Delete(1)

	if got := e.Get(1); got != -1 {
		t.Fatalf("expected deleted key 1: -1, got %d", got)
	}
	if got := e.Get(2); got != 20 {
		t.Fatalf("expected key 2: 20, got %d", got)
	}
}

func TestEngineThreeLevels(t *testing.T) {
	e := createTestEngine(t)

	if len(e.sstable) != 3 {
		t.Fatalf("expected 3 SSTable levels, got %d", len(e.sstable))
	}
}
