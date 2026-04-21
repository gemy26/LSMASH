package lsmash

import (
	"lsmash/config"
	memTable "lsmash/internal/memtable"
	"testing"
)

func TestEngineInsertGet(t *testing.T) {
	cfg := config.DefaultConfig()
	e := &Engine{
		memtable: memTable.NewMemTable(),
		config:   cfg,
	}

	e.Insert(1, 10)
	e.Insert(2, 20)

	if got := e.Get(1); got != 10 {
		t.Fatalf("expected key 1 value 10, got %d", got)
	}
	if got := e.Get(2); got != 20 {
		t.Fatalf("expected key 2 value 20, got %d", got)
	}
	if got := e.Get(3); got != -1 {
		t.Fatalf("expected missing key to return -1, got %d", got)
	}
}

func TestEngineGetFromImmutable(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MemTableSizeLimit = 16
	e := &Engine{
		memtable: memTable.NewMemTable(),
		config:   cfg,
	}

	e.Insert(1, 10)
	e.Insert(2, 20)
	e.Insert(3, 30)

	if len(e.immutable) != 2 {
		t.Fatalf("expected 2 immutable memtables, got %d", len(e.immutable))
	}
	if got := e.Get(1); got != 10 {
		t.Fatalf("expected key 1 value 10 from immutable, got %d", got)
	}
}

func TestEngineDeleteSkipped(t *testing.T) {
	t.Skip("Delete behavior is not implemented via tombstones in SkipList yet")
}
