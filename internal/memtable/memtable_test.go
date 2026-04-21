package memTable

import (
	"lsmash/config"
	"testing"
)

func TestNewMemTable(t *testing.T) {
	mt := NewMemTable()
	cfg := config.DefaultConfig()

	if mt == nil {
		t.Fatalf("expected non-nil memtable")
	}
	if mt.Size != 0 {
		t.Fatalf("expected size 0, got %d", mt.Size)
	}
	if mt.SkipList == nil {
		t.Fatalf("expected non-nil skiplist")
	}
	if mt.SkipList.maxLevels != cfg.SkipListMaxLevels {
		t.Fatalf("expected max levels %d, got %d", cfg.SkipListMaxLevels, mt.SkipList.maxLevels)
	}
	if mt.SkipList.levels != 1 {
		t.Fatalf("expected initial levels 1, got %d", mt.SkipList.levels)
	}
}
