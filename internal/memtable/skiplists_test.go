package memTable

import "testing"

func markTombstone(s *SkipList, key int) {
	node := s.head.next[0]
	for node != nil && node.key < key {
		node = node.next[0]
	}
	if node != nil && node.key == key {
		node.val.tombstone = true
	}
}

func TestSkipListInsertSearch(t *testing.T) {
	s := NewSkipList(5, 0.5)
	s.Insert(1, 10)
	s.Insert(2, 20)

	if v, ok := s.Search(1); !ok || v != 10 {
		t.Fatalf("expected key 1 value 10, got %v, %v", v, ok)
	}
	if v, ok := s.Search(2); !ok || v != 20 {
		t.Fatalf("expected key 2 value 20, got %v, %v", v, ok)
	}

	s.Insert(1, 15)
	if v, ok := s.Search(1); !ok || v != 15 {
		t.Fatalf("expected key 1 value 15 after update, got %v, %v", v, ok)
	}

	markTombstone(s, 1)
	if _, ok := s.Search(1); ok {
		t.Fatalf("expected tombstoned key to be absent")
	}
}

func TestSkipListScan(t *testing.T) {
	s := NewSkipList(5, 0.5)
	for i := 1; i <= 5; i++ {
		s.Insert(i, i*10)
	}
	markTombstone(s, 3)

	result := s.Scan(2, 4)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if result[0].Key != 2 || result[0].Val != 20 {
		t.Fatalf("unexpected first entry: %+v", result[0])
	}
	if result[1].Key != 4 || result[1].Val != 40 {
		t.Fatalf("unexpected second entry: %+v", result[1])
	}
}

func TestSkipListIteratorSkipsTombstones(t *testing.T) {
	s := NewSkipList(5, 0.5)
	for i := 1; i <= 5; i++ {
		s.Insert(i, i*10)
	}
	markTombstone(s, 2)
	markTombstone(s, 4)

	it := s.Iterator()
	if !it.Valid() {
		t.Fatalf("expected iterator to be valid at start")
	}

	entries := make([]Entry, 0, 5)
	entries = append(entries, it.Entry())
	for it.Next() {
		entries = append(entries, it.Entry())
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Key != 1 || entries[1].Key != 3 || entries[2].Key != 5 {
		t.Fatalf("unexpected iterator keys: %+v", entries)
	}
}
