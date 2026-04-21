package memTable

import "testing"

func TestMergeIterators(t *testing.T) {
	s1 := NewSkipList(5, 0.5)
	s2 := NewSkipList(5, 0.5)

	for _, kv := range []Entry{{Key: 1, Val: 10}, {Key: 3, Val: 30}, {Key: 5, Val: 50}} {
		s1.Insert(kv.Key, kv.Val)
	}
	for _, kv := range []Entry{{Key: 2, Val: 20}, {Key: 4, Val: 40}, {Key: 6, Val: 60}} {
		s2.Insert(kv.Key, kv.Val)
	}

	merged := Merge([]*Iterator{s1.Iterator(), s2.Iterator()})
	if len(merged) != 6 {
		t.Fatalf("expected 6 entries, got %d", len(merged))
	}
	for i, entry := range merged {
		expectedKey := i + 1
		expectedVal := expectedKey * 10
		if entry.Key != expectedKey || entry.Val != expectedVal {
			t.Fatalf("unexpected entry at index %d: %+v", i, entry)
		}
	}
}
