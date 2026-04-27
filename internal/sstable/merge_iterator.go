package sstable

import (
	"container/heap"
	"io"
	memTable "lsmash/internal/memtable"
)

type IteratorHeap []*Iterator

func (h IteratorHeap) Len() int { return len(h) }
func (h IteratorHeap) Less(i, j int) bool {
	k1, _ := h[i].Key()
	k2, _ := h[j].Key()
	if k1 != k2 {
		return k1 < k2
	}
	return h[i].index < h[j].index
}
func (h IteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *IteratorHeap) Push(x any)   { *h = append(*h, x.(*Iterator)) }
func (h *IteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type MergeIterator struct {
	currEntry *memTable.Entry
	heap      *IteratorHeap
}

func NewMergeIterator(iterators []*Iterator) *MergeIterator {
	h := &IteratorHeap{}
	heap.Init(h)
	for i, it := range iterators {
		it.index = uint32(i)
		if err := it.Next(); err == nil {
			heap.Push(h, it)
		}
	}
	return &MergeIterator{
		currEntry: nil,
		heap:      h,
	}
}

func (iterator *MergeIterator) Next() bool {
	if iterator.heap.Len() == 0 {
		return false
	}
	minEntry := heap.Pop(iterator.heap).(*Iterator)
	iterator.currEntry = minEntry.Value()
	key, _ := minEntry.Key()

	if err := minEntry.Next(); err != io.EOF {
		heap.Push(iterator.heap, minEntry)
	}

	for iterator.heap.Len() > 0 {
		topKey, _ := (*iterator.heap)[0].Key()
		if topKey != key {
			break
		}
		dup := heap.Pop(iterator.heap).(*Iterator)
		if err := dup.Next(); err != io.EOF {
			heap.Push(iterator.heap, dup)
		}
	}
	return true
}

func (iterator *MergeIterator) Value() *memTable.Entry {
	return iterator.currEntry
}

func (iterator *MergeIterator) Valid() bool {
	return iterator.currEntry != nil
}

func (it *MergeIterator) Key() (int64, bool) {
	if it.currEntry != nil {
		return it.currEntry.Key, true
	}
	return -1, false
}
