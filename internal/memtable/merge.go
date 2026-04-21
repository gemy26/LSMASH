package memTable

import (
	"container/heap"
)

type IteratorHeap []*Iterator

func (h IteratorHeap) Len() int           { return len(h) }
func (h IteratorHeap) Less(i, j int) bool { return h[i].Key() < h[j].Key() }
func (h IteratorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *IteratorHeap) Push(x any)        { *h = append(*h, x.(*Iterator)) }
func (h *IteratorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func Merge(iterators []*Iterator) []Entry {
	h := &IteratorHeap{}
	heap.Init(h)
	list := make([]Entry, 0)
	seen := make(map[int]bool)
	for _, it := range iterators {
		if it.Valid() {
			heap.Push(h, it)
		}
	}

	for h.Len() > 0 {
		//pop the minimum one
		//if not in answer add it to result
		//if already in answer its old one skip it
		//push the next of the poped/skipped one
		minIt := heap.Pop(h).(*Iterator)
		if minIt.Valid() {
			entry := minIt.Entry()
			if !seen[entry.Key] {
				seen[entry.Key] = true
				list = append(list, entry)
			}
		}
		if minIt.Next() {
			heap.Push(h, minIt)
		}
	}
	return list
}
