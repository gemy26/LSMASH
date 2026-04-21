package memTable

import (
	"fmt"
	"math/rand"
	"strings"
)

type Value struct {
	data      int
	tombstone bool
}
type Node struct {
	key  int
	val  Value
	next []*Node // next[i] is the forward pointer at level i
}

type Entry struct {
	Key int
	Val int
}

type SkipList struct {
	maxLevels int
	levels    int
	head      *Node
	p         float64
}

func NewSkipList(maxLevels int, p float64) *SkipList {
	return &SkipList{
		maxLevels: maxLevels,
		levels:    1,
		head:      &Node{next: make([]*Node, maxLevels)},
		p:         p,
	}
}

func (s *SkipList) randomLevel() int {
	level := 1
	for level < s.maxLevels && rand.Float64() < s.p {
		level++
	}
	return level
}

func (s *SkipList) Search(key int) (int, bool) {
	node := s.head
	for l := s.levels - 1; l >= 0; l-- {
		for node.next[l] != nil && node.next[l].key < key {
			node = node.next[l]
		}
	}
	candidate := node.next[0]
	if candidate != nil && candidate.key == key {
		if candidate.val.tombstone {
			return 0, false
		}
		return candidate.val.data, true
	}
	return 0, false
}

func (s *SkipList) Insert(key int, val int) {
	node := s.head
	update := make([]*Node, s.maxLevels)

	for l := s.levels - 1; l >= 0; l-- {
		for node.next[l] != nil && node.next[l].key < key {
			node = node.next[l]
		}
		update[l] = node
	}

	if node.next[0] != nil && node.next[0].key == key {
		node.next[0].val.data = val
		return
	}

	newLevel := s.randomLevel()

	if newLevel > s.levels {
		for i := s.levels; i < newLevel; i++ {
			update[i] = s.head
		}
		s.levels = newLevel
	}

	newNode := &Node{key: key, val: Value{val, false}, next: make([]*Node, newLevel)}
	for l := 0; l < newLevel; l++ {
		newNode.next[l] = update[l].next[l]
		update[l].next[l] = newNode
	}
}

func (s *SkipList) Traverse() {
	var nodes []*Node
	cur := s.head.next[0]
	for cur != nil {
		nodes = append(nodes, cur)
		cur = cur.next[0]
	}

	if len(nodes) == 0 {
		fmt.Println("(empty skip list)")
		return
	}

	maxW := 0
	for _, n := range nodes {
		if w := len(fmt.Sprintf("%d", n.key)); w > maxW {
			maxW = w
		}
	}

	nodeSlot := func(key int) string {
		return fmt.Sprintf(" --> [%*d]", maxW, key)
	}
	slotWidth := len(nodeSlot(0))
	dashSlot := strings.Repeat("-", slotWidth)

	fmt.Printf("\n=== Skip List (%d levels) ===\n", s.levels)
	for l := s.levels - 1; l >= 0; l-- {
		fmt.Printf("L%d: HEAD", l)
		for _, n := range nodes {
			if len(n.next) > l {
				if n.val.tombstone == true {
					fmt.Print("(x deleted) ")
				}
				fmt.Print(nodeSlot(n.key))
			} else {
				fmt.Print(dashSlot)
			}
		}
		fmt.Println(" --> nil")
	}
	fmt.Println()
}

func (s *SkipList) Delete(key int) {
	node := s.head
	for l := s.levels - 1; l >= 0; l-- {
		for node.next[l] != nil && node.next[l].key < key {
			node = node.next[l]
		}
		if node.key == key {
			node.val.tombstone = true
		}
	}
}

func (s *SkipList) Scan(start, end int) []Entry {
	node := s.head.next[0]
	data := []Entry{}

	for node != nil && node.key < start {
		node = node.next[0]
	}
	for ; node != nil && node.key >= start && node.key <= end; node = node.next[0] {
		if !node.val.tombstone {
			data = append(data, Entry{Key: node.key, Val: node.val.data})
		}
	}
	return data
}
