package memTable

type Iterator struct {
	curr *Node
	end  int
}

func (i *Iterator) Next() bool {
	if i.curr == nil {
		return false
	}

	i.curr = i.curr.next[0]
	for i.curr != nil && i.curr.val.tombstone == true { //skip all deleted nodes
		i.curr = i.curr.next[0]
	}
	if i.curr == nil {
		return false
	}
	if i.curr != nil && i.curr.key > i.end {
		i.curr = nil
		return false
	}
	return true
}

func (i *Iterator) Key() int {
	return i.curr.key
}

func (i *Iterator) Value() int {
	return i.curr.val.data
}

func (i *Iterator) Valid() bool {
	return i.curr != nil && !i.curr.val.tombstone
}

func (it *Iterator) Entry() Entry {
	return Entry{
		Key: it.curr.key,
		Val: it.curr.val.data,
	}
}
