package memTable

type Iterator struct {
	curr *Node
	end  int64
}

func (i *Iterator) Next() bool {
	if i.curr == nil {
		return false
	}
	i.curr = i.curr.next[0]
	if i.curr == nil || i.curr.key > i.end {
		i.curr = nil
		return false
	}
	return true
}

func (i *Iterator) Key() int64 {
	return i.curr.key
}

func (i *Iterator) Value() int64 {
	return i.curr.val.data
}

func (i *Iterator) Valid() bool {
	return i.curr != nil
}

func (it *Iterator) Entry() Entry {
	return Entry{
		Key:        it.curr.key,
		Val:        it.curr.val.data,
		Tombstoned: it.curr.val.tombstone,
	}
}
