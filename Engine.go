package lsmash

import (
	config "lsmash/config"
	"lsmash/internal/memtable"
)

type Engine struct {
	memtable  *memTable.MemTable
	immutable []*memTable.MemTable
	config    config.Config
}

func (e *Engine) Insert(key int, value int) {
	if e.memtable.Size+16 > e.config.MemTableSizeLimit {
		e.immutable = append([]*memTable.MemTable{e.memtable}, e.immutable...)
		e.memtable = memTable.NewMemTable()
	}
	e.memtable.SkipList.Insert(key, value)
	e.memtable.Size += 16
}
func (e *Engine) Find(key int) int {
	// first try to get it from memtable the active one and immutables
	// if not found search in the
	// sstables in all level in order from 1 -> n
	if val, ok := e.memtable.SkipList.Search(key); ok {
		return val
	}
	if val, ok := e.memtable.SkipList.Search(key); ok {
		return val
	}
	// 3. TODO: search SSTables level 0 → n
	return -1
}

func (e *Engine) Delete(key int) {
	e.memtable.SkipList.Delete(key)
}
