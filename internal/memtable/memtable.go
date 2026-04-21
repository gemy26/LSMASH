package memTable

import "lsmash/config"

type MemTable struct {
	SkipList *SkipList
	Size     int64
}

func NewMemTable() *MemTable {
	cfg := config.DefaultConfig()
	return &MemTable{
		SkipList: NewSkipList(cfg.SkipListMaxLevels, cfg.SkipListprobability),
		Size:     0,
	}
}
