package config

import (
	"os"
	"path/filepath"
)

type Config struct {
	MemTableSizeLimit     int64 //max number of entries
	SkipListMaxLevels     int
	SkipListprobability   float64
	SstableFileSizeLimit  int64 // max file size in bytes
	SstablelevelSizeLimit int64 // max total size of level 0 in bytes
	LevelSizeMultiplier   int64 // multiplier for each level size
	MaxImmutableMemtables int
	WorkingDir            string
}

func DefaultConfig() Config {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	dataDir := filepath.Join(wd, "data")
	_ = os.MkdirAll(dataDir, 0755)
	return Config{
		MemTableSizeLimit:     5, // number of entries in memtable
		SkipListMaxLevels:     5,
		SkipListprobability:   0.1,
		SstableFileSizeLimit:  116, // 116 bytes (5 entries + bloom + header)
		SstablelevelSizeLimit: 580, // 580 bytes for Level 0 (5 SSTables)
		LevelSizeMultiplier:   10,  // Level N will be 10x larger than level N-1
		MaxImmutableMemtables: 5,
		WorkingDir:            dataDir,
	}
}

func (c Config) GetLevelDataSizeLimit(level int) int64 {
	size := c.SstablelevelSizeLimit
	for i := 0; i < level; i++ {
		size *= c.LevelSizeMultiplier
	}
	return size
}
