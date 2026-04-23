package config

import (
	"os"
	"path/filepath"
)

type Config struct {
	MemTableSizeLimit     int64
	SkipListMaxLevels     int
	SkipListprobability   float64
	SstableFileSizeLimit  int64
	SstablelevelSizeLimit int64 //level 0
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
		MemTableSizeLimit:     85,
		SkipListMaxLevels:     5,
		SkipListprobability:   0.1,
		SstableFileSizeLimit:  116,
		SstablelevelSizeLimit: 348,
		WorkingDir:            dataDir,
	}
}
