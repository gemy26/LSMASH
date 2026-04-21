package config

type Config struct {
	MemTableSizeLimit   int64
	SkipListMaxLevels   int
	SkipListprobability float64
}

func DefaultConfig() Config {
	return Config{
		MemTableSizeLimit:   32000, //16 * 200
		SkipListMaxLevels:   5,
		SkipListprobability: 0.6,
	}
}
