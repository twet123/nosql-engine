package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WalSegmentSize    uint64 `yaml:"wal_segment_size"`
	MemtableSize      uint64 `yaml:"memtable_size"`
	MemtableStructure string `yaml:"memtable_structure"`
	BTreeMin          uint64 `yaml:"btree_min"`
	BTreeMax          uint64 `yaml:"btree_max"`
	SkipListLevels    uint64 `yaml:"skiplist_levels"`
	SummaryCount      uint64 `yaml:"summary_count"`
}

func GetConfig() *Config {
	var config Config
	configData, err := os.ReadFile("config.yml")

	if err != nil {
		config.WalSegmentSize = 20
		config.MemtableSize = 20
		config.MemtableStructure = "skiplist"
		config.BTreeMin = 2
		config.BTreeMax = 4
		config.SkipListLevels = 32
		config.SummaryCount = 3
	}

	yaml.Unmarshal(configData, &config)
	return &config
}
