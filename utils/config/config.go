package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WalSegmentSize    uint64   `yaml:"wal_segment_size"`
	MemtableSize      uint64   `yaml:"memtable_size"`
	MemtableStructure string   `yaml:"memtable_structure"`
	BTreeMin          uint64   `yaml:"btree_min"`
	BTreeMax          uint64   `yaml:"btree_max"`
	SkipListLevels    uint64   `yaml:"skiplist_levels"`
	SummaryCount      uint64   `yaml:"summary_count"`
	CacheSize         uint64   `yaml:"cache_size"`
	LsmLevels         uint64   `yaml:"lsm_levels"`
	SSTableFiles      string   `yaml:"sstable_files"`
	LsmMaxPerLevel    uint64   `yaml:"lsm_max_per_level"`
	ReqPerTime        uint64   `yaml:"req_per_time"`
	TimeUnit          string   `yaml:"time_unit"` // possible values "second", "minute", "day"
	LsmLeveledComp    []uint64 `yaml:"lsm_leveled_compaction_cfg"`
	SSTableSize       uint64   `yaml:"sstable_size"`
}

func GetConfig() *Config {
	var config Config
	configData, err := os.ReadFile("config.yml")

	if err != nil {
		print(err)
		config.WalSegmentSize = 20
		config.MemtableSize = 20
		config.MemtableStructure = "skiplist"
		config.BTreeMin = 2
		config.BTreeMax = 4
		config.SkipListLevels = 32
		config.SummaryCount = 3
		config.CacheSize = 10
		config.SSTableFiles = "many"
		config.ReqPerTime = 60
		config.TimeUnit = "minute"
		config.LsmMaxPerLevel = 5
		config.LsmLevels = 4
		config.SSTableSize = 10
		config.LsmLeveledComp = []uint64{4, 10, 100}
	} else {
		err := yaml.Unmarshal(configData, &config)
		if err != nil {
			panic(err)
		}
	}
	return &config
}
