package database

import (
	"nosql-engine/packages/utils/cache"
	"nosql-engine/packages/utils/config"
	database_elem "nosql-engine/packages/utils/database-elem"
	"nosql-engine/packages/utils/memtable"
	"nosql-engine/packages/utils/sstable"
	"nosql-engine/packages/utils/wal"
	"time"
)

type Database struct {
	config   config.Config
	memtable memtable.MemTable
	wal      wal.WAL
	cache    cache.Cache
}

func New() *Database {
	config := config.GetConfig()

	walObj := wal.New("data/wal", uint32(config.WalSegmentSize), 0)
	walEntries := walObj.ReadAllEntries()

	var memtableObj *memtable.MemTable
	if config.MemtableStructure == "btree" {
		memtableObj = memtable.New(int(config.MemtableSize), config.MemtableStructure, config.BTreeMax, config.BTreeMin, int(config.SummaryCount), config.SSTableFiles)
	} else {
		memtableObj = memtable.New(int(config.MemtableSize), config.MemtableStructure, config.SkipListLevels, 0, int(config.SummaryCount), config.SSTableFiles)
	}

	for _, entry := range walEntries {
		memtableObj.Insert(entry.Key, database_elem.DatabaseElem{
			Value:     entry.Value,
			Tombstone: entry.Tombstone,
			Timestamp: entry.Timestamp,
		})

		if memtableObj.CheckFlushed() {
			walObj.EmptyWAL()
		}
	}

	return &Database{
		config:   *config,
		memtable: *memtableObj,
		wal:      *walObj,
		cache:    cache.New(int(config.CacheSize)),
	}
}

func (db *Database) Put(key string, value []byte) bool {
	dbElem := &database_elem.DatabaseElem{
		Value:     value,
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	}

	if db.wal.PutEntry(key, value, 0) {
		db.memtable.Insert(key, *dbElem)

		if db.memtable.CheckFlushed() {
			db.wal.EmptyWAL()
		}

		return true
	}

	return false
}

func (db *Database) Delete(key string) bool {
	if db.wal.PutEntry(key, []byte(""), 1) {
		db.memtable.Delete(key)
		db.cache.Delete(key)

		if db.memtable.CheckFlushed() {
			db.wal.EmptyWAL()
		}

		return true
	}

	return false
}

func (db *Database) Get(key string) []byte {
	found, keyValue := db.memtable.Find(key)

	if found {
		if keyValue.Value.Tombstone == 1 {
			return nil
		} else {
			return keyValue.Value.Value
		}
	}

	if db.cache.Contains(key) {
		elem := db.cache.Refer(key, database_elem.DatabaseElem{
			Value:     []byte(""),
			Tombstone: 0,
			Timestamp: uint64(time.Now().Unix()),
		})

		if elem.Tombstone == 1 {
			return nil
		} else {
			return elem.Value
		}
	}

	found, elem := sstable.Find(key, "data/usertables", db.config.LsmLevels, db.config.SSTableFiles)

	if found {
		db.cache.Refer(key, *elem)

		if elem.Tombstone == 1 {
			return nil
		} else {
			return elem.Value
		}
	}

	return nil
}

// treba dodati citanje iz wala i rekonstrukciju memtable-a
// dodati tipove
// dodati rate limiting/token bucket
