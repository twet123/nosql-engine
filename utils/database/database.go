package database

import (
	bloomfilter "nosql-engine/packages/utils/bloom-filter"
	"nosql-engine/packages/utils/cache"
	"nosql-engine/packages/utils/cms"
	"nosql-engine/packages/utils/compaction"
	"nosql-engine/packages/utils/config"
	database_elem "nosql-engine/packages/utils/database-elem"
	generic_types "nosql-engine/packages/utils/generic-types"
	"nosql-engine/packages/utils/hll"
	"nosql-engine/packages/utils/memtable"
	simhash "nosql-engine/packages/utils/sim-hash"
	"nosql-engine/packages/utils/sstable"
	tokenbucket "nosql-engine/packages/utils/token-bucket"
	"nosql-engine/packages/utils/wal"
	"sort"
	"strings"
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

	walObj := wal.New("data/wal/", uint32(config.WalSegmentSize), 0)
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
	if !db.CheckTokens() || checkReserved(key) {
		return false
	}

	return db.put(key, value)
}

func (db *Database) put(key string, value []byte) bool {
	dbElem := &database_elem.DatabaseElem{
		Value:     value,
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	}

	if db.wal.PutEntry(key, value, 0) {
		db.memtable.Insert(key, *dbElem)

		if db.memtable.CheckFlushed() {
			db.wal.EmptyWAL()
			for i := 0; i < int(db.config.LsmLevels); i++ {
				compaction.MergeCompaction(i, "data/usertables")
			}
		}

		return true
	}

	return false
}

func (db *Database) Delete(key string) bool {
	if !db.CheckTokens() || checkReserved(key) {
		return false
	}

	return db.delete(key)
}

func (db *Database) delete(key string) bool {
	if db.wal.PutEntry(key, []byte(""), 1) {
		db.memtable.Delete(key)
		db.cache.Delete(key)

		if db.memtable.CheckFlushed() {
			db.wal.EmptyWAL()
			for i := 0; i < int(db.config.LsmLevels); i++ {
				compaction.MergeCompaction(i, "data/usertables")
			}
		}

		return true
	}

	return false
}

// first parameter will return false if the request limit was exceeded or you tried to access system values
func (db *Database) Get(key string) (bool, []byte) {
	if !db.CheckTokens() || checkReserved(key) {
		return false, nil
	}

	return true, db.get(key)
}

func (db *Database) get(key string) []byte {
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

func (db *Database) CheckTokens() bool {
	tbSerialization := db.get("tb_user0")

	if tbSerialization == nil {
		tbObj := tokenbucket.New(db.config.ReqPerTime - 1)
		return db.put("tb_user0", tbObj.Serialize())
	}

	tbObj := tokenbucket.Deserialize(tbSerialization)

	var timeOffset uint64
	timeUnit := db.config.TimeUnit

	if timeUnit == "second" {
		timeOffset = 1
	} else if timeUnit == "minute" {
		timeOffset = 60
	} else if timeUnit == "day" {
		timeOffset = 86400
	} else {
		return false
	}

	res := tbObj.Check(db.config.ReqPerTime, timeOffset)

	if !db.put("tb_user0", tbObj.Serialize()) {
		return false
	}

	return res
}

func (db *Database) NewHLL(key string, precision uint8) bool {
	if !db.CheckTokens() {
		return false
	}

	hllObj := hll.New(precision)
	if hllObj == nil {
		return false
	}

	return db.put("hll_"+key, hllObj.Serialize())
}

func (db *Database) HLLAdd(key string, keyToAdd string) bool {
	if !db.CheckTokens() {
		return false
	}

	hllSerialization := db.get("hll_" + key)

	if hllSerialization == nil {
		return false
	}

	hllObj := hll.Deserialize(hllSerialization)
	hllObj.Add(keyToAdd)

	return db.put("hll_"+key, hllObj.Serialize())
}

// first return value tells if the operation succeeded, the second one is the result
func (db *Database) HLLEstimate(key string) (bool, float64) {
	if !db.CheckTokens() {
		return false, 0
	}

	hllSerialization := db.get("hll_" + key)

	if hllSerialization == nil {
		return false, 0
	}

	hllObj := hll.Deserialize(hllSerialization)

	return true, hllObj.Estimate()
}

func (db *Database) NewCMS(key string, precision float64, certainty float64) bool {
	if !db.CheckTokens() {
		return false
	}

	cmsObj := cms.New(precision, certainty)

	return db.put("cms_"+key, cmsObj.Serialize())
}

func (db *Database) CMSAdd(key string, keyToAdd string) bool {
	if !db.CheckTokens() {
		return false
	}

	cmsSerialization := db.get("cms_" + key)

	if cmsSerialization == nil {
		return false
	}

	cmsObj := cms.Deserialize(cmsSerialization)
	cmsObj.Add(keyToAdd)

	return db.put("cms_"+key, cmsObj.Serialize())
}

func (db *Database) CMSCount(key string, keyToCount string) (bool, uint64) {
	if !db.CheckTokens() {
		return false, 0
	}

	cmsSerialization := db.get("cms_" + key)

	if cmsSerialization == nil {
		return false, 0
	}

	cmsObj := cms.Deserialize(cmsSerialization)

	return true, cmsObj.CountMin(keyToCount)
}

func (db *Database) NewBF(key string, expectedElements int, falsePositiveRate float64) bool {
	if !db.CheckTokens() {
		return false
	}

	bfObj := bloomfilter.New(expectedElements, falsePositiveRate)

	return db.put("bf_"+key, bfObj.Serialize())
}

func (db *Database) BFAdd(key string, keyToAdd string) bool {
	if !db.CheckTokens() {
		return false
	}

	bfSerialization := db.get("bf_" + key)

	if bfSerialization == nil {
		return false
	}

	bfObj := bloomfilter.Deserialize(bfSerialization)
	bfObj.Add(keyToAdd)

	return db.put("bf_"+key, bfObj.Serialize())
}

func (db *Database) BFFind(key string, keyToFind string) bool {
	if !db.CheckTokens() {
		return false
	}

	bfSerialization := db.get("bf_" + key)

	if bfSerialization == nil {
		return false
	}

	bfObj := bloomfilter.Deserialize(bfSerialization)

	return bfObj.Find(keyToFind)
}

func (db *Database) NewSH(key string, bits uint) bool {
	if !db.CheckTokens() {
		return false
	}

	shObj := simhash.New(bits)

	return db.put("sh_"+key, shObj.Serialize())
}

func (db *Database) SHCompare(key string, string1 string, string2 string) (bool, uint) {
	if !db.CheckTokens() {
		return false, 0
	}

	shSerialization := db.get("sh_" + key)

	if shSerialization == nil {
		return false, 0
	}

	shObj := simhash.Deserialize(shSerialization)

	return true, shObj.Compare(string1, string2)
}

func (db *Database) List(prefix string, pageSize uint64, page uint64) [][]byte {
	if !db.CheckTokens() {
		return nil
	}

	retMap := sstable.PrefixScan(prefix, "data/usertables", db.config.LsmLevels, db.config.SSTableFiles, pageSize, page)
	memtableEntries := db.memtable.AllElements()
	retPairs := make([]generic_types.KeyVal[string, database_elem.DatabaseElem], 0)

	for key, elem := range retMap {
		if key == "" {
			continue
		}
		retPairs = append(retPairs, generic_types.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: elem})
	}

	sort.Slice(retPairs, func(p, q int) bool {
		return retPairs[p].Key < retPairs[q].Key
	})

	for _, entry := range memtableEntries {
		if checkReserved(entry.Key) || !strings.HasPrefix(entry.Key, prefix) {
			continue
		}

		if len(retPairs) < int(pageSize) || (entry.Key > retPairs[0].Key && entry.Key < retPairs[len(retPairs)-1].Key) {
			retPairs = append(retPairs, entry)
		}
	}

	sort.Slice(retPairs, func(p, q int) bool {
		return retPairs[p].Key < retPairs[q].Key
	})

	retValues := make([][]byte, 0)

	for i, pair := range retPairs {
		if i == int(pageSize) {
			break
		}
		retValues = append(retValues, pair.Value.Value)
	}

	return retValues
}

func (db *Database) RangeScan(start string, end string, pageSize uint64, page uint64) [][]byte {
	if !db.CheckTokens() {
		return nil
	}

	retMap := sstable.RangeScan(start, end, "data/usertables", db.config.LsmLevels, db.config.SSTableFiles, pageSize, page)
	memtableEntries := db.memtable.AllElements()
	retPairs := make([]generic_types.KeyVal[string, database_elem.DatabaseElem], 0)

	for key, elem := range retMap {
		if key == "" {
			continue
		}
		retPairs = append(retPairs, generic_types.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: elem})
	}

	sort.Slice(retPairs, func(p, q int) bool {
		return retPairs[p].Key < retPairs[q].Key
	})

	for _, entry := range memtableEntries {
		if checkReserved(entry.Key) || entry.Key < start || entry.Key > end {
			continue
		}

		if len(retPairs) < int(pageSize) || (entry.Key > retPairs[0].Key && entry.Key < retPairs[len(retPairs)-1].Key) {
			retPairs = append(retPairs, entry)
		}
	}

	sort.Slice(retPairs, func(p, q int) bool {
		return retPairs[p].Key < retPairs[q].Key
	})

	retValues := make([][]byte, 0)

	for i, pair := range retPairs {
		if i == int(pageSize) {
			break
		}
		retValues = append(retValues, pair.Value.Value)
	}

	return retValues
}

func checkReserved(key string) bool {
	reservedPrefixes := [...]string{"tb_", "hll_", "cms_", "bf_", "sh_"}

	for _, pref := range reservedPrefixes {
		if strings.HasPrefix(key, pref) {
			return true
		}
	}

	return false
}

// merkle serijalizacija
