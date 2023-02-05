package compaction

import (
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	SSTable "nosql-engine/packages/utils/sstable"
	"sort"
	"strconv"
	"testing"
	"time"
)

var mode string = "one"
var keyNum int = 10000
var count int = 3

func createElements1(from, to int) []GTypes.KeyVal[string, database_elem.DatabaseElem] {
	dbelems := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
	keys := make([]string, 0)
	for i := from; i < to; i++ {
		key := "key A" + strconv.Itoa(i)
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i := 0; i < to-from; i++ {
		ts := time.Now().Unix()
		key := keys[i]
		val := database_elem.DatabaseElem{Tombstone: 0, Value: []byte("nesto" + strconv.Itoa(i)), Timestamp: uint64(ts)}
		dbelems = append(dbelems, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: val})
	}
	return dbelems
}

func TestLeveledCompaction(t *testing.T) {

	dbelems := createElements1(0, 100)
	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	dbelems = createElements1(50, 150)
	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	dbelems = createElements1(20, 70)
	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	dbelems = createElements1(200, 290)
	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	dbelems = createElements1(200, 400)
	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)

	LeveledCompaction(0, "data/testTables")
}
