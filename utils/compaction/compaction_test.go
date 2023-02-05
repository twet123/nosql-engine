package compaction

import (
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	SSTable "nosql-engine/packages/utils/sstable"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

var mode string = "one"
var keyNum int = 100
var count int = 3

func createElements1() []GTypes.KeyVal[string, database_elem.DatabaseElem] {
	dbelems := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
	keys := make([]string, 0)
	for i := 0; i < keyNum; i++ {
		key := "key A" + strconv.Itoa(i)
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i := 0; i < keyNum; i++ {
		ts := time.Now().Unix()
		key := keys[i]
		val := database_elem.DatabaseElem{Tombstone: 0, Value: []byte("nesto" + strconv.Itoa(i)), Timestamp: uint64(ts)}
		dbelems = append(dbelems, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: val})
	}
	return dbelems
}

func TestMergeCompaction(t *testing.T) {
	dbelems := createElements1()

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, err := os.Open("data/testTables/usertable-L0-1-Data.db")
	if err != nil {
		t.Fatal(err)
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ = os.Open("data/testTables/usertable-L0-1-Data.db")
	_ = file.Close()

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ = os.Open("data/testTables/usertable-L0-1-Data.db")
	_ = file.Close()

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ = os.Open("data/testTables/usertable-L0-1-Data.db")
	_ = file.Close()

	MergeCompaction(0, "data/testTables")
}

func TestLeveledCompaction(t *testing.T) {
	dbelems := createElements1()

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, err := os.Open("data/testTables/usertable-L0-1-Data.db")
	if err != nil {
		t.Fatal(err)
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ = os.Open("data/testTables/usertable-L0-1-Data.db")
	_ = file.Close()

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ = os.Open("data/testTables/usertable-L0-1-Data.db")
	_ = file.Close()

	SSTable.CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ = os.Open("data/testTables/usertable-L0-1-Data.db")
	_ = file.Close()

	LeveledCompaction(0, "data/testTables")
}
