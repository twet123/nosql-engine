package sstable

import (
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

var mode string = "one"
var keyNum int = 100

func TestSStable(t *testing.T) {
	count := 3
	dbelems := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
	keys := make([]string, 0)
	for i := 0; i < keyNum; i++ {
		key := "key" + strconv.Itoa(i)
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i := 0; i < keyNum; i++ {
		ts := time.Now().Unix()
		key := keys[i]
		val := database_elem.DatabaseElem{Tombstone: 0, Value: []byte("nesto" + strconv.Itoa(i)), Timestamp: uint64(ts)}
		dbelems = append(dbelems, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: val})
	}

	CreateSStable(dbelems, count, "data/testTables", 0, mode)
	file, _ := os.Open("data/testTables/usertable-L0-1-Data.db")
	ReadRecord(file, 1000)

}

func TestFindKey(t *testing.T) {
	prefix := "data/testTables"
	found, dbel := Find("key0", prefix, 1, mode)
	if !found || dbel == nil {
		t.Fatalf("find not working for key0")
	}
	found, _ = Find("key150", prefix, 1, mode)
	if found {
		t.Fatalf("find not working for key150")
	}
	found, dbel = Find("key7", prefix, 1, mode)
	if !found || dbel == nil {
		t.Fatalf("find not working for key7")
	}

	//os.RemoveAll("data/")
}

func TestPrefixSearch(t *testing.T) {
	prefix := "data/testTables"
	pmap := PrefixScan("key", prefix, uint64(1), mode)
	for i := 0; i < keyNum; i++ {
		_, ok := pmap["key"+strconv.Itoa(i)]
		if !ok {
			t.Fatalf("Prefix scan failed for key" + strconv.Itoa(i))
		}
	}
	//os.RemoveAll("data/")
}

func TestRangeSearch(t *testing.T) {
	prefix := "data/testTables"
	pmap := RangeScan("key0", "key999", prefix, uint64(1), mode)
	for i := 0; i < keyNum; i++ {
		_, ok := pmap["key"+strconv.Itoa(i)]
		if !ok {
			t.Fatalf("Prefix scan failed for key" + strconv.Itoa(i))
		}
	}
	os.RemoveAll("data/")
}
