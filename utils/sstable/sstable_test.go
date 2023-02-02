package sstable

import (
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	"os"

	"strconv"
	"testing"
	"time"
)

var mode string = "one"

func TestSStable(t *testing.T) {
	count := 3
	dbelems := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
	for i := 0; i < 10; i++ {
		ts := time.Now().Unix()
		key := "key" + strconv.Itoa(i)
		val := database_elem.DatabaseElem{Tombstone: 0, Value: []byte("nesto" + strconv.Itoa(i)), Timestamp: uint64(ts)}
		dbelems = append(dbelems, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: val})
	}

	CreateSStable(dbelems, count, "data/testTables", 0, mode)
}

func TestFindKey(t *testing.T) {
	prefix := "data/testTables"
	found, dbel := Find("key0", prefix, 1, mode)
	if !found || dbel == nil {
		t.Fatalf("find not working")
	}
	found, _ = Find("key10", prefix, 1, mode)
	if found {
		t.Fatalf("find not working")
	}
	found, dbel = Find("key7", prefix, 1, mode)
	if !found || dbel == nil {
		t.Fatalf("find not working")
	}

	//os.RemoveAll("data/")
}

func TestPrefixSearch(t *testing.T) {
	prefix := "data/testTables"
	pmap := PrefixScan("key", prefix, uint64(1), mode)
	for i := 0; i < 10; i++ {
		_, ok := pmap["key"+strconv.Itoa(i)]
		if !ok {
			t.Fatalf("Prefix scan failed for key" + strconv.Itoa(i))
		}
	}
	os.RemoveAll("data/")
}
