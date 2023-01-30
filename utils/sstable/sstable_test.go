package sstable

import (
	db "nosql-engine/packages/utils/database"
	GTypes "nosql-engine/packages/utils/generic-types"

	"strconv"
	"testing"
	"time"
)

func TestSStable(t *testing.T) {
	count := 3
	dbelems := make([]GTypes.KeyVal[string, db.DatabaseElem], 0)
	for i := 0; i < 10; i++ {
		ts := time.Now().Unix()
		key := "key" + strconv.Itoa(i)
		val := db.DatabaseElem{Tombstone: 0, Value: []byte("nesto" + strconv.Itoa(i)), Timestamp: uint64(ts)}
		dbelems = append(dbelems, GTypes.KeyVal[string, db.DatabaseElem]{Key: key, Value: val})
	}

	CreateSStable(dbelems, count, "files")
}

func TestFindKey(t *testing.T) {
	prefix := "files/"
	found, dbel := Find("key0", prefix)
	if !found || dbel == nil {
		t.Fatalf("find not working")
	}
	found, dbel = Find("key10", prefix)
	if found {
		t.Fatalf("find not working")
	}
	found, dbel = Find("key7", prefix)
	if !found || dbel == nil {
		t.Fatalf("find not working")
	}
}
