package cache

import (
	"fmt"
	databaseelem "nosql-engine/packages/utils/database-elem"
	"strconv"
	"testing"
	"time"
)

func SlicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestCache(t *testing.T) {
	cache := New(5)
	emptyDBelem := databaseelem.DatabaseElem{
		Value:     []byte(""),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	}

	cache.Refer("5", databaseelem.DatabaseElem{
		Value:     []byte("balsa"),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	})
	cache.Refer("6", databaseelem.DatabaseElem{
		Value:     []byte("teodor"),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	})
	cache.Refer("7", databaseelem.DatabaseElem{
		Value:     []byte("vlada"),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	})
	cache.Refer("8", databaseelem.DatabaseElem{
		Value:     []byte("danilo"),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	})

	if !SlicesEqual(cache.Refer("7", emptyDBelem).Value, []byte("vlada")) {
		t.Fatalf("Cache doesn't contain good value for key 7")
	}

	cache.Refer("9", databaseelem.DatabaseElem{
		Value:     []byte("sasa"),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	})

	if cache.Contains("15") {
		t.Fatalf("Failed, key 15 should not exists")
	}

	// res := cache.Refer(15, nil)
	// fmt.Println(res)

	for i := 5; i < 10; i++ {
		if !cache.Contains(fmt.Sprint(i)) {
			t.Fatalf("Cache failed for key " + strconv.Itoa(i))
		}
	}

	cache.Delete("9")

	if cache.Refer("9", databaseelem.DatabaseElem{
		Value:     []byte("sasa"),
		Tombstone: 0,
		Timestamp: uint64(time.Now().Unix()),
	}).Tombstone != 1 {
		t.Fatalf("Cache deleting failed")
	}
}
