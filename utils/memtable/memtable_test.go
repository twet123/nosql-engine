package memtable

import (
	"fmt"
	"math/rand"
	"nosql-engine/packages/utils/database"
	"testing"
	"time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestMemTable(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	elementsCnt := 100
	capacity := 40

	memtableTree := New(capacity, "btree")
	memtableList := New(capacity, "skiplist")

	randomStr := make([]string, elementsCnt)
	for i := 0; i < elementsCnt; i++ {
		// testing for flush
		if i != 0 && i%capacity == 0 {
			if memtableList.capacity != 0 || memtableTree.capacity != 0 {
				t.Fatalf("Flush didn't happen!")
			}
		}

		randomStr[i] = randSeq(10)
		memtableTree.Insert(randomStr[i], database.DatabaseElem{
			Value:     []byte(randomStr[i]),
			Tombstone: 0,
			Timestamp: uint64(time.Now().Unix()),
		})
		memtableList.Insert(randomStr[i], database.DatabaseElem{
			Value:     []byte(randomStr[i]),
			Tombstone: 0,
			Timestamp: uint64(time.Now().Unix()),
		})
	}

	// test finding
	for i := 0; i < elementsCnt; i++ {
		found, _ := memtableList.Find(randomStr[i])

		if found && i < elementsCnt-(elementsCnt%capacity)-1 {
			t.Fatalf("MemtableTree find failed for key " + randomStr[i])
		}

		found, _ = memtableTree.Find(randomStr[i])

		if found && i < elementsCnt-(elementsCnt%capacity)-1 {
			t.Fatalf("MemtableList find failed for key " + randomStr[i])
		}
	}

	// test deleting
	for i := elementsCnt - (elementsCnt % capacity) - 1; i < elementsCnt; i++ {
		memtableList.Delete(randomStr[i])
		memtableTree.Delete(randomStr[i])

		found, keyval := memtableList.Find(randomStr[i])

		if !found || keyval.Value.Tombstone != 1 {
			t.Fatalf("MemtableList delete failed for key " + randomStr[i])
		}

		found, keyval = memtableTree.Find(randomStr[i])

		if !found || keyval.Value.Tombstone != 1 {
			t.Fatalf("MemtableTree delete failed for key " + randomStr[i])
		}
	}

	capacityBefore := memtableList.capacity
	for i := 0; i < memtableList.maxCapacity-capacityBefore; i++ {
		memtableList.Delete(randomStr[i])
	}

	if memtableList.capacity != 0 {
		t.Fatalf("MemtableList delete failed! " + fmt.Sprint(memtableList.capacity))
	}

	capacityBefore = memtableTree.capacity
	for i := 0; i < memtableTree.maxCapacity-capacityBefore; i++ {
		memtableTree.Delete(randomStr[i])
	}

	if memtableTree.capacity != 0 {
		t.Fatalf("MemtableTree delete failed! " + fmt.Sprint(memtableTree.capacity))
	}
}
