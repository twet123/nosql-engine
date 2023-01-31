package skiplist

import (
	"math/rand"
	database_elem "nosql-engine/packages/utils/database-elem"
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

func TestSkipList(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maxHeight := 32
	elementsCnt := 100

	skipList := New(maxHeight)
	randomStr := make([]string, elementsCnt)

	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		newElem := &database_elem.DatabaseElem{Value: []byte(randomStr[i]), Tombstone: 0, Timestamp: uint64(time.Now().Unix())}
		skipList.Add(randomStr[i], *newElem)
	}

	for i := 0; i < elementsCnt; i++ {
		if skipList.Find(randomStr[i]) == nil {
			t.Fatalf("SkipList failed for key " + randomStr[i])
		}
	}

	skipList.PrintLevels()
	if len(skipList.Flush()) < elementsCnt {
		t.Fatalf("SkipList flush failed")
	}
}
