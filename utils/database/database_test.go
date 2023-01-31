package database

import (
	"math/rand"
	"os"
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

func TestDatabase(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	elementsCnt := 100

	db := New()
	randomStr := make([]string, elementsCnt)

	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		if !db.Put(randomStr[i], []byte(randomStr[i])) {
			t.Fatalf("Database PUT failed for key " + randomStr[i])
		}
	}

	_, err := os.ReadDir("./data/usertables")

	if os.IsNotExist(err) {
		t.Fatalf("Database PUT failed!")
	}

	_, err = os.ReadDir("./data/wal")

	if os.IsNotExist(err) {
		t.Fatalf("Database PUT failed!")
	}

	os.RemoveAll("./data")
}
