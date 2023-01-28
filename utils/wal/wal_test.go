package wal

import (
	"fmt"
	"math"
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

func TestWAL(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	elementsCnt := 100
	segmentSize := 20
	path := "../../data/testWal/"

	wal := New(path, uint32(segmentSize), 10)
	randomStr := make([]string, elementsCnt)

	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		wal.PutEntry(randomStr[i], []byte(randomStr[i]), 0)
	}

	for i := 0; i < int(math.Ceil(float64(elementsCnt)/float64(segmentSize))); i++ {
		_, error := os.Stat(path + "log_" + fmt.Sprint(i+1) + ".bin")

		if os.IsNotExist(error) {
			t.Fatalf("File " + path + "log_" + fmt.Sprint(i+1) + ".bin" + " does not exist!")
		}
	}

	os.RemoveAll(path)
}
