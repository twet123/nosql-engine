package main

import (
	"fmt"
	"math"
	"math/rand"
	"nosql-engine/packages/utils/wal"
	"os"
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

func main() {
	fmt.Println("test")

	rand.Seed(time.Now().UnixNano())
	elementsCnt := 100
	segmentSize := 20
	path := "../../data/wal/"

	wal := wal.New(path, uint32(segmentSize), 10)
	randomStr := make([]string, elementsCnt)

	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		wal.PutEntry(randomStr[i], []byte(randomStr[i]), 0)
	}

	for i := 0; i < int(math.Ceil(float64(elementsCnt)/float64(segmentSize))); i++ {
		_, error := os.Stat(path + "log_" + fmt.Sprint(i) + ".bin")

		if os.IsNotExist(error) {
			fmt.Println("error nema ga")
		}
	}
}
