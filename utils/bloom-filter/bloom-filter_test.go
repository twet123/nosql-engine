package bloomfilter

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

func TestBloomFilter(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	elementsCnt := 100
	bloomFilter := New(elementsCnt, 0.01)
	randomStr := make([]string, elementsCnt)

	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		bloomFilter.Add(randomStr[i])
	}

	for i := 0; i < elementsCnt; i++ {
		if !bloomFilter.Find(randomStr[i]) {
			t.Fatalf("Bloom filter failed for key " + randomStr[i])
		}
	}

	path := "../../data/filter/"
	filename := "testFilter.bin"

	bloomFilter.MakeFile(path, filename, "many")
	bloomFilter = NewFromFile(path+filename, 0)

	for i := 0; i < elementsCnt; i++ {
		if !bloomFilter.Find(randomStr[i]) {
			t.Fatalf("Bloom filter failed for key " + randomStr[i] + " after deserialization")
		}
	}

	os.RemoveAll(path)

	serialization := bloomFilter.Serialize()
	bloomFilter = Deserialize(serialization)

	for i := 0; i < elementsCnt; i++ {
		if !bloomFilter.Find(randomStr[i]) {
			t.Fatalf("Bloom filter failed for key " + randomStr[i] + " after byte deserialization")
		}
	}
}
