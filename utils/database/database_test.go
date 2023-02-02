package database

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
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

	// testing put function
	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		if !db.Put(randomStr[i], []byte(randomStr[i])) {
			t.Fatalf("Database PUT failed for key " + randomStr[i])
		}
	}

	_, err := os.ReadDir("./data/wal")

	if os.IsNotExist(err) {
		t.Fatalf("Database PUT failed!")
	}

	// testing get function
	for i := 0; i < elementsCnt; i++ {
		if !reflect.DeepEqual([]byte(randomStr[i]), db.Get(randomStr[i])) {
			t.Fatalf("Database GET failed for key " + randomStr[i])
		}
	}

	for i := 0; i < elementsCnt; i++ {
		if db.Get(randSeq(11)) != nil {
			t.Fatalf("Database GET failed for non-existent key " + randomStr[i])
		}
	}

	// testing delete
	for i := 0; i < elementsCnt; i++ {
		db.Delete(randomStr[i])
		if db.Get(randomStr[i]) != nil {
			t.Fatalf("Database DELETE failed for key " + randomStr[i])
		}
	}

	// testing db HLL
	if !db.NewHLL("myHLL", 6) {
		t.Fatalf("New HLL failed")
	}

	for i := 0; i < elementsCnt; i++ {
		if !db.HLLAdd("myHLL", randomStr[i]) {
			t.Fatalf("Database HLL add failed")
		}
	}

	succ, hllRes := db.HLLEstimate("myHLL")

	if !succ || hllRes <= 1 {
		t.Fatalf("Database HLL estimate failed %f", hllRes)
	}

	// testing db CMS
	if !db.NewCMS("myCMS", 0.1, 0.01) {
		t.Fatalf("New CMS failed")
	}

	for i := 0; i < elementsCnt; i++ {
		if !db.CMSAdd("myCMS", randomStr[i]) {
			t.Fatalf("Database CMS add failed")
		}
	}

	for i := 0; i < elementsCnt; i++ {
		succ, cmsRes := db.CMSCount("myCMS", randomStr[i])

		if !succ || cmsRes < 1 {
			t.Fatalf("Database CMS counting failed %x", cmsRes)
		}
	}

	// testing db BloomFilter
	if !db.NewBF("myBF", elementsCnt, 0.01) {
		t.Fatalf("New BloomFilter failed")
	}

	for i := 0; i < elementsCnt; i++ {
		if !db.BFAdd("myBF", randomStr[i]) {
			t.Fatalf("Database BloomFilter add failed")
		}
	}

	for i := 0; i < elementsCnt; i++ {
		if !db.BFFind("myBF", randomStr[i]) {
			t.Fatalf("Database BloomFilter find failed")
		}
	}

	// testing db SimHash
	if !db.NewSH("mySH", 16) {
		t.Fatalf("New SimHash failed")
	}

	succ, shRes := db.SHCompare("mySH", strings.Join(randomStr[0:50], " "), strings.Join(randomStr[50:], " "))

	if !succ {
		t.Fatalf("Database SimHash failed, impossible result")
	}

	fmt.Println("Database SimHash result:", shRes)

	os.RemoveAll("./data")
}
