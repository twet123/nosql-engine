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
		if !db.put(randomStr[i], []byte(randomStr[i])) {
			t.Fatalf("Database PUT failed for key " + randomStr[i])
		}
	}

	_, err := os.ReadDir("./data/wal")

	if os.IsNotExist(err) {
		t.Fatalf("Database PUT failed!")
	}

	// testing get function
	for i := 0; i < elementsCnt; i++ {
		if !reflect.DeepEqual([]byte(randomStr[i]), db.get(randomStr[i])) {
			t.Fatalf("Database GET failed for key " + randomStr[i])
		}
	}

	for i := 0; i < elementsCnt; i++ {
		if db.get(randSeq(11)) != nil {
			t.Fatalf("Database GET failed for non-existent key " + randomStr[i])
		}
	}

	// testing delete
	for i := 0; i < elementsCnt; i++ {
		db.delete(randomStr[i])
		if db.get(randomStr[i]) != nil {
			t.Fatalf("Database DELETE failed for key " + randomStr[i])
		}
	}

	// for testing purposes
	// testing DB types, putting requests per minute to 800
	db.config.ReqPerTime = 800

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
		succ, res := db.BFFind("myBF", randomStr[i])
		if !succ || !res {
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

	// testing db list
	// element were deleted previously, so adding them back
	for i := 0; i < elementsCnt; i++ {
		db.Put(randomStr[i], []byte(randomStr[i]))
	}

	listRes := db.List(randomStr[0][0:2], 100, 0)

	fmt.Println("List for", randomStr[0][0:2])
	for _, res := range listRes {
		fmt.Println(string(res))
		if !strings.HasPrefix(string(res), randomStr[0][0:2]) {
			t.Fatal("Database list failed, returned a string without given prefix")
		}
	}

	// testing db range scan
	rangeRes := db.RangeScan(randomStr[0], randomStr[elementsCnt-1], 100, 0)

	fmt.Println("Range for", randomStr[0], "-", randomStr[elementsCnt-1])
	for _, res := range rangeRes {
		fmt.Println(string(res))
		if !(string(res) >= randomStr[0] && string(res) <= randomStr[elementsCnt-1]) {
			t.Fatal("Database range scan failed, returned a string without given prefix")
		}
	}

	// testing for rate limiting
	db.delete("tb_user0")
	db.config.ReqPerTime = 60

	for i := 0; i < elementsCnt; i++ {
		if i <= 59 {
			res, _ := db.Get(randomStr[i])

			if !res {
				t.Fatal("Rate limiting failed!")
			}
		} else {
			res, _ := db.Get(randomStr[i])

			if res {
				t.Fatal("Rate limiting failed")
			}
		}
	}

	// passing but commented for timeout reasons
	// time.Sleep(1 * time.Minute)

	// // token bucket should be refreshed
	// for i := 0; i < elementsCnt; i++ {
	// 	if i <= 59 {
	// 		res, _ := db.Get(randomStr[i])

	// 		if !res {
	// 			t.Fatal("Rate limiting failed!")
	// 		}
	// 	} else {
	// 		res, _ := db.Get(randomStr[i])

	// 		if res {
	// 			t.Fatal("Rate limiting failed")
	// 		}
	// 	}
	// }

	os.RemoveAll("./data")
}

func TestCompactions(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	elementsCnt := 1000

	db := New()
	randomStr := make([]string, elementsCnt)

	for i := 0; i < elementsCnt; i++ {
		randomStr[i] = randSeq(10)
		if !db.put(randomStr[i], []byte(randomStr[i])) {
			t.Fatalf("Database PUT failed for key " + randomStr[i])
		}
	}

	for i := 0; i < elementsCnt; i++ {
		if !reflect.DeepEqual([]byte(randomStr[i]), db.get(randomStr[i])) {
			// fmt.Println("Nije nasao", i, randomStr[i], res)
			t.Fatalf("Database GET failed for key " + randomStr[i])
		}
	}
}

func TestTestCompactions(t *testing.T) {
	for i := 0; i < 4; i++ {
		TestCompactions(t)
	}
}
