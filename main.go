package main

import (
	"fmt"
	"nosql-engine/packages/utils/database"
	"strings"
)

func IsSpecialKey(key string) bool {
	specials := make([]string, 5)
	specials[0] = "bf_"
	specials[1] = "cms_"
	specials[2] = "tb_"
	specials[3] = "hll_"
	specials[4] = "sh_"

	for _, spec := range specials {
		if strings.HasPrefix(key, spec) {
			return true
		}
	}
	return false
}

func Menu() {
	fmt.Println("")
	fmt.Println("MENU")
	fmt.Println("==========================")
	fmt.Println("1 - PUT")
	fmt.Println("2 - DELETE")
	fmt.Println("3 - GET")
	fmt.Println("")
	fmt.Println("4 - NEW HYPER LOG LOG")
	fmt.Println("5 - HLL ADD")
	fmt.Println("6 - HLL ESTIMATE")
	fmt.Println("")

	fmt.Println("7 - NEW COUNT MIN SKETCH")
	fmt.Println("8 - CMS ADD")
	fmt.Println("9 - CMS COUNT")
	fmt.Println("")

	fmt.Println("10 - NEW BLOOM FILTER")
	fmt.Println("11 - BF ADD")
	fmt.Println("12 - BF FIND")
	fmt.Println("")
	fmt.Println("13 - NEW SIM HASH")
	fmt.Println("14 - SH COMPARE")
	fmt.Println("")
	fmt.Println("15 - RANGE SCAN")
	fmt.Println("16 - LIST(PREFIX) SCAN")
	fmt.Println("")

	fmt.Println("17 - EXIT")
	fmt.Println("")
}

func GetOp(max int) int {
	for {
		fmt.Print("Choose an option: ")
		var op int = 0
		_, err := fmt.Scanf("%d", &op)
		fmt.Scanln()
		if err != nil {
			fmt.Println(err)
			continue
		}
		if op < 1 || op > max {
			fmt.Println("Please insert again")
			continue
		}
		return op
	}
}

func GetKey() string {
	var key string

	fmt.Print("Key: ")
	fmt.Scanln(&key)
	return key
}

func GetValue() []byte {
	value := make([]byte, 0)
	return value
}

func GetUint64() uint64 {
	for {
		var precision uint64
		_, err := fmt.Scanf("%d", precision)
		fmt.Scanln()
		if err != nil {
			fmt.Println("Please insert correct value")
		}
		return precision
	}
}

func GetFloat64() float64 {
	for {
		var precision float64

		_, err := fmt.Scanf("%f", precision)
		fmt.Scanln()
		if err != nil {
			fmt.Println("Please insert correct value")
		}
		return precision
	}
}

func PutOperation(db *database.Database) {
	key := GetKey()
	value := GetValue()

	ok := db.Put(key, value)
	if !ok {
		fmt.Println("PUT failed for key " + key)
	} else {
		fmt.Println("OK")
	}
}

func DeleteOperation(db *database.Database) {
	key := GetKey()

	ok := db.Delete(key)
	if !ok {
		fmt.Println("DELETE failed for key " + key)
	} else {
		fmt.Println("OK")
	}
}

func GetOperation(db *database.Database) []byte {
	key := GetKey()

	ok, value := db.Get(key)
	if !ok {
		fmt.Println("GET failed for key " + key)
	} else {
		fmt.Println("OK")
		fmt.Print("Value = ")
		fmt.Println(value)
	}
	return value
}

func NewHLLOperation(db *database.Database) {
	key := GetKey()
	fmt.Print("Precision: ")
	precision := GetUint64()

	ok := db.NewHLL(key, uint8(precision))
	if !ok {
		fmt.Println("NEW HLL failed for key " + key)
	} else {
		fmt.Println("OK")
	}
}

func HLLAddOperation(db *database.Database) {
	key := GetKey()
	var keyToAdd string

	fmt.Print("Key to add: ")
	fmt.Scanf("%s", &key)
	fmt.Scanln()

	ok := db.HLLAdd(key, keyToAdd)
	if !ok {
		fmt.Println("HLL ADD failed for key " + key + " and key to add " + keyToAdd)
	} else {
		fmt.Println("OK")
	}
}

func HLLEstimateOperation(db *database.Database) float64 {
	key := GetKey()

	ok, res := db.HLLEstimate(key)
	if !ok {
		fmt.Println("HLL ESTIMATE failed for key " + key)
	} else {
		fmt.Println("OK")
		fmt.Print("Estimate = ")
		fmt.Println(res)
	}
	return res
}

func NewCMSOperation(db *database.Database) {
	key := GetKey()

	fmt.Print("Precision: ")
	precision := GetFloat64()

	fmt.Print("Certainty: ")
	certainty := GetFloat64()

	ok := db.NewCMS(key, precision, certainty)
	if !ok {
		fmt.Println("NEW CMS failed for key " + key)
	} else {
		fmt.Println("OK")
	}
}

func CMSAddOperation(db *database.Database) {
	key := GetKey()
	var keyToAdd string

	fmt.Print("Key to add: ")
	fmt.Scanf("%s", &key)
	fmt.Scanln()

	ok := db.CMSAdd(key, keyToAdd)
	if !ok {
		fmt.Println("CMS ADD failed for key " + key + " and key to add " + keyToAdd)
	} else {
		fmt.Println("OK")
	}
}

func CMSCountOperation(db *database.Database) uint64 {
	key := GetKey()
	var keyToCount string

	fmt.Print("Key to count: ")
	fmt.Scanf("%s", &key)
	fmt.Scanln()

	ok, res := db.CMSCount(key, keyToCount)
	if !ok {
		fmt.Println("CMS COUNT failed for key " + key + " and key to count " + keyToCount)
	} else {
		fmt.Println("OK")
		fmt.Print("Count = ")
		fmt.Println(res)
	}
	return res
}

func NewBFOperation(db *database.Database) {
	key := GetKey()
	fmt.Print("Expected number of elements: ")
	expel := GetUint64()

	fprate := GetFloat64()

	ok := db.NewBF(key, int(expel), fprate)
	if !ok {
		fmt.Println("NEW BF failed for key " + key)
	} else {
		fmt.Println("OK")
	}
}

func BFAddOperation(db *database.Database) {
	key := GetKey()
	var keyToAdd string

	fmt.Print("Key to add: ")
	fmt.Scanf("%s", &key)
	fmt.Scanln()

	ok := db.BFAdd(key, keyToAdd)
	if !ok {
		fmt.Println("BF ADD failed for key " + key + " and key to add " + keyToAdd)
	} else {
		fmt.Println("OK")
	}
}

func BFFindOperation(db *database.Database) {
	key := GetKey()
	var keyToFind string

	fmt.Print("Key to find: ")
	fmt.Scanf("%s", &keyToFind)
	fmt.Scanln()

	ok, res := db.BFFind(key, keyToFind)
	if !ok {
		fmt.Println("BF FIND failed for key " + key)
	} else {
		fmt.Print("Found = ")
		fmt.Println(res)
	}

}

func NewSHOperation(db *database.Database) {
	key := GetKey()
	fmt.Print("Bits: ")
	bits := GetUint64()

	ok := db.NewSH(key, uint(bits))
	if !ok {
		fmt.Println("NEW HLL failed for key " + key)
	} else {
		fmt.Println("OK")
	}
}

func SHCompareOperation(db *database.Database) uint {
	key := GetKey()
	var key1 string
	var key2 string

	fmt.Print("string 1: ")
	fmt.Scanf("%s", &key1)
	fmt.Scanln()

	fmt.Print("string 2: ")
	fmt.Scanf("%s", &key2)
	fmt.Scanln()

	ok, res := db.SHCompare(key, key1, key2)
	if !ok {
		fmt.Println("SH COMPARE failed for key " + key + " and keys to compare [" + key1 + "], [" + key2 + "]")
	} else {
		fmt.Println("OK")
		fmt.Print("Result = ")
		fmt.Println(res)
	}
	return res
}

func ListScanOperation(db *database.Database) {
	var prefix string

	fmt.Print("Prefix: ")
	fmt.Scanf("%s", &prefix)
	fmt.Scanln()
	pageSize, pageNumber := Pagination()
	res := db.List(prefix, pageSize, pageNumber)
	fmt.Print("KV found: ")
	fmt.Println(res)
}

func RangeScanOperation(db *database.Database) {
	var start, stop string

	fmt.Print("Start: ")
	fmt.Scanf("%s", &start)
	fmt.Scanln()

	fmt.Print("Stop: ")
	fmt.Scanf("%s", &stop)
	fmt.Scanln()

	pageSize, pageNumber := Pagination()
	res := db.RangeScan(start, stop, pageSize, pageNumber)
	fmt.Print("KV found: ")
	fmt.Println(res)
}

func Pagination() (uint64, uint64) {
	fmt.Print("Page Size: ")
	ps := GetUint64()
	fmt.Print("Page number (starts with 0): ")
	pn := GetUint64()

	return ps, pn
}

func main() {
	db := database.New()
	db.Put("lala", make([]byte, 0))
	br := false
	for {
		if br {
			break
		}

		Menu()
		op := GetOp(17)

		switch op {
		case 1:
			PutOperation(db)
		case 2:
			DeleteOperation(db)
		case 3:
			_ = GetOperation(db)
		case 4:
			NewHLLOperation(db)
		case 5:
			HLLAddOperation(db)
		case 6:
			_ = HLLEstimateOperation(db)
		case 7:
			NewCMSOperation(db)
		case 8:
			CMSAddOperation(db)
		case 9:
			_ = CMSCountOperation(db)
		case 10:
			NewBFOperation(db)
		case 11:
			BFAddOperation(db)
		case 12:
			BFFindOperation(db)
		case 13:
			NewSHOperation(db)
		case 14:
			SHCompareOperation(db)
		case 15:
			RangeScanOperation(db)
		case 16:
			ListScanOperation(db)
		case 17:
			br = true
		}
	}

	fmt.Println("BYE BYE")
}
