package main

import (
	"fmt"
	"nosql-engine/packages/utils/database"
)

func Menu() {
	fmt.Println("")
	fmt.Println("MENU")
	fmt.Println("==========================")
	fmt.Println("1 - PUT")
	fmt.Println("2 - DELETE")
	fmt.Println("3 - GET")
	fmt.Println("")
	fmt.Println("4 - NEW HYPER LOG LOG")
	fmt.Println("5 - NEW COUNT MIN SKETCH")
	fmt.Println("6 - NEW BLOOM FILTER")
	fmt.Println("7 - NEW SIM HASH")
	fmt.Println("")
	fmt.Println("8 - EXIT")
	fmt.Println("")
}

func HLLMenu() {
	fmt.Println("")
	fmt.Println("HLL MENU")
	fmt.Println("==========================")
	fmt.Println("1 - ADD")
	fmt.Println("2 - ESTIMATE")
	fmt.Println("")
	fmt.Println("8 - EXIT")
	fmt.Println("")
}

func GetOp(max int) int {
	for {
		fmt.Print("Choose an option: ")

		var op int = 0
		_, err := fmt.Scanf("%d", &op)
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
	fmt.Scanf("%s", &key)
	return key
}

func GetValue() []byte {
	value := make([]byte, 0)
	return value
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
		fmt.Println("DELETE failed for key " + key)
	} else {
		fmt.Println("OK")
	}
	return value
}

func NewHLLOperation(db *database.Database) {
	key := GetKey()
	var precision uint8
	fmt.Print("Precision: ")
	fmt.Scan("%d", precision)

	ok := db.NewHLL(key, precision)
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
	}
	return res
}

func main() {
	db := database.New()
	br := false
	for {
		if br {
			break
		}
		Menu()
		op := GetOp(8)

		switch op {
		case 1:
			PutOperation(db)
		case 2:
			DeleteOperation(db)
		case 3:
			_ = GetOperation(db)
		case 4:
		case 8:
			br = true
		}
	}

	fmt.Println("BYE BYE")
}
