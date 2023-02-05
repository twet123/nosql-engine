package btree

import (
	"fmt"
	"testing"

	"math/rand"
)

func TestBTreeOnRandomSample(t *testing.T) {
	rand.Seed(42)
	a := map[int32]int32{}
	tree := Init[int32, int32](2, 4)

	for i := 0; i < 10000; i++ {
		operation := rand.Intn(3)
		key := int32(rand.Intn(100) + 1)
		value := int32(rand.Intn(100) + 1)
		switch operation {
		case 0:
			fmt.Println("Add", key)
			if key == 14 && len(tree.root.keyValues) > 0 && tree.root.keyValues[0].Key == 23 {
				print(value)
			}
			tree.Set(key, value)
			a[key] = value
			//fmt.Println(tree)
		case 1:
			for i := 0; i < 10; i++ {
				key := int32(rand.Intn(100) + 1)
				fmt.Println("Gde", key)
				v := a[key]
				found, kv := tree.Get(key)
				if v == 0 && found {
					fmt.Println("Odd kv", kv)
					t.Fatalf("Found non existing key")
				}
				if v != 0 && kv.Value != v {
					fmt.Println("Expected:", a[key], "Got:", kv)
					t.Fatalf("Found wrong key")
				}
			}
		case 2:
			fmt.Println("Del", key)
			delete(a, key)
			tree.Remove(key)
			//fmt.Println(tree)
		}
	}

	fmt.Println(tree)
	fmt.Println(tree.SortedSlice())
}

func TestBTreeOperationByOperation(t *testing.T) {
	tree := Init[int32, int32](3, 5)

	fmt.Println(tree)

	fmt.Println("Add 5, 10, 15, 20, 25")
	tree.Set(5, 1)
	tree.Set(10, 1)
	tree.Set(15, 1)
	tree.Set(20, 1)
	tree.Set(25, 1)
	fmt.Println(tree)

	fmt.Println("Add 13. Root overflow")
	tree.Set(13, 2)
	fmt.Println(tree)

	fmt.Println("Add 2,3,4. Right rotation")
	tree.Set(2, 3)
	tree.Set(3, 3)
	tree.Set(4, 3)
	fmt.Println(tree)

	fmt.Println("Remove 2")
	tree.Remove(2)
	fmt.Println(tree)

	fmt.Println(" Add 30,35,40. Left rotation")
	tree.Set(30, 4)
	tree.Set(35, 4)
	tree.Set(40, 4)
	fmt.Println(tree)

	fmt.Println(" Add 30,35,40. Left rotation")
	tree.Set(30, 5)
	tree.Set(35, 5)
	tree.Set(40, 5)
	fmt.Println(tree)

	fmt.Println(" Add 45. Non root overflow")
	tree.Set(45, 6)
	fmt.Println(tree)

	fmt.Println(" Add 9, 8, 7. Non root overflow")
	tree.Set(9, 7)
	tree.Set(8, 7)
	tree.Set(7, 7)
	fmt.Println(tree)

	fmt.Println(" Remove 35. Delegate to the child")
	tree.Remove(35)
	fmt.Println(tree)

	fmt.Println(" Remove 10. Delegate to the child")
	tree.Remove(10)
	fmt.Println(tree)
}

// var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// func randSeq(n int) string {
// 	b := make([]rune, n)
// 	for i := range b {
// 		b[i] = letters[rand.Intn(len(letters))]
// 	}
// 	return string(b)
// }

// func TestBTree(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())
// 	elementsCnt := 100

// 	btree := Init[string, databaseelem.DatabaseElem](3, 5)
// 	// randomStr := make([]string, elementsCnt)

// 	for i := 0; i < elementsCnt; i++ {
// 		// randomStr[i] = randSeq(10)
// 		newElem := &databaseelem.DatabaseElem{Value: []byte("key" + strconv.Itoa(i)), Tombstone: 0, Timestamp: uint64(time.Now().Unix())}
// 		btree.Set("key"+strconv.Itoa(i), *newElem)
// 	}

// 	for i := 0; i < elementsCnt; i++ {
// 		found, _ := btree.Get("key" + strconv.Itoa(i))
// 		if !found {
// 			t.Fatalf("BTree failed for key " + "key" + strconv.Itoa(i))
// 		}
// 	}
// }
