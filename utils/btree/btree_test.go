package btree

import (
	"fmt"
	databaseelem "nosql-engine/packages/utils/database-elem"
	"strconv"
	"testing"
)

func TestBtree(t *testing.T) {
	bt := Init(2, 4)
	tmp := 100
	for i := 0; i < tmp; i++ {
		key := "key" + strconv.Itoa(i)
		val := databaseelem.DatabaseElem{}
		bt.Set(key, val)
	}
	for i := 0; i < tmp; i++ {
		key := "key" + strconv.Itoa(i)
		found, _ := bt.Get(key)
		if !found {
			t.Fatalf("ne valja za kluc " + key)
		}
	}
	sl := bt.SortedSlice()
	fmt.Println(sl)
}
