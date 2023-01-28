package memtable

import (
	"fmt"
	"strconv"
	"testing"
)

func TestMemTable_Create(t *testing.T) {
	conf := readConfig("")
	mt := CreateMemTable[string, []byte](conf.MemtableSize, conf.MemtableStructure)
	mt.Flush()
}

func TestMemTable_Insert(t *testing.T) {
	conf := readConfig("")
	mt := CreateMemTable[string, []byte](conf.MemtableSize, conf.MemtableStructure)
	for i := 0; i < 120; i++ {
		elem := MemTableElem[string, []byte]{key: strconv.FormatInt(int64(i), 10), tombstone: 1, timestamp: 25, value: []byte(strconv.Itoa(i))}
		mt.Insert(elem)
		if i != 6 {
			mt.Delete(elem)
		}
		if mt.capacity >= mt.maxCapacity {
			mt.Flush()
			fmt.Println("----------------------")
		}
	}
}
