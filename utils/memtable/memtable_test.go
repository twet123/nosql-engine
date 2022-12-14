package main

import (
	"fmt"
	"strconv"
	"testing"
)

func TestMemTable_Create(t *testing.T) {
	conf := readConfig("")
	mt := createMemTable(conf.MemtableSize, conf.MemtableStructure)
	mt.Flush()
}

func TestMemTable_Insert(t *testing.T) {
	conf := readConfig("")
	mt := createMemTable(conf.MemtableSize, conf.MemtableStructure)
	for i := 0; i < 120; i++ {
		elem := MemTableElem{key: strconv.FormatInt(int64(i), 10), tombstone: 1, timestamp: 25, value: nil}
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
