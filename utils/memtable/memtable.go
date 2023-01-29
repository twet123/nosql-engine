package memtable

import (
	"fmt"
	btree "nosql-engine/packages/utils/b-tree"
	skiplist "nosql-engine/packages/utils/skip-list"
	"time"
)

type MemTable struct {
	structType  string
	maxCapacity int
	capacity    int
	tree        *btree.BTree[string, MemTableElem]
	list        *skiplist.SkipList
}

type MemTableElem struct {
	tombstone byte
	value     []byte
	timestamp uint64
}

func New(capacity int, structType string) *MemTable {
	if structType == "btree" {
		return &MemTable{
			structType:  structType,
			maxCapacity: capacity,
			capacity:    0,
			tree:        btree.Init[string, MemTableElem](2, 4),
			list:        nil,
		}
	} else if structType == "skiplist" {
		return &MemTable{
			structType:  structType,
			maxCapacity: capacity,
			capacity:    0,
			tree:        nil,
			list:        skiplist.New(32),
		}
	} else {
		panic("Invalid structType!")
	}
}

func (mt *MemTable) insertSkipList(key string, elem MemTableElem) {
	res := mt.list.Add(key, elem.value)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) insertBTree(key string, elem MemTableElem) {
	res := mt.tree.Set(key, elem)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) Insert(key string, elem MemTableElem) {
	if mt.structType == "btree" {
		mt.insertBTree(key, elem)
	}
	if mt.structType == "skiplist" {
		mt.insertSkipList(key, elem)
	}
}

func (mt *MemTable) deleteBTree(key string) {
	found, elem := mt.tree.Get(key)

	if found {
		elem.Value.tombstone = 1
		mt.tree.Set(elem.Key, elem.Value)
	} else {
		deletedElem := &MemTableElem{tombstone: 1, value: []byte(""), timestamp: uint64(time.Now().Unix())}
		mt.tree.Set(key, *deletedElem)
	}
}

func (mt *MemTable) deleteSkipList(key string) {
	// dodati remove za listu
}

func (mt *MemTable) Delete(key string) {
	if mt.structType == "btree" {
		mt.deleteBTree(key)
	} else if mt.structType == "skiplist" {
		mt.deleteSkipList(key)
	}
}

func (mt *MemTable) Flush() {
	if mt.structType == "btree" {
		fmt.Println(mt.tree.SortedSlice())
		mt.tree = btree.Init[string, MemTableElem](2, 4)
	}
	if mt.structType == "skiplist" {
		mt.list.PrintLevels()
		mt.list = skiplist.New(32)
	}

	mt.capacity = 0
}
