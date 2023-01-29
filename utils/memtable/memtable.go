package memtable

import (
	"fmt"
	btree "nosql-engine/packages/utils/b-tree"
	"nosql-engine/packages/utils/database"
	skiplist "nosql-engine/packages/utils/skip-list"
	"time"
)

type MemTable struct {
	structType  string
	maxCapacity int
	capacity    int
	tree        *btree.BTree[string, database.DatabaseElem]
	list        *skiplist.SkipList
}

// type MemTableElem struct {
// 	tombstone byte
// 	value     []byte
// 	timestamp uint64
// }

func New(capacity int, structType string) *MemTable {
	if structType == "btree" {
		return &MemTable{
			structType:  structType,
			maxCapacity: capacity,
			capacity:    0,
			tree:        btree.Init[string, database.DatabaseElem](2, 4),
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

func (mt *MemTable) insertSkipList(key string, elem database.DatabaseElem) {
	res := mt.list.Add(key, elem)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) insertBTree(key string, elem database.DatabaseElem) {
	res := mt.tree.Set(key, elem)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) Insert(key string, elem database.DatabaseElem) {
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
		elem.Value.Tombstone = 1
		mt.tree.Set(elem.Key, elem.Value)
	} else {
		deletedElem := &database.DatabaseElem{Tombstone: 1, Value: []byte(""), Timestamp: uint64(time.Now().Unix())}
		mt.tree.Set(key, *deletedElem)
		mt.capacity++
	}
}

func (mt *MemTable) deleteSkipList(key string) {
	res := mt.list.Remove(key)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) Delete(key string) {
	if mt.structType == "btree" {
		mt.deleteBTree(key)
	} else if mt.structType == "skiplist" {
		mt.deleteSkipList(key)
	}
}

func (mt *MemTable) Find(key string) (value []byte) {
	// TODO

	return nil
}

func (mt *MemTable) Flush() {
	if mt.structType == "btree" {
		fmt.Println(mt.tree.SortedSlice())
		mt.tree = btree.Init[string, database.DatabaseElem](2, 4)
	}
	if mt.structType == "skiplist" {
		fmt.Println(mt.list.Flush())
		mt.list = skiplist.New(32)
	}

	mt.capacity = 0
}
