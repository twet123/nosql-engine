package memtable

import (
	btree "nosql-engine/packages/utils/b-tree"
	database_elem "nosql-engine/packages/utils/database-elem"

	generic_types "nosql-engine/packages/utils/generic-types"
	skiplist "nosql-engine/packages/utils/skip-list"
	"nosql-engine/packages/utils/sstable"
	"time"
)

type MemTable struct {
	structType   string
	maxCapacity  int
	capacity     int
	tree         *btree.BTree[string, database_elem.DatabaseElem]
	list         *skiplist.SkipList
	summaryCount int
	sstableMode  string
}

// type MemTableElem struct {
// 	tombstone byte
// 	value     []byte
// 	timestamp uint64
// }

// max representing max levels for skiplist or max elements in node for btree, min repsresents min elements in node for btree
func New(capacity int, structType string, max uint64, min uint64, summaryCount int, sstableMode string) *MemTable {
	if structType == "btree" {
		return &MemTable{
			structType:   structType,
			maxCapacity:  capacity,
			capacity:     0,
			tree:         btree.Init[string, database_elem.DatabaseElem](int(min), int(max)),
			list:         nil,
			summaryCount: summaryCount,
			sstableMode:  sstableMode,
		}
	} else if structType == "skiplist" {
		return &MemTable{
			structType:   structType,
			maxCapacity:  capacity,
			capacity:     0,
			tree:         nil,
			list:         skiplist.New(int(max)),
			summaryCount: summaryCount,
			sstableMode:  sstableMode,
		}
	} else {
		panic("Invalid structType!")
	}
}

func (mt *MemTable) insertSkipList(key string, elem database_elem.DatabaseElem) {
	res := mt.list.Add(key, elem)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) insertBTree(key string, elem database_elem.DatabaseElem) {
	res := mt.tree.Set(key, elem)

	if res {
		mt.capacity++
	}
}

func (mt *MemTable) Insert(key string, elem database_elem.DatabaseElem) {
	if mt.structType == "btree" {
		mt.insertBTree(key, elem)
	}
	if mt.structType == "skiplist" {
		mt.insertSkipList(key, elem)
	}
	if mt.capacity >= mt.maxCapacity {
		mt.Flush()
	}
}

func (mt *MemTable) deleteBTree(key string) {
	found, elem := mt.tree.Get(key)

	if found {
		elem.Value.Tombstone = 1
		mt.tree.Set(elem.Key, elem.Value)
	} else {
		deletedElem := &database_elem.DatabaseElem{Tombstone: 1, Value: []byte(""), Timestamp: uint64(time.Now().Unix())}
		mt.tree.Set(key, *deletedElem)
		mt.capacity++
	}

	if mt.capacity >= mt.maxCapacity {
		mt.Flush()
	}
}

func (mt *MemTable) deleteSkipList(key string) {
	res := mt.list.Remove(key)

	if res {
		mt.capacity++
	}

	if mt.capacity >= mt.maxCapacity {
		mt.Flush()
	}
}

func (mt *MemTable) Delete(key string) {
	if mt.structType == "btree" {
		mt.deleteBTree(key)
	} else if mt.structType == "skiplist" {
		mt.deleteSkipList(key)
	}
}

func (mt *MemTable) findBTree(key string) (found bool, elem generic_types.KeyVal[string, database_elem.DatabaseElem]) {
	found, keyval := mt.tree.Get(key)

	return found, keyval
}

func (mt *MemTable) findSkipList(key string) (found bool, elem generic_types.KeyVal[string, database_elem.DatabaseElem]) {
	node := mt.list.Find(key)
	elem.Key = key

	if node == nil {
		found = false
		elem.Value = database_elem.DatabaseElem{
			Tombstone: 0,
			Value:     []byte(""),
			Timestamp: 0,
		}
	} else {
		found = true
		elem.Value = *skiplist.NodeToElem(*node)
	}

	return
}

// First element returned is a boolean telling if the element was found, the second is a KeyValue pair
// containing element info. Check if the tombstone is 0 before returning in read path!
func (mt *MemTable) Find(key string) (bool, generic_types.KeyVal[string, database_elem.DatabaseElem]) {
	if mt.structType == "btree" {
		return mt.findBTree(key)
	} else {
		return mt.findSkipList(key)
	}
}

func (mt *MemTable) Flush() {
	if mt.structType == "btree" {
		prevMin := mt.tree.MinElementsCnt
		prevMax := mt.tree.MaxElementsCnt
		sstable.CreateSStable(mt.tree.SortedSlice(), mt.summaryCount, "data/usertables", 0, mt.sstableMode)
		mt.tree = btree.Init[string, database_elem.DatabaseElem](prevMin, prevMax)
	}
	if mt.structType == "skiplist" {
		prevMax := mt.list.MaxHeight
		sstable.CreateSStable(mt.list.Flush(), mt.summaryCount, "data/usertables", 0, mt.sstableMode)
		mt.list = skiplist.New(prevMax)
	}

	mt.capacity = 0
}

func (mt *MemTable) CheckFlushed() bool {
	return mt.capacity == 0
}

func (mt *MemTable) AllElements() []generic_types.KeyVal[string, database_elem.DatabaseElem] {
	if mt.structType == "btree" {
		return mt.tree.SortedSlice()
	} else {
		return mt.list.Flush()
	}
}
