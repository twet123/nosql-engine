package memtable

import (
	"fmt"
	b_tree "nosql-engine/packages/utils/btree"

	"golang.org/x/exp/constraints"
)

type MemTable[K constraints.Ordered, V any] struct {
	struktura   string
	maxCapacity int
	capacity    int
	stablo      b_tree.BTree[K, MemTableElem[K, V]]
	lista       SkipList[K, V]
}

type MemTableElem[K comparable, V any] struct {
	tombstone byte
	key       K
	value     V
	timestamp uint64
}

func (mt *MemTable[K, V]) Insert(elem MemTableElem[K, V]) {
	if mt.struktura == "btree" {
		mt.insertBTree(elem)
	}
	if mt.struktura == "skiplist" {
		mt.insertSkipList(elem)
	}
}
func (mt *MemTable[K, V]) insertSkipList(elem MemTableElem[K, V]) {
	if !mt.lista.Search(elem.key) {
		if mt.lista.Insert(elem) {
			mt.capacity++
		}
	} else {
		mt.lista.Update(elem)
	}
}

func (mt *MemTable[K, V]) insertBTree(elem MemTableElem[K, V]) {
	found, _, _ := mt.stablo.Search(elem.key)
	if !found {
		mt.capacity++
	}
	mt.stablo.Set(elem.key, elem)
}

func (mt *MemTable[K, V]) Delete(elem MemTableElem[K, V]) {
	elem.tombstone = 0
	if mt.struktura == "btree" {
		mt.deleteBTree(elem)
	}
	if mt.struktura == "skiplist" {
		mt.deleteSkipList(elem)
	}
}

func (mt *MemTable[K, V]) deleteBTree(elem MemTableElem[K, V]) {
	mt.stablo.Set(elem.key, elem)
}

func (mt *MemTable[K, V]) deleteSkipList(elem MemTableElem[K, V]) {
	mt.lista.Update(elem)
}

func (mt *MemTable[K, V]) Flush() {
	if mt.struktura == "btree" {
		fmt.Println(mt.stablo.SortedSlice())
	}
	if mt.struktura == "skiplist" {
		mt.lista.Print()
	}
	mt.stablo = *b_tree.Init[K, MemTableElem[K, V]](2, 4)
	mt.lista = MakeNew[K, V](15)
	mt.capacity = 0
}
func CreateMemTable[K constraints.Ordered, V any](cap int, tip string) MemTable[K, V] {
	sl := MakeNew[K, V](15)
	return MemTable[K, V]{struktura: tip, capacity: 0, lista: sl, stablo: *b_tree.Init[K, MemTableElem[K, V]](2, 4), maxCapacity: cap}
}
