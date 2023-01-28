package memtable

import (
	"fmt"
	"math/rand"

	"golang.org/x/exp/constraints"
)

type SkipList[K constraints.Ordered, V any] struct {
	maxHeight int
	sentinel  *SkipListNode[K, V]
	size      int
}

type SkipListNode[K constraints.Ordered, V any] struct {
	key   K
	value MemTableElem[K, V]
	next  []*SkipListNode[K, V]
}

func (sl *SkipList[K, V]) roll() int {
	level := 1
	for {
		if rand.Int31n(10) > 4 {
			break
		}
		level++
		if level >= sl.maxHeight {
			return level
		}
	}
	return level
}

func (sl *SkipList[K, V]) Search(key K) bool {
	cursor := sl.sentinel
	for i := sl.maxHeight - 1; i > -1; i-- {
		if cursor.next[i] == nil {
			continue
		}
		if cursor.next[i].key < key {
			cursor = cursor.next[i]
			i++
			continue
		}
		if cursor.next[i].key == key {
			return true
		}
		if cursor.next[i].key > key {
			continue
		}
	}
	return false
}

func (sl *SkipList[K, V]) search2(key K) (bool, []*SkipListNode[K, V]) {
	cursor := sl.sentinel
	retlist := make([]*SkipListNode[K, V], 0)
	for i := sl.maxHeight - 1; i > -1; i-- {
		if cursor.next[i] == nil {
			retlist = append(retlist, cursor)
			continue
		}
		if cursor.next[i].key < key {
			cursor = cursor.next[i]
			i++
			continue
		}
		if cursor.next[i].key == key {
			retlist = append(retlist, cursor)
			if i == 0 {
				return true, retlist
			}
			continue
		}
		if cursor.next[i].key > key {
			retlist = append(retlist, cursor)
			continue
		}
	}
	return false, retlist
}

func (sl *SkipList[K, V]) Insert(elem MemTableElem[K, V]) bool {
	key := elem.key
	found, lista := sl.search2(key)
	if found {
		return false
	}
	level := sl.roll()
	lista = reverse(lista)
	novi := SkipListNode[K, V]{key: key, next: make([]*SkipListNode[K, V], level), value: elem}
	for i := 0; i < level; i++ {
		novi.next[i] = lista[i].next[i]
		lista[i].next[i] = &novi
	}
	sl.size++
	return true
}

func (sl *SkipList[K, V]) Update(elem MemTableElem[K, V]) bool {
	key := elem.key
	found, lista := sl.search2(key)
	if !found {
		return false
	}
	lista = reverse(lista)
	target := lista[0].next[sl.maxHeight-len(lista)]
	target.value = elem

	return true
}

func (sl *SkipList[K, V]) Delete(key K) bool {
	found, lista := sl.search2(key)
	if !found {
		return false
	}
	lista = reverse(lista)
	target := lista[0].next[sl.maxHeight-len(lista)]

	if len(lista) < sl.maxHeight {
		tmp := lista[0]
		lista = reverse(lista)
		for i := len(lista); i < sl.maxHeight; i++ {
			lista = append(lista, tmp)
		}
		lista = reverse(lista)
	}

	for i := 0; i < len(target.next); i++ {
		lista[i].next[i] = target.next[i]
	}
	sl.size--
	return true
}

func (sl *SkipList[K, V]) Print() {
	cursor := sl.sentinel.next[0]
	for ; cursor != nil; cursor = cursor.next[0] {
		fmt.Print(cursor.key)
		fmt.Print(" ")
	}
	fmt.Println()
}

func (sl *SkipList[K, V]) Clear() {
	tmp := MakeNew[K, V](sl.maxHeight)
	sl = &tmp
}

func MakeNew[K constraints.Ordered, V any](maxHeight int) SkipList[K, V] {
	sentinel := &SkipListNode[K, V]{next: make([]*SkipListNode[K, V], maxHeight)}
	sl := SkipList[K, V]{sentinel: sentinel, maxHeight: maxHeight, size: 0}
	return sl
}

func reverse[K constraints.Ordered, V any](lista []*SkipListNode[K, V]) []*SkipListNode[K, V] {
	for i := 0; i < len(lista)/2; i++ {
		lista[i] = lista[len(lista)-1-i]
	}
	return lista
}
