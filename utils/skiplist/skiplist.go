package main

import (
	"fmt"
	"math/rand"
)

type SkipList struct {
	maxHeight int
	sentinel  *SkipListNode
	size      int
}

type SkipListNode struct {
	key   int
	value string
	next  []*SkipListNode
}

func (sl *SkipList) roll() int {
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

func (sl *SkipList) Search(key int) bool {
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

func (sl *SkipList) search2(key int) (bool, []*SkipListNode) {
	cursor := sl.sentinel
	retlist := make([]*SkipListNode, 0)
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

func (sl *SkipList) Insert(key int) bool {
	found, lista := sl.search2(key)
	if found {
		return false
	}
	level := sl.roll()
	lista = reverse(lista)
	novi := SkipListNode{key: key, next: make([]*SkipListNode, level)}
	for i := 0; i < level; i++ {
		novi.next[i] = lista[i].next[i]
		lista[i].next[i] = &novi
	}
	sl.size++
	return true
}

func (sl *SkipList) Delete(key int) bool {
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

func (sl *SkipList) Print() {
	cursor := sl.sentinel.next[0]
	for ; cursor != nil; cursor = cursor.next[0] {
		fmt.Print(cursor.key)
		fmt.Print(" ")
	}
	fmt.Println()
}

func makeSkipList(maxHeight int) SkipList {
	sentinel := &SkipListNode{next: make([]*SkipListNode, maxHeight)}
	sl := SkipList{sentinel: sentinel, maxHeight: maxHeight, size: 0}
	return sl
}

func reverse(lista []*SkipListNode) []*SkipListNode {
	for i := 0; i < len(lista)/2; i++ {
		lista[i] = lista[len(lista)-1-i]
	}
	return lista
}

func main() {
	sl := makeSkipList(15)
	niz := make([]int, 0)
	for i := 0; i < 20; i++ {
		a := int(rand.Int31n(2000))
		niz = append(niz, a)
		sl.Insert(a)
	}

	fmt.Println("-------------------")
	sl.Print()
	fmt.Println("-------------------")

	for i := 0; i < len(niz); i++ {
		sl.Delete(niz[i])
		fmt.Println("-------------------")
		sl.Print()
		fmt.Println("-------------------")
	}

	fmt.Println(sl.size)
}
