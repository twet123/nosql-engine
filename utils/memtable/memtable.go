package main

type MemTable struct {
	struktura   string
	maxCapacity int
	capacity    int
	stablo      BTree
	lista       SkipList
}

type MemTableElem struct {
	tombstone byte
	key       string
	value     []byte
	timestamp uint64
}

func (mt *MemTable) Insert(elem MemTableElem) {
	if mt.struktura == "btree" {
		mt.insertBTree(elem)
	}
	if mt.struktura == "skiplist" {
		mt.insertSkipList(elem)
	}
}
func (mt *MemTable) insertSkipList(elem MemTableElem) {
	if !mt.lista.Search(elem.key) {
		if mt.lista.Insert(elem) {
			mt.capacity++
		}
	} else {
		mt.lista.Update(elem)
	}
}

func (mt *MemTable) insertBTree(elem MemTableElem) {
	if !mt.stablo.Search(elem.key) {
		if mt.stablo.Insert(elem) {
			mt.capacity++
		}
	} else {
		mt.stablo.Update(elem)
	}
}

func (mt *MemTable) Delete(elem MemTableElem) {
	elem.tombstone = 0
	if mt.struktura == "btree" {
		mt.deleteBTree(elem)
	}
	if mt.struktura == "skiplist" {
		mt.deleteSkipList(elem)
	}
}

func (mt *MemTable) deleteBTree(elem MemTableElem) {
	mt.stablo.Update(elem)
}

func (mt *MemTable) deleteSkipList(elem MemTableElem) {
	mt.lista.Update(elem)
}

func (mt *MemTable) Flush() {
	if mt.struktura == "btree" {
		mt.stablo.Print()
	}
	if mt.struktura == "skiplist" {
		mt.lista.Print()
	}
	mt.stablo = createTree(3)
	mt.lista = makeNew(15)
	mt.capacity = 0
}
func createMemTable(cap int, tip string) MemTable {
	sl := makeNew(15)
	return MemTable{struktura: tip, capacity: 0, lista: sl, stablo: createTree(3), maxCapacity: cap}
}
