package main

import (
	"fmt"
	"sort"
)

type BTreeNode struct {
	keys     []string
	values   []MemTableElem
	children []*BTreeNode
	parent   *BTreeNode
}

type BTree struct {
	head        *BTreeNode
	maxChildren int
	height      int
}

func createTree(max int) BTree {
	head := BTreeNode{parent: nil, keys: make([]string, 0), children: make([]*BTreeNode, 0)}

	return BTree{maxChildren: max, height: 0, head: &head}
}

func (bt BTree) Search(key string) bool {
	cursor := bt.head
	for h := 0; h <= bt.height; h++ {
		for i := 0; i < len(cursor.keys); i++ {
			trenutni_kljuc := cursor.keys[i]
			if trenutni_kljuc == key {
				return true
			}
			if trenutni_kljuc > key {
				if !bt.validnaVisina(h) {
					return false
				}
				cursor = cursor.children[i]
				break
			} else {
				if i == len(cursor.keys)-1 {
					if !bt.validnaVisina(h) {
						return false
					}
					cursor = cursor.children[i+1]
					break
				}
			}
		}
	}
	return false
}

func (bt BTree) insertSearch(key string) (*BTreeNode, bool) {
	cursor := bt.head

	for h := 0; h <= bt.height; h++ {
		for i := 0; i < len(cursor.keys); i++ {
			trenutni_kljuc := cursor.keys[i]
			if trenutni_kljuc == key {
				return cursor, true
			}
			if trenutni_kljuc > key {
				if !bt.validnaVisina(h) {
					return cursor, false
				}
				cursor = cursor.children[i]
				break
			} else {
				if i == len(cursor.keys)-1 {
					if !bt.validnaVisina(h) {
						return cursor, false
					}
					cursor = cursor.children[i+1]
					break
				}
			}
		}
	}
	return cursor, false
}

func (bt BTree) validnaVisina(visina int) bool {
	if visina == bt.height {
		return false
	}
	return true
}

func (bt *BTree) Insert(elem MemTableElem) bool {
	key := elem.key
	cursor, found := bt.insertSearch(key)
	if found {
		return false
	}
	cursor.keys = append(cursor.keys, key)
	cursor.values = append(cursor.values, elem)
	cursor.keys = sortiraj(cursor.keys)
	cursor.values = sortirajValues(cursor.values)
	if len(cursor.keys) <= bt.maxChildren {
		return true
	}
	for {
		bt.razdvoj(cursor)
		cursor = cursor.parent
		if cursor == nil {
			return true
		}
		if len(cursor.keys) <= bt.maxChildren {
			return true
		}
	}
}

func (bt *BTree) Update(elem MemTableElem) bool {
	key := elem.key
	cursor, found := bt.insertSearch(key)
	if !found {
		return false
	}
	index := findElem(cursor, key)
	cursor.values[index] = elem
	return true
}

func (bt *BTree) Print() {
	cursor := bt.head
	fmt.Println(cursor.keys[0])
}

func (bt *BTree) razdvoj(cursor *BTreeNode) {
	srednjiIndex := (int)((bt.maxChildren + bt.maxChildren%2) / 2)
	//srednjiIndex = 1
	srednji := cursor.keys[srednjiIndex]
	srednjiV := cursor.values[srednjiIndex]
	parent := cursor.parent
	if parent == nil {
		parent = &BTreeNode{parent: nil, children: []*BTreeNode{cursor}, keys: make([]string, 0)}
		bt.head = parent
		bt.height++
		cursor.parent = parent
	}

	parent.keys = append(parent.keys, srednji)
	parent.values = append(parent.values, srednjiV)
	parent.keys = sortiraj(parent.keys)
	parent.values = sortirajValues(parent.values)

	///////decu isto razdvojiti
	//levo := BTreeNode{parent: parent, children: make([]*BTreeNode, 0), keys: cursor.keys[:srednjiIndex]}
	//desno := BTreeNode{parent: parent, children: make([]*BTreeNode, 0), keys: cursor.keys[srednjiIndex+1:]}
	levo, desno := napraviDecu(cursor, srednjiIndex)

	index := findElem(parent, srednji)

	parent.children = append(parent.children[:index], parent.children[index+1:]...)

	parent.children = append(parent.children, nil)
	parent.children = append(parent.children, nil)
	copy(parent.children[index+2:], parent.children[index:])
	parent.children[index] = levo
	parent.children[index+1] = desno

	for i := 0; i < len(levo.children); i++ {
		levo.children[i].parent = levo
	}
	for i := 0; i < len(desno.children); i++ {
		desno.children[i].parent = desno
	}
}

func napraviDecu(cursor *BTreeNode, srednjiIndex int) (*BTreeNode, *BTreeNode) {
	parent := cursor.parent
	if len(cursor.children) == 0 {
		levo := BTreeNode{parent: parent, children: make([]*BTreeNode, 0), keys: cursor.keys[:srednjiIndex]}
		desno := BTreeNode{parent: parent, children: make([]*BTreeNode, 0), keys: cursor.keys[srednjiIndex+1:]}
		return &levo, &desno
	}
	levo := BTreeNode{parent: parent, children: cursor.children[:srednjiIndex+1], keys: cursor.keys[:srednjiIndex]}
	desno := BTreeNode{parent: parent, children: cursor.children[srednjiIndex+1:], keys: cursor.keys[srednjiIndex+1:]}
	return &levo, &desno
}

func findElem(cursor *BTreeNode, key string) int {
	for i := 0; i < len(cursor.keys); i++ {
		if cursor.keys[i] == key {
			return i
		}
	}
	return -1
}

func sortiraj(niz []string) []string {
	sort.Slice(niz, func(i, j int) bool {
		return niz[i] < niz[j]
	})
	return niz
}

func sortirajValues(niz []MemTableElem) []MemTableElem {
	sort.Slice(niz, func(i, j int) bool {
		return niz[i].key < niz[j].key
	})
	return niz
}

func (bt *BTree) Delete(key string) {
	cursor, found := bt.insertSearch(key)
	if !found {
		fmt.Println("greska ne postoji kljuc")
		return
	}
	if cursor == bt.head && bt.height == 0 && len(bt.head.keys) == 1 {
		bt.head.keys = bt.head.keys[:0]
		return
	}
	if len(cursor.children) == 0 { //ako je list
		if len(cursor.keys) > 1 {
			index := findElem(cursor, key)
			cursor.keys = append(cursor.keys[:index], cursor.keys[index+1:]...) //remove at index
		} else {
			indexCurrent := findNode(cursor, key)                //index lista u odnosu na roditelja
			indexSibling := bt.findSibling(cursor, indexCurrent) //index brata sa >1 elemenata
			if indexSibling != -1 {                              //rotacija u odnosu na poziciju brata
				if indexCurrent < indexSibling {
					cursor.keys[0] = cursor.parent.keys[0]
					cursor.parent.keys[0] = cursor.parent.children[indexSibling].keys[0]
					tmp := cursor.parent.children[indexSibling]
					tmp.keys = append(tmp.keys[:0], tmp.keys[1:]...)
				} else {
					cursor.keys[0] = cursor.parent.keys[indexCurrent-1]
					tmp := cursor.parent
					//copy(tmp.keys[1:], tmp.keys[0:])
					tmp.keys[indexCurrent-1] = tmp.children[indexSibling].keys[len(tmp.children[indexSibling].keys)-1]
					//tmp.keys[0] = tmp.children[indexSibling].keys[len(tmp.children[indexSibling].keys)-1]
					tmp = tmp.children[indexSibling]
					tmp.keys = tmp.keys[:len(tmp.keys)-1]
				}
			} else { //ako nema brace sa >1 kljuceva
				if len(cursor.parent.keys) == 1 { //ako roditelj ima samo 1 kljuc
					cursor.keys[0] = cursor.parent.keys[0]
					bt.Delete(cursor.keys[0])
				} else {
					index := findElem(cursor, key)                                      //0
					cursor.keys = append(cursor.keys[:index], cursor.keys[index+1:]...) //obrisemo kljuc -> []
					indexSibling = indexCurrent
					if indexSibling == bt.maxChildren-1 {
						indexSibling--
					}
					cursor = cursor.parent
					cursor.children = append(cursor.children[:indexCurrent], cursor.children[indexCurrent+1:]...)

					cursor.children[indexSibling].keys = append(cursor.children[indexSibling].keys, cursor.keys[indexSibling])
					cursor.children[indexSibling].keys = sortiraj(cursor.children[indexSibling].keys)

					cursor.keys = append(cursor.keys[:indexSibling], cursor.keys[indexSibling+1:]...)
				}
				//fmt.Println("dadadadadada")
			}
		}
	} else { //ako nije list
		indexCurrent := findNode(cursor, key)
		if len(cursor.keys) == 1 && allOnes(cursor) { //ako su ispod jedinice
			indexSibling := bt.findSibling(cursor, indexCurrent)
			if indexSibling != -1 { //ako ima brata sa >1 kljuceva
				if indexSibling < indexCurrent {
					cursor.keys[0] = cursor.parent.keys[indexCurrent-1]
					tmp := cursor.parent.children[indexSibling]
					cursor.parent.keys[indexCurrent-1] = tmp.keys[len(tmp.keys)-1]
					tmp.keys = tmp.keys[:len(tmp.keys)-1]

					tmp2 := tmp.children[len(tmp.children)-1]
					tmp.children = tmp.children[:len(tmp.children)-1]

					merge(cursor.children[0], cursor.children[1])
					cursor.children = append(cursor.children, tmp2)
					tmp2 = cursor.children[0]
					cursor.children[1] = cursor.children[0]
					cursor.children[0] = tmp2

					//cursor.children[0].keys = append(cursor.children[0].keys, cursor.children[1].keys...)
					//cursor.children[1].keys = cursor.children[0].keys
					//cursor.children[0].keys = tmp2
				} else {
					cursor.keys[0] = cursor.parent.keys[indexCurrent]
					tmp := cursor.parent.children[indexSibling]
					cursor.parent.keys[indexCurrent] = tmp.keys[0]
					tmp.keys = tmp.keys[1:]

					tmp2 := tmp.children[0]
					tmp.children = tmp.children[1:] ////////////////

					merge(cursor.children[0], cursor.children[1])
					cursor.children = append(cursor.children, tmp2)

					//cursor.children[0].keys = append(cursor.children[0].keys, cursor.children[1].keys...)
					//cursor.children[1].keys = tmp2
				}
			} else { //nema brata sa >1 kljuceva
				//tmp2 := false
				if indexCurrent == 0 {
					indexCurrent = 1
					//tmp2 = true
					//cursor.keys[0] = cursor.parent.children[1].keys[0]
					//cursor = cursor.parent.children[1]
				}
				if cursor.parent == nil {
					bt.height--
					for i := 1; i < len(cursor.children); i++ {
						cursor.children[0].keys = append(cursor.children[0].keys, cursor.children[i].keys...)
					}
					bt.head = cursor.children[0]
					bt.head.parent = nil
					return
				}

				cursor.keys[0] = cursor.parent.keys[indexCurrent-1] //
				//cursor.parent.keys = cursor.parent.keys[:len(cursor.parent.keys)-1] //
				cursor.parent.keys = append(cursor.parent.keys[:indexCurrent-1], cursor.parent.keys[indexCurrent:]...)

				parent := cursor.parent
				merge(cursor.children[0], cursor.children[1])
				merge(parent.children[indexCurrent-1], parent.children[indexCurrent]) //
				//parent.children[indexCurrent-1].keys = append(parent.children[indexCurrent-1].keys, parent.children[indexCurrent].keys...) //
				//cursor.children[0].keys = append(cursor.children[0].keys, cursor.children[1].keys...)                                      ///ostala deca unutra se spajaju...
				//cursor.children = cursor.children[:len(cursor.children)-1]
				//x := parent.children[indexCurrent].children
				//parent.children = append(parent.children[:indexCurrent], parent.children[indexCurrent+1:]...)
				//parent.children = parent.children[:len(parent.children)-1]
				//if tmp2 {
				//	cursor.children = append(cursor.children, x...)
				//} else {
				//	parent.children[indexCurrent-1].children = append(parent.children[indexCurrent-1].children, cursor.children...)
				//}

				////parent.children[indexCurrent-1].keys = sortiraj(parent.children[indexCurrent-1].keys)
				if len(parent.keys) == 0 {
					if bt.head == parent {
						bt.height--
						bt.head = parent.children[0]
						parent.children[0].parent = nil
					} else {
						parent.keys = append(parent.keys, cursor.keys[0])
						bt.Delete(parent.keys[0])
					}
				}
			}
		} else {
			deletingIndex := findElem(cursor, key)
			leaf := findLeaf(cursor, deletingIndex)
			if len(leaf.keys) > 1 {
				cursor.keys[deletingIndex] = leaf.keys[len(leaf.keys)-1]
				leaf.keys = leaf.keys[:len(leaf.keys)-1]
			} else {
				tmp := leaf.keys[0]
				bt.Delete(tmp)
				cursor.keys[deletingIndex] = tmp
			}
		}
	}
}

/*func (bt *BTree) Delete2(key int) {
	cursor, found := bt.insertSearch(key)
	index := findElem(cursor, key)
	indexCurrent := findNode(cursor, key)
	if !found {
		fmt.Println("ne valja nema kljuca")
		return
	}
	if isLeaf(cursor) {
		if len(cursor.keys) > 1 {
			cursor.keys = append(cursor.keys[:index], cursor.keys[index+1:]...)
			return
		}
		indexSibling := bt.findSibling(cursor, indexCurrent)
		if indexSibling == -1 {
			index = 0                                                           //0
			cursor.keys = append(cursor.keys[:index], cursor.keys[index+1:]...) //obrisemo kljuc -> []
			indexSibling = indexCurrent
			if indexSibling == bt.maxChildren-1 {
				indexSibling--
			}
			cursor = cursor.parent
			cursor.children = append(cursor.children[:indexCurrent], cursor.children[indexCurrent+1:]...)

			cursor.children[indexSibling].keys = append(cursor.children[indexSibling].keys, cursor.keys[indexSibling])
			cursor.children[indexSibling].keys = sortiraj(cursor.children[indexSibling].keys)

			cursor.keys = append(cursor.keys[:indexSibling], cursor.keys[indexSibling+1:]...)
			return
		}
		if indexCurrent < indexSibling {
			cursor.keys[0] = cursor.parent.keys[indexCurrent]
			sibling := cursor.parent.children[indexSibling]
			parent := cursor.parent
			parent.keys[indexCurrent] = sibling.keys[0]
			sibling.keys = sibling.keys[1:]
		} else {
			cursor.keys[0] = cursor.parent.keys[indexCurrent-1]
			sibling := cursor.parent.children[indexSibling]
			parent := cursor.parent
			parent.keys[indexCurrent-1] = sibling.keys[len(sibling.keys)-1]
			sibling.keys = sibling.keys[:len(sibling.keys)-1]
		}
		return
	}
	deletingIndex := findElem(cursor, key)
	leaf := findPredecessor(cursor, deletingIndex)
	if len(leaf.keys) > 1 {
		cursor.keys[deletingIndex] = leaf.keys[len(leaf.keys)-1]
		leaf.keys = leaf.keys[:len(leaf.keys)-1]
		return
	}
	leaf = findPredecessor(cursor, deletingIndex)
	if len(leaf.keys) > 1 {
		cursor.keys[deletingIndex] = leaf.keys[len(leaf.keys)-1]
		leaf.keys = leaf.keys[:len(leaf.keys)-1]
		return
	}
	merge(cursor.children[deletingIndex], cursor.children[deletingIndex+1], cursor)
	cursor.keys = append(cursor.keys[:deletingIndex], cursor.keys[:deletingIndex+1]...)

	if len(cursor.keys) == 0 {
		indexSibling := bt.findSibling(cursor, deletingIndex)
		if cursor == bt.head {
			bt.head = cursor
			bt.head.parent = nil
			bt.height--
			return
		}
		if indexSibling == -1 {
			if indexCurrent == 0 {
				indexCurrent = 1
			}
			parent := cursor.parent
			sib := parent.children[indexCurrent-1]
			sib.children = append(sib.children, cursor.children...)
			sib.keys = append(sib.keys, parent.keys[indexCurrent])
			parent.keys = append(parent.keys[:indexCurrent], parent.keys[indexCurrent+1:]...)
		}

		cursor.keys[0] = cursor.parent.keys[indexCurrent]

	}
}*/

/*func (bt *BTree) emptyParent(cursor *BTreeNode, deletingIndex int) {
	if len(cursor.keys) == 0 {
		indexSibling := bt.findSibling(cursor, deletingIndex)
		if cursor == bt.head {
			bt.head = cursor
			bt.head.parent = nil
			bt.height--
			return
		}
		if indexSibling == -1 {
			if indexCurrent == 0 {
				indexCurrent = 1
			}
			parent := cursor.parent
			sib := parent.children[indexCurrent-1]
			sib.children = append(sib.children, cursor.children...)
			sib.keys = append(sib.keys, parent.keys[indexCurrent])
			parent.keys = append(parent.keys[:indexCurrent], parent.keys[indexCurrent+1:]...)
		}

		cursor.keys[0] = cursor.parent.keys[indexCurrent]

	}
}*/

func isLeaf(cursor *BTreeNode) bool {
	if len(cursor.children) == 0 {
		return true
	}
	return false
}

func allOnes(cursor *BTreeNode) bool {
	for i := 0; i < len(cursor.children); i++ {
		if len(cursor.children[i].keys) != 1 {
			return false
		}
	}
	return true
}

func findLeaf(cursor *BTreeNode, indexCurrent int) *BTreeNode {
	cursor = cursor.children[indexCurrent]
	for len(cursor.children) > 0 {
		cursor = cursor.children[len(cursor.children)-1]
	}
	return cursor
}

func findPredecessor(cursor *BTreeNode, indexCurrent int) *BTreeNode {
	cursor = cursor.children[indexCurrent]
	for len(cursor.children) > 0 {
		cursor = cursor.children[len(cursor.children)-1]
	}
	return cursor
}

func findSuccessor(cursor *BTreeNode, indexCurrent int) *BTreeNode {
	cursor = cursor.children[indexCurrent+1]
	for len(cursor.children) > 0 {
		cursor = cursor.children[0]
	}
	return cursor
}

func findNode(cursor *BTreeNode, key string) int {
	cursor = cursor.parent
	if cursor == nil {
		return 0
	}
	for i := 0; i < len(cursor.children); i++ {
		for j := 0; j < len(cursor.children[i].keys); j++ {
			if cursor.children[i].keys[j] == key {
				return i
			}
		}
	}
	return -1
}

func (bt BTree) findSibling(cursor *BTreeNode, pocetak int) int {
	cursor = cursor.parent
	if cursor == nil {
		return -1
	}
	min := pocetak - 1
	max := pocetak + 1
	if pocetak == 0 {
		min = pocetak
		max = 1
	} else {
		if pocetak == len(cursor.children)-1 {
			max = pocetak
			min = max - 1
		}
	}
	for i := min; i <= max; i++ {
		if len(cursor.children[i].keys) > 1 {
			return i
		}
	}
	return -1
}

func merge(levi *BTreeNode, desni *BTreeNode) {
	if len(levi.children) != 0 {
		//levi = levi.children[len(levi.children)-1]
		//desni = desni.children[0]
		merge(levi.children[len(levi.children)-1], desni.children[0])
	} else {
		levi.keys = append(levi.keys, desni.keys...)
		parent := desni.parent
		if parent != nil {
			parent.children = parent.children[1:]
		}
		return
	}
	levi.children = append(levi.children, desni.children...)
	levi.keys = append(levi.keys, desni.keys...)
	parent := desni.parent
	if parent != nil {
		parent.children = parent.children[1:]
	}
}

func merge2(levi, desni, parentt *BTreeNode) {
	if len(levi.children) != 0 {
		merge2(levi.children[len(levi.children)-1], desni.children[0], parentt)
	} else {
		levi.keys = append(levi.keys, desni.keys...)
		parent := desni.parent
		if parent == parentt {
			x := findNode(desni, desni.keys[0])
			parent.children = append(parent.children[:x], parent.children[x+1:]...)
			return
		}
		parent.children = parent.children[1:]
		return
	}
	levi.children = append(levi.children, desni.children...)
	levi.keys = append(levi.keys, desni.keys...)
	parent := desni.parent
	if parent != nil {
		parent.children = parent.children[1:]
	}
}

/*func main() {
stablo := createTree(3)
for i := 5; i < 20; i++ {
	stablo.Insert(i)
}
for i := 5; i < 20; i++ {
	fmt.Println(i)
	stablo.Delete(i)
}
/*stablo.Insert(5)
stablo.Insert(6)
stablo.Insert(7)
stablo.Insert(8)
stablo.Delete(16)
stablo.Delete(11)
stablo.Delete(9)
stablo.Delete(6)
stablo.Delete(12)
stablo.Delete(15)
stablo.Delete(16)
stablo.Delete(6)
stablo.Delete(9)
//stablo.Insert(15)
//stablo.Insert(16)
stablo.Delete(11)
stablo.Delete(14)
stablo.Delete(13)*/
/*
	fmt.Println(stablo.Search(6))

}*/
