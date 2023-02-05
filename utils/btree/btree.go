package btree

import (
	databaseelem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	Slice "nosql-engine/packages/utils/slice-utils"
	"sort"
)

type BTreeNode struct {
	elements []GTypes.KeyVal[string, databaseelem.DatabaseElem]
	children []*BTreeNode
	parent   *BTreeNode
}

type BTree struct {
	root        *BTreeNode
	MaxChildren int
	MinChildren int
	height      int
}

func createTree(max int) BTree {
	root := BTreeNode{parent: nil, elements: make([]GTypes.KeyVal[string, databaseelem.DatabaseElem], 0), children: make([]*BTreeNode, 0)}

	return BTree{MaxChildren: max, height: 0, root: &root}
}

func (bt BTree) Get(key string) (bool, GTypes.KeyVal[string, databaseelem.DatabaseElem]) {
	empty := GTypes.KeyVal[string, databaseelem.DatabaseElem]{}
	cursor := bt.root
	for h := 0; h <= bt.height; h++ {
		for i, elem := range cursor.elements {
			trenutni_kljuc := elem.Key
			if trenutni_kljuc == key {
				return true, elem
			}
			if trenutni_kljuc > key {
				if !bt.validHeight(h) {
					return false, empty
				}
				cursor = cursor.children[i]
				break
			} else {
				if i == len(cursor.elements)-1 {
					if !bt.validHeight(h) {
						return false, empty
					}
					cursor = cursor.children[i+1]
					break
				}
			}
		}
	}
	return false, empty
}

func (bt BTree) validHeight(visina int) bool {
	if visina == bt.height {
		return false
	}
	return true
}

func (bt *BTree) Set(key string, value databaseelem.DatabaseElem) bool {
	cursor, found := bt.insertSearch(key)
	resetCursor(cursor)
	if found {
		for i, elem := range cursor.elements {
			if elem.Key == key {
				elem.Value = value
				cursor.elements[i] = elem
				return false
			}
		}
	}
	newEl := GTypes.KeyVal[string, databaseelem.DatabaseElem]{Key: key, Value: value}

	cursor.elements = append(cursor.elements, newEl)
	cursor.elements = sortSlice(cursor.elements)
	if len(cursor.elements) <= bt.MaxChildren {
		return true
	}
	for {
		bt.split(cursor)
		cursor = cursor.parent
		if cursor == nil {
			return true
		}
		if len(cursor.elements) <= bt.MaxChildren {
			return true
		}
	}
}

func (bt BTree) insertSearch(key string) (*BTreeNode, bool) {
	cursor := bt.root

	for h := 0; h <= bt.height; h++ {
		for i := 0; i < len(cursor.elements); i++ {
			trenutni_kljuc := cursor.elements[i].Key
			if trenutni_kljuc == key {
				return cursor, true
			}
			if trenutni_kljuc > key {
				if !bt.validHeight(h) {
					return cursor, false
				}
				cursor = cursor.children[i]
				break
			} else {
				if i == len(cursor.elements)-1 {
					if !bt.validHeight(h) {
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

func resetCursor(cursor *BTreeNode) {
	tmp := cursor.elements
	cursor.elements = make([]GTypes.KeyVal[string, databaseelem.DatabaseElem], len(tmp))
	for i := 0; i < len(cursor.elements); i++ {
		cursor.elements[i] = tmp[i]
	}
}

func sortSlice(niz []GTypes.KeyVal[string, databaseelem.DatabaseElem]) []GTypes.KeyVal[string, databaseelem.DatabaseElem] {
	sort.Slice(niz, func(i, j int) bool {
		return niz[i].Key < niz[j].Key
	})
	return niz
}

func (bt *BTree) split(cursor *BTreeNode) {
	srednjiIndex := (int)((bt.MaxChildren + bt.MaxChildren%2) / 2)
	srednji := cursor.elements[srednjiIndex]
	parent := cursor.parent
	if parent == nil {
		parent = &BTreeNode{parent: nil, children: make([]*BTreeNode, 0), elements: make([]GTypes.KeyVal[string, databaseelem.DatabaseElem], 0)}
		parent.children = append(parent.children, cursor)
		bt.root = parent
		bt.height++
		cursor.parent = parent
	}

	parent.elements = append(parent.elements, srednji)
	parent.elements = sortSlice(parent.elements)

	levo, desno := makeChildren(cursor, srednjiIndex)

	index := findElem(parent, srednji.Key)

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

func makeChildren(cursor *BTreeNode, srednjiIndex int) (*BTreeNode, *BTreeNode) {
	parent := cursor.parent
	if len(cursor.children) == 0 {
		levo := BTreeNode{parent: parent, children: make([]*BTreeNode, 0), elements: cursor.elements[:srednjiIndex]}
		desno := BTreeNode{parent: parent, children: make([]*BTreeNode, 0), elements: cursor.elements[srednjiIndex+1:]}
		return &levo, &desno
	}
	levo := BTreeNode{parent: parent, children: cursor.children[:srednjiIndex+1], elements: cursor.elements[:srednjiIndex]}
	desno := BTreeNode{parent: parent, children: cursor.children[srednjiIndex+1:], elements: cursor.elements[srednjiIndex+1:]}
	return &levo, &desno
}

func findElem(cursor *BTreeNode, key string) int {
	for i, elem := range cursor.elements {
		if elem.Key == key {
			return i
		}
	}
	return -1
}

func Init(min, max int) *BTree {
	root := BTreeNode{elements: make([]GTypes.KeyVal[string, databaseelem.DatabaseElem], 0), children: make([]*BTreeNode, 0), parent: nil}
	return &BTree{root: &root, MaxChildren: max, MinChildren: min, height: 0}
}

func (cur *BTreeNode) SortedSlice() (slice []GTypes.KeyVal[string, databaseelem.DatabaseElem]) {
	if len(cur.children) == 0 {
		return Slice.Copy(cur.elements)
	}

	for i, child := range cur.children {
		slice = append(slice, child.SortedSlice()...)
		if i < len(cur.children)-1 {
			slice = append(slice, cur.elements[i])
		}
	}
	return
}

func (tree *BTree) SortedSlice() []GTypes.KeyVal[string, databaseelem.DatabaseElem] {
	return tree.root.SortedSlice()
}
