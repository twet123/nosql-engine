package main

import (
	"fmt"
	"github.com/disiqueira/gotree"
	"golang.org/x/exp/constraints"
	Slice "nosql-engine/packages/utils/slice-utils"
)

// Node B-Tree node - don't create them by yourself - use BTree
type Node[K constraints.Ordered, V any] struct {
	owner    *BTree[K, V]
	parent   *Node[K, V]
	children []*Node[K, V]
	keys     []K
	values   []V
}

// BTree is an object containing root of a B-Tree (Node) and all important methods
type BTree[K constraints.Ordered, V any] struct {
	root           *Node[K, V]
	MaxElementsCnt int
	MinElementsCnt int
}

func Init[K constraints.Ordered, V any](minElementsCnt, maxElementsCnt int) *BTree[K, V] {
	tree := BTree[K, V]{
		MinElementsCnt: minElementsCnt,
		MaxElementsCnt: maxElementsCnt,
		root:           new(Node[K, V]),
	}
	tree.root.owner = &tree
	return &tree
}

func (cur *Node[K, V]) Search(key K) (found bool, node *Node[K, V], index int) {
	if len(cur.keys) == 0 {
		return false, cur, 0
	}

	for i, k := range cur.keys {
		if k == key {
			return true, cur, i
		}
		if key < k && len(cur.children) == 0 {
			return false, cur, i
		}
		if key < k {
			return cur.children[i].Search(key)
		}
	}
	if len(cur.children) == 0 {
		return false, cur, len(cur.keys)
	}
	return cur.children[len(cur.keys)].Search(key)
}

// Get return value if found
func (tree *BTree[K, V]) Get(key K) (found bool, value V) {
	found, node, index := tree.root.Search(key)
	if found {
		value = node.values[index]
	}
	return
}

// Search return Node and index of Key/Value inside this Node if found
func (tree *BTree[K, V]) Search(key K) (found bool, node *Node[K, V], index int) {
	return tree.root.Search(key)
}

// Add Key/Value to the BTree
func (tree *BTree[K, V]) Add(key K, value V) (added bool) {
	found, node, i := tree.Search(key)
	if found {
		return false
	}

	// Common insertion
	if i == len(node.keys) {
		node.keys = Slice.Copy(append(node.keys, key))
		node.values = Slice.Copy(append(node.values, value))
	} else {
		node.keys = Slice.Copy(append(node.keys[:i+1], node.keys[i:]...))
		node.keys[i] = key
		node.values = Slice.Copy(append(node.values[:i+1], node.values[i:]...))
		node.values[i] = value
	}

	tree.fightOverflowInNode(node, i)
	return true
}

func (tree *BTree[K, V]) fightOverflowInNode(node *Node[K, V], index int) {
	if len(node.keys) <= tree.MaxElementsCnt {
		return
	}

	// Rotation
	if node.parent != nil {
		// Search for node in parent
		for nodeI, nodeCandidate := range node.parent.children {
			if nodeCandidate == node {
				if nodeI > 0 && len(node.parent.children[nodeI].keys) < tree.MaxElementsCnt {
					// Rotation to the left
					// Parent key to the left child
					leftNode := node.parent.children[nodeI-1]
					leftNode.keys = Slice.Copy(append(leftNode.keys, node.parent.keys[nodeI-1]))
					leftNode.values = Slice.Copy(append(leftNode.values, node.parent.values[nodeI-1]))

					// Current child most left key to parent
					node.parent.keys[nodeI-1] = node.keys[0]
					node.parent.values[nodeI-1] = node.values[0]

					// Remove current child most left key
					node.keys = node.keys[1:]
					node.values = node.values[1:]
					return
				}
				if nodeI < len(node.parent.keys) &&
					len(node.parent.children[nodeI+1].children) < tree.MaxElementsCnt {
					// Rotation to the right
					// Parent key to the right child
					print("before rotation\n", tree.String())
					rightNode := node.parent.children[nodeI+1]
					rightNode.keys = Slice.ConcatBothCopy(node.parent.keys[nodeI:nodeI+1], rightNode.keys)
					rightNode.values = Slice.ConcatBothCopy(node.parent.values[nodeI:nodeI+1], rightNode.values)

					print(tree.String())
					// Current child most right key to parent
					node.parent.keys[nodeI] = node.keys[len(node.keys)-1]
					node.parent.values[nodeI] = node.values[len(node.values)-1]

					print(tree.String())
					// Remove current child most right key
					node.keys = node.keys[:len(node.keys)-1]
					node.values = node.values[:len(node.values)-1]
					return
				}
			}
		}
	}

	// Division
	middleI := len(node.keys) / 2

	left, right := new(Node[K, V]), new(Node[K, V])
	left.parent, right.parent = node, node

	left.keys = node.keys[:middleI]
	left.values = node.values[:middleI]
	if len(node.children) > 0 {
		left.children = node.children[:middleI+1]
	}

	right.keys = node.keys[middleI+1:]
	right.values = node.values[middleI+1:]
	if len(node.children) > 0 {
		right.children = node.children[middleI+1:]
	}

	// Divide root
	if node.parent == nil {
		node.children = []*Node[K, V]{left, right}
		node.keys = []K{node.keys[middleI]}
		node.values = []V{node.values[middleI]}
		return
	}
	// Or not root
	for nodeI, nodeCandidate := range node.parent.children {
		if nodeCandidate == node {
			if nodeI == 0 {
				node.parent.keys = Slice.Copy(append([]K{node.keys[middleI]}, node.keys...))
				node.parent.values = Slice.Copy(append([]V{node.values[middleI]}, node.values...))
				node.parent.children = Slice.Copy(append([]*Node[K, V]{left, right}, node.children[1:]...))
				tree.fightOverflowInNode(node.parent, index)
				return
			}
			if nodeI == len(node.parent.children)-1 {
				node.parent.keys = Slice.Copy(append(Slice.Copy(node.keys), node.keys[middleI]))
				node.parent.values = Slice.Copy(append(Slice.Copy(node.values), node.values[middleI]))
				node.parent.children = Slice.Copy(append(Slice.Copy(node.children[:nodeI]), left, right))
				tree.fightOverflowInNode(node.parent, index)
				return
			}

			node.parent.keys = Slice.InsertCopy(node.keys[:nodeI], node.keys[nodeI:], node.keys[middleI])
			node.parent.values = Slice.InsertCopy(node.values[:nodeI], node.values[nodeI:], node.values[middleI])
			node.parent.children = Slice.InsertCopy(node.children[:nodeI], node.children[nodeI+1:], left, right)
			tree.fightOverflowInNode(node.parent, index)
		}
	}
	return
}

func (cur *Node[K, V]) GoTree(root *gotree.Tree) {
	if len(cur.keys) == 0 {
		(*root).Add("x")
	}
	var last gotree.Tree = nil
	for i, k := range cur.keys {
		if i < len(cur.children) {
			last = (*root).Add("")
			cur.children[i].GoTree(&last)
		}
		(*root).Add(fmt.Sprint(k, " : ", cur.values[i]))
	}
	if len(cur.children) > 0 {
		last = (*root).Add("")
		cur.children[len(cur.children)-1].GoTree(&last)
	}
}

func (tree *BTree[K, V]) String() string {
	root := gotree.New("Root")
	tree.root.GoTree(&root)
	return root.Print()
}

func main() {
	tree := Init[int32, int32](3, 3)

	res, _, _ := tree.Search(2)
	fmt.Println(res)
	tree.Add(3, 0)
	fmt.Println(tree)
	tree.Add(2, 1)
	fmt.Println(tree)
	tree.Add(3, 2)
	fmt.Println(tree)
	tree.Add(5, 3)
	fmt.Println(tree)
	res, _, _ = tree.Search(2)
	fmt.Println(res)
	tree.Add(8, 4)
	fmt.Println(tree)
	tree.Add(-2, 5)
	fmt.Println(tree)
	tree.Add(4, 6)
	fmt.Println(tree)
}
