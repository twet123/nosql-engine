package b_tree

import (
	"fmt"
	"github.com/disiqueira/gotree"
	"golang.org/x/exp/constraints"
	GTypes "nosql-engine/packages/utils/generic-types"
	Slice "nosql-engine/packages/utils/slice-utils"
)

// Node B-Tree node - don't create them by yourself - use BTree
type Node[K constraints.Ordered, V any] struct {
	owner     *BTree[K, V]
	parent    *Node[K, V]
	children  []*Node[K, V]
	keyValues []GTypes.KeyVal[K, V]
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

func (cur *Node[K, V]) MostRightLeaf() *Node[K, V] {
	if len(cur.children) > 0 {
		return cur.children[len(cur.children)-1].MostRightLeaf()
	}
	return cur
}

func (cur *Node[K, V]) MostLeftLeaf() *Node[K, V] {
	if len(cur.children) > 0 {
		return cur.children[0].MostLeftLeaf()
	}
	return cur
}

func (cur *Node[K, V]) Search(key K) (found bool, node *Node[K, V], index int) {
	if len(cur.keyValues) == 0 {
		return false, cur, 0
	}

	for i, kv := range cur.keyValues {
		if kv.Key == key {
			return true, cur, i
		}
		if key < kv.Key && len(cur.children) == 0 {
			return false, cur, i
		}
		if key < kv.Key {
			return cur.children[i].Search(key)
		}
	}
	if len(cur.children) == 0 {
		return false, cur, len(cur.keyValues)
	}
	return cur.children[len(cur.keyValues)].Search(key)
}

// Get return KeyVal if found
func (tree *BTree[K, V]) Get(key K) (found bool, keyVal GTypes.KeyVal[K, V]) {
	found, node, index := tree.root.Search(key)
	if found {
		keyVal = node.keyValues[index]
	}
	return
}

// Search return Node and index of Key/Value inside this Node if found
func (tree *BTree[K, V]) Search(key K) (found bool, node *Node[K, V], index int) {
	return tree.root.Search(key)
}

// Set Key/Value to the BTree
func (tree *BTree[K, V]) Set(key K, value V) (newElementAdded bool) {
	found, node, i := tree.Search(key)
	if found {
		node.keyValues[i].Value = value
		return false
	}

	kv := GTypes.KeyVal[K, V]{key, value}
	// Common insertion
	if i == len(node.keyValues) {
		node.keyValues = append(node.keyValues, kv)
	} else {
		node.keyValues = Slice.InsertCopy(node.keyValues[:i], node.keyValues[i:], kv)
	}

	tree.fightOverflowInNode(node, i)
	return true
}

func (tree *BTree[K, V]) fightOverflowInNode(node *Node[K, V], index int) {
	if len(node.keyValues) <= tree.MaxElementsCnt {
		return
	}

	// Rotation
	if node.parent != nil && len(node.children) == 0 {
		// Search for node in parent
		for nodeI, nodeCandidate := range node.parent.children {
			if nodeCandidate == node {
				if nodeI > 0 && len(node.parent.children[nodeI-1].MostRightLeaf().keyValues) < tree.MaxElementsCnt {
					// Rotation to the left
					// Parent key to the left child
					leftNode := node.parent.children[nodeI-1].MostRightLeaf()
					leftNode.keyValues = append(leftNode.keyValues, node.parent.keyValues[nodeI-1])

					// Current child most left key to parent
					node.parent.keyValues[nodeI-1] = node.keyValues[0]

					// Remove current child most left key
					node.keyValues = node.keyValues[1:]
					return
				}
				if nodeI < len(node.parent.keyValues) &&
					len(node.parent.children[nodeI+1].MostLeftLeaf().keyValues) < tree.MaxElementsCnt {
					// Rotation to the right
					// Parent key to the right child
					rightNode := node.parent.children[nodeI+1].MostLeftLeaf()
					rightNode.keyValues = Slice.ConcatBothCopy(node.parent.keyValues[nodeI:nodeI+1], rightNode.keyValues)

					// Current child most right key to parent
					node.parent.keyValues[nodeI] = node.keyValues[len(node.keyValues)-1]

					// Remove current child most right key
					node.keyValues = node.keyValues[:len(node.keyValues)-1]
					return
				}
			}
		}
	}

	// Division
	middleI := len(node.keyValues) / 2

	left, right := new(Node[K, V]), new(Node[K, V])

	left.keyValues = node.keyValues[:middleI]
	if len(node.children) > 0 {
		left.children = Slice.Copy(node.children[:middleI+1])
		for _, child := range left.children {
			child.parent = left
		}
	}

	right.keyValues = node.keyValues[middleI+1:]
	if len(node.children) > 0 {
		right.children = Slice.Copy(node.children[middleI+1:])
		for _, child := range right.children {
			child.parent = right
		}
	}

	// Divide root
	if node.parent == nil {
		left.parent, right.parent = node, node
		node.children = []*Node[K, V]{left, right}
		node.keyValues = []GTypes.KeyVal[K, V]{node.keyValues[middleI]}
		return
	}

	// Or not root
	left.parent, right.parent = node.parent, node.parent
	for nodeI, nodeCandidate := range node.parent.children {
		if nodeCandidate == node {
			if nodeI == 0 {
				node.parent.keyValues = append([]GTypes.KeyVal[K, V]{node.keyValues[middleI]}, node.parent.keyValues...)
				node.parent.children = append([]*Node[K, V]{left, right}, node.parent.children[1:]...)
				tree.fightOverflowInNode(node.parent, index)
				return
			}
			if nodeI == len(node.parent.children)-1 {
				node.parent.keyValues = append(Slice.Copy(node.parent.keyValues), node.keyValues[middleI])
				node.parent.children = append(Slice.Copy(node.parent.children[:nodeI]), left, right)
				tree.fightOverflowInNode(node.parent, index)
				return
			}

			node.parent.keyValues = Slice.InsertCopy(node.parent.keyValues[:nodeI], node.parent.keyValues[nodeI:], node.keyValues[middleI])
			node.parent.children = Slice.InsertCopy(node.parent.children[:nodeI], node.parent.children[nodeI+1:], left, right)
			tree.fightOverflowInNode(node.parent, index)
		}
	}
	return
}

func (tree *BTree[K, V]) Remove(key K) (removed bool) {
	found, node, i := tree.Search(key)
	if !found {
		return false
	}

	if len(node.children) > 0 {
		left := node.children[i]
		for len(left.children) > 0 {
			left = left.children[len(left.children)-1]
		}
		node.keyValues[i] = left.keyValues[len(left.keyValues)-1]
		node = left
		i = len(left.keyValues) - 1
	}

	if i == 0 {
		node.keyValues = node.keyValues[1:]
	} else if i == len(node.keyValues)-1 {
		node.keyValues = node.keyValues[:i]
	} else {
		node.keyValues = Slice.ConcatBothCopy(node.keyValues[:i], node.keyValues[i+1:])
	}
	tree.fightUnderflowInNode(node, i)
	return true
}

func (tree *BTree[K, V]) fightUnderflowInNode(node *Node[K, V], index int) {
	if len(node.keyValues) >= tree.MinElementsCnt {
		return
	}

	if node.parent == nil {
		return
	}

	for nodeI, nodeCandidate := range node.parent.children {
		if nodeCandidate == node {
			// Try to borrow something
			if nodeI > 0 && len(node.parent.children[nodeI-1].MostRightLeaf().keyValues) > tree.MinElementsCnt {
				// Borrow element from left
				leftNode := node.parent.children[nodeI-1].MostRightLeaf()
				node.keyValues = append(
					[]GTypes.KeyVal[K, V]{node.parent.keyValues[nodeI-1]},
					node.keyValues...)
				node.parent.keyValues[nodeI-1] = leftNode.keyValues[len(leftNode.keyValues)-1]
				leftNode.keyValues = leftNode.keyValues[:len(leftNode.keyValues)-1]
				return
			}
			if nodeI < len(node.parent.keyValues) &&
				len(node.parent.children[nodeI+1].MostLeftLeaf().keyValues) > tree.MinElementsCnt {
				// Borrow element from right
				rightNode := node.parent.children[nodeI+1].MostLeftLeaf()
				node.keyValues = append(node.keyValues, node.parent.keyValues[nodeI])
				node.parent.keyValues[nodeI] = rightNode.keyValues[0]
				rightNode.keyValues = rightNode.keyValues[1:]
				return
			}

			// Merge left
			if nodeI > 0 {
				leftNode := node.parent.children[nodeI-1].MostRightLeaf()
				leftNode.keyValues = append(
					append(leftNode.keyValues, node.parent.keyValues[nodeI-1]),
					node.keyValues...,
				)
				if nodeI < len(node.parent.keyValues) {
					node.parent.children = Slice.ConcatBothCopy(node.parent.children[:nodeI], node.parent.children[nodeI+1:])
					node.parent.keyValues = Slice.ConcatBothCopy(node.parent.keyValues[:nodeI-1], node.parent.keyValues[nodeI:])
				} else {
					node.parent.children = node.parent.children[:nodeI]
					node.parent.keyValues = node.parent.keyValues[:nodeI-1]
				}
				if len(node.parent.keyValues) == 0 {
					node.parent.keyValues = node.parent.children[nodeI-1].keyValues
					node.parent.children = node.parent.children[nodeI-1].children
					for _, child := range node.parent.children {
						child.parent = node.parent
					}
				}
				return
			}
			// Merge right
			if nodeI < len(node.parent.keyValues) {
				rightNode := node.parent.children[nodeI+1].MostLeftLeaf()
				rightNode.keyValues = Slice.InsertCopy(node.keyValues, rightNode.keyValues, node.parent.keyValues[nodeI])
				if nodeI > 0 {
					node.parent.children = Slice.ConcatBothCopy(node.parent.children[:nodeI], node.parent.children[nodeI+1:])
					node.parent.keyValues = Slice.ConcatBothCopy(node.parent.keyValues[:nodeI-1], node.parent.keyValues[nodeI:])
				} else {
					node.parent.children = node.parent.children[1:]
					node.parent.keyValues = node.parent.keyValues[1:]
				}
				if len(node.parent.keyValues) == 0 {
					node.parent.keyValues = node.parent.children[nodeI].keyValues
					node.parent.children = node.parent.children[nodeI].children
					for _, child := range node.parent.children {
						child.parent = node.parent
					}
				}
				return
			}
		}
	}
}

func (cur *Node[K, V]) GoTree(root *gotree.Tree) {
	if len(cur.keyValues) == 0 {
		(*root).Add("x")
	}
	var last gotree.Tree = nil
	n := len(cur.keyValues)
	for i := n - 1; i >= 0; i-- {
		if len(cur.children) > 0 {
			last = (*root).Add("")
			cur.children[i+1].GoTree(&last)
		}
		(*root).Add(fmt.Sprint(cur.keyValues[i]))
	}
	if len(cur.children) > 0 {
		last = (*root).Add("")
		cur.children[0].GoTree(&last)
	}
}

func (tree *BTree[K, V]) String() string {
	root := gotree.New("Root")
	tree.root.GoTree(&root)
	return root.Print()
}

func (cur *Node[K, V]) SortedSlice() (slice []GTypes.KeyVal[K, V]) {
	if len(cur.children) == 0 {
		return Slice.Copy(cur.keyValues)
	}

	for i, child := range cur.children {
		slice = append(slice, child.SortedSlice()...)
		if i < len(cur.children)-1 {
			slice = append(slice, cur.keyValues[i])
		}
	}
	return
}

func (tree *BTree[K, V]) SortedSlice() []GTypes.KeyVal[K, V] {
	return tree.root.SortedSlice()
}
