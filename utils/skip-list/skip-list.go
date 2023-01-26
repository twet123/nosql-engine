package skiplist

import (
	"fmt"
	"math/rand"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	key   string
	value []byte
	next  []*SkipListNode
}

func New(maxHeight int) *SkipList {
	headNext := make([]*SkipListNode, maxHeight)

	head := &SkipListNode{
		key:   "",
		value: []byte(""),
		next:  headNext,
	}

	return &SkipList{
		maxHeight: maxHeight,
		height:    1,
		size:      0,
		head:      head,
	}
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			if level > s.height {
				s.height = level
			}
			return level
		}
	}
	if level > s.height {
		s.height = level
	}
	return level
}

func (s *SkipList) Find(key string) *SkipListNode {
	current := s.head

	// start from the highest level and go to the bottom
	for i := s.height - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key <= key {
			current = current.next[i]
		}

		if current.key == key {
			return current
		}
	}

	return nil
}

func (s *SkipList) Add(key string, value []byte) bool {
	// check if element is already there if it is update the timestamp and data
	if s.Find(key) != nil {
		return false
	}

	current := s.head
	level := s.roll()
	if level >= s.maxHeight {
		level = s.maxHeight - 1
	}

	newNode := &SkipListNode{
		key:   key,
		value: value,
		next:  make([]*SkipListNode, s.maxHeight),
	}

	// start from the level of insertion and insert downwards
	for i := level; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].key <= key {
			current = current.next[i]
		}

		newNode.next[i] = current.next[i]
		current.next[i] = newNode
	}

	s.size++
	return true
}

func (s *SkipList) PrintLevels() {
	for i := s.maxHeight - 1; i >= 0; i-- {
		current := s.head
		for current != nil {
			fmt.Println(current.key)

			current = current.next[i]
		}
	}
}
