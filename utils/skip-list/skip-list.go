package skiplist

import (
	"fmt"
	"math/rand"
	database_elem "nosql-engine/packages/utils/database-elem"
	generic_types "nosql-engine/packages/utils/generic-types"
	"time"
)

type SkipList struct {
	MaxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	key       string
	value     []byte
	tombstone byte
	timestamp uint64
	next      []*SkipListNode
}

func New(maxHeight int) *SkipList {
	headNext := make([]*SkipListNode, maxHeight)

	head := &SkipListNode{
		key:   "",
		value: []byte(""),
		next:  headNext,
	}

	return &SkipList{
		MaxHeight: maxHeight,
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
		if level >= s.MaxHeight {
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

func (s *SkipList) Add(key string, elem database_elem.DatabaseElem) bool {
	// check if element is already there if it is update the timestamp and data
	oldElem := s.Find(key)
	if oldElem != nil {
		oldElem.value = elem.Value
		oldElem.timestamp = uint64(time.Now().Unix())

		return false
	}

	current := s.head
	level := s.roll()
	if level >= s.MaxHeight {
		level = s.MaxHeight - 1
	}

	newNode := &SkipListNode{
		key:       key,
		value:     elem.Value,
		tombstone: elem.Tombstone,
		timestamp: uint64(time.Now().Unix()),
		next:      make([]*SkipListNode, s.MaxHeight),
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

// returns true if capacity has to be updated
func (s *SkipList) Remove(key string) bool {
	oldElem := s.Find(key)

	if oldElem != nil {
		oldElem.tombstone = 1

		return false
	}

	newElem := &database_elem.DatabaseElem{
		Value:     []byte(""),
		Tombstone: 1,
		Timestamp: uint64(time.Now().Unix()),
	}

	s.Add(key, *newElem)
	return true
}

func (s *SkipList) PrintLevels() {
	for i := s.MaxHeight - 1; i >= 0; i-- {
		fmt.Println("------------- LEVEL " + fmt.Sprint(i) + " -------------")
		current := s.head
		for current != nil {
			fmt.Println(current.key)

			current = current.next[i]
		}
	}
}

func (s *SkipList) Flush() []generic_types.KeyVal[string, database_elem.DatabaseElem] {
	elems := make([]generic_types.KeyVal[string, database_elem.DatabaseElem], s.size)
	current := s.head
	current = current.next[0]

	for i := 0; i < s.size; i++ {
		elems[i].Key = current.key
		elems[i].Value.Value = current.value
		elems[i].Value.Tombstone = current.tombstone
		elems[i].Value.Timestamp = current.timestamp

		current = current.next[0]
	}

	return elems
}

func NodeToElem(node SkipListNode) *database_elem.DatabaseElem {
	return &database_elem.DatabaseElem{
		Value:     node.value,
		Tombstone: node.tombstone,
		Timestamp: node.timestamp,
	}
}
