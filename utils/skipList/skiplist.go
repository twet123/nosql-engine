package main

import (
	"math"
	"math/rand"
)

const (
	maxHeight = 32
	P         = 0.5
)

type Node struct {
	value int
	ls    []*Level
}

type Level struct {
	next *Node
}

type SkipList struct {
	head   *Node
	height int
	size   int
}

func New() *SkipList {
	return &SkipList{
		head: NewNode(maxHeight, int(math.Inf(-1))),
		/////////////
		height: 0,
		size:   0,
	}
}

func NewNode(level, value int) *Node {
	node := new(Node)
	node.value = value
	node.ls = make([]*Level, level)

	for i := 0; i < len(node.ls); i++ {
		node.ls[i] = new(Level)
	}

	return node
}

func (sl *SkipList) roll() int {
	level := 1

	for ; rand.Int31n(2) == 1; level++ {
		if level >= maxHeight {
			break
		}
	}
	if level > sl.height {
		sl.height = level
		//vraca random generisan level sve dok nije veci od max visine
	}
	return level
}

func (sl *SkipList) query(value int) (*Node, bool) {
	var node *Node
	th := sl.head
	//trazi cvor sa zadatom vrednoscu
	//ide najviseg ka najnizem nivou
	for i := sl.height - 1; i >= 0; i-- {
		for th.ls[i].next != nil && th.ls[i].next.value <= value {
			th = th.ls[i].next
			//ide desno sve dok postoji desno cvor i cija vrednost nije veca od trazene
		}

		if th.value == value {
			node = th
			break
		}

		//ako ne moze vise desno,ide na nizi nivo
	}

	if node == nil {
		return nil, false
	}
	return node, true
}

func (sl *SkipList) insert(value int) bool {
	update := make([]*Node, maxHeight)
	//lista pokazivaca na sledeci cvor i-tog nivoa novog cvor

	th := sl.head
	for i := sl.height - 1; i >= 0; i-- {
		for th.ls[i].next != nil && th.ls[i].next.value < value {
			th = th.ls[i].next
			//ide desno sve dok postoji desno cvor i cija vrednost nije veca od trazene
		}

		if th.ls[i].next != nil && th.ls[i].next.value == value {
			return false
			//ako vec postoji element sa tom vrednoscu,vraca false
		}

		update[i] = th //th cvor ce biti prvi levo od novog cvora na tom nivou

	}

	level := sl.roll()
	node := NewNode(level, value)

	for i := 0; i < level; i++ {
		//ako na tom nivou nema nijedan cvor, pokazivac head pokazuje na novi cvor
		if update[i] == nil {
			sl.head.ls[i].next = node
			continue
		}

		//ako postoje cvorovi na tom nivou, onda se insertuje novi cvor

		node.ls[i].next = update[i].ls[i].next
		update[i].ls[i].next = node

	}

	//provera da li postoji cvor nakon inserta
	sl.size++
	return true
}

func (sl *SkipList) delete(value int) bool {
	var node *Node
	update := make([]*Node, sl.height)
	th := sl.head

	for i := sl.height - 1; i >= 0; i-- {
		for th.ls[i].next != nil && th.ls[i].next.value < value {
			th = th.ls[i].next
			//ide desno sve dok postoji desno cvor i cija vrednost nije veca od trazene
		}

		if th.ls[i].next != nil && th.ls[i].next.value == value {
			node = th.ls[i].next
			//u node smjestamo trazeni cvor
		}

		update[i] = th //pamtimo koji cvor se nalazi prije trazenog na tom nivou
	}

	if node == nil {
		//ako ne postoji trazeni cvor,vracamo false
		return false
	}

	for i := 0; i < len(node.ls); i++ {
		//od dna skip liste node prije povezuje sa onim sledecim i tako brise zadati node na tom nivou
		update[i].ls[i].next = node.ls[i].next
		node.ls[i].next = nil
	}

	for i := 0; i < len(sl.head.ls); i++ {
		//kada dodje da head pokazuje na nil to znaci da je stigao do trenutne visine skip liste
		if sl.head.ls[i].next == nil {
			sl.height = i
			break
		}
	}

	sl.size--
	return true

}

func (sl *SkipList) Height() int {
	return sl.height + 1
}

func (sl *SkipList) isEmpty() bool {
	return sl.size == 0
}

func (sl *SkipList) Size() int {
	return sl.size
}
