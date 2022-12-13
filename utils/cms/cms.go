package main

type Cms struct {
	m   uint
	k   uint
	mat [][]uint64
	fns []HashWithSeed
}

func (cm *Cms) Insert(s string) {
	var brojac int32 = 0
	for _, fn := range cm.fns {
		data := []byte(s)
		a := fn.Hash(data)
		a = a % uint64(cm.m)
		cm.mat[brojac][a] += 1
		brojac++
	}
}

func (cm *Cms) Search(s string) uint64 {
	var min uint64 = 0
	var brojac int32 = 0
	for _, fn := range cm.fns {
		data := []byte(s)
		a := fn.Hash(data)
		a = a % uint64(cm.m)
		if min > cm.mat[brojac][a] {
			min = cm.mat[brojac][a]
		}
		brojac++
	}
	return min
}
