package main

type BloomFilter struct {
	k   uint
	m   uint
	niz []byte
	fns []HashWithSeed
}

func (bf *BloomFilter) Insert(rec string) {
	for _, fn := range bf.fns {
		data := []byte(rec)
		a := fn.Hash(data)
		a = a % uint64(bf.m)
		bf.niz[a] = 1
	}
}

func (bf BloomFilter) Search(rec string) bool {
	for _, fn := range bf.fns {
		data := []byte(rec)
		a := fn.Hash(data)
		a = a % uint64(bf.m)
		if bf.niz[a] == 0 {
			return false
		}
	}
	return true
}
