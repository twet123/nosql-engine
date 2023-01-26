package main

type Interface interface {
	Add(item []byte)
	isThere(item []byte)
}

type BloomFilter struct {
	k             uint // number of hash values
	n             uint // number of elements
	m             uint // size of the filter
	bitfield      []bool
	hashFunctions []HashWithSeed
}

func New(size int) *BloomFilter {
	t := CalculateM(size, 0.1)
	v := CalculateK(100, t)
	return &BloomFilter{
		bitfield:      make([]bool, size),
		m:             uint(size),
		k:             v,
		n:             uint(0),
		hashFunctions: CreateHashFunctions(v),
	}
}

func (bf *BloomFilter) Add(item []byte) {
	hashes := bf.hashValues(item)
	i := uint(0)
	for {
		if i >= bf.k {
			break
		}
		position := uint(hashes[i]) % bf.m
		bf.bitfield[uint(position)] = true

		i += 1

	}
}

func (bf *BloomFilter) hashValues(item []byte) []uint64 {
	var retVal []uint64

	//fmt.Println(bf.hashFunctions)

	for _, hf := range bf.hashFunctions {
		//fmt.Println(hf.Hash(item))
		retVal = append(retVal, hf.Hash(item))
	}
	//fmt.Println(retVal)

	return retVal
}

func (bf *BloomFilter) isThere(item []byte) bool {
	hashes := bf.hashValues(item)
	exist := true
	i := uint(0)
	for {
		if i >= bf.k {
			break
		}

		position := uint(hashes[i]) % bf.m
		if bf.bitfield[uint(position)] == false {
			return false
		}
		i += 1
	}

	return exist
}
