package main

type Interface interface {
	add(item []byte)
	findMin(item []byte)
}

type CountMinSketch struct {
	k             uint // number of hash values
	n             uint // number of elements
	m             uint // size of the filter
	bitMatrix     [][]uint64
	hashFunctions []HashWithSeed
}

func New(k float64, m float64) *CountMinSketch {
	t := CalculateM(m) //m
	v := CalculateK(k) //k
	Matrix := make([][]uint64, v)
	for i := uint(0); i < v; i++ {
		Matrix[i] = make([]uint64, t)
	}
	return &CountMinSketch{
		bitMatrix:     Matrix,
		m:             uint(t),
		k:             uint(v),
		n:             uint(0),
		hashFunctions: CreateHashFunctions(v),
	}
}

func (cms *CountMinSketch) hashValues(item []byte) []uint64 {
	var retVal []uint64

	for _, hf := range cms.hashFunctions {
		retVal = append(retVal, hf.Hash(item))
	}

	return retVal
}

func (cms *CountMinSketch) Add(item []byte) {
	hashes := cms.hashValues(item)
	i := uint(0)
	for {
		if i >= cms.k {
			break
		}
		j := uint(hashes[i]) % cms.m
		cms.bitMatrix[i][j] += 1

		i += 1
	}
}

func (cms *CountMinSketch) findMin(item []byte) uint64 {
	hashes := cms.hashValues(item)
	min := uint64(0)
	i := uint(0)
	for {
		if i >= cms.k {
			break
		}
		j := uint(hashes[i]) % cms.m
		if i == 0 {
			min = cms.bitMatrix[i][j]
		} else if min > cms.bitMatrix[i][j] {
			min = cms.bitMatrix[i][j]
		}

		i += 1

	}
	return min
}
