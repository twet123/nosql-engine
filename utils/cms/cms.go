package cms

import (
	"nosql-engine/packages/utils/hash"
)

type CountMinSketch struct {
	k             uint // number of hash functions
	m             uint // size of each num array
	valueMatrix   [][]uint64
	hashFunctions []hash.HashWithSeed
}

func New(epsilon float64, delta float64) *CountMinSketch {
	tempM := CalculateM(epsilon)
	tempK := CalculateK(delta)

	tempMatrix := make([][]uint64, tempK)

	for i := uint(0); i < tempK; i++ {
		tempMatrix[i] = make([]uint64, tempM)
	}

	return &CountMinSketch{
		k:             tempK,
		m:             tempM,
		valueMatrix:   tempMatrix,
		hashFunctions: hash.CreateHashFunctions(tempK),
	}
}

func (cms *CountMinSketch) Add(key string) {
	for i, hashFunction := range cms.hashFunctions {
		j := hashFunction.Hash([]byte(key)) % uint64(cms.m)

		cms.valueMatrix[i][j] += 1
	}
}

func (cms *CountMinSketch) CountMin(key string) uint64 {
	min := uint64(0)

	for i, hashFunction := range cms.hashFunctions {
		j := hashFunction.Hash([]byte(key)) % uint64(cms.m)

		if i == 0 {
			min = cms.valueMatrix[i][j]
		} else if min > cms.valueMatrix[i][j] {
			min = cms.valueMatrix[i][j]
		}
	}

	return min
}
