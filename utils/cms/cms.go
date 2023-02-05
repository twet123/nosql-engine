package cms

import (
	"encoding/binary"
	"nosql-engine/packages/utils/hash"
)

type CountMinSketch struct {
	k             uint64 // number of hash functions
	m             uint64 // size of each num array
	valueMatrix   [][]uint64
	hashFunctions []hash.HashWithSeed
}

func New(epsilon float64, delta float64) *CountMinSketch {
	tempM := uint64(CalculateM(epsilon))
	tempK := uint64(CalculateK(delta))

	tempMatrix := make([][]uint64, tempK)

	for i := uint64(0); i < tempK; i++ {
		tempMatrix[i] = make([]uint64, tempM)
	}

	return &CountMinSketch{
		k:             tempK,
		m:             tempM,
		valueMatrix:   tempMatrix,
		hashFunctions: hash.CreateHashFunctions(uint(tempK)),
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

func (cms *CountMinSketch) Serialize() []byte {
	ret := make([]byte, 0)

	// put k
	ret = binary.BigEndian.AppendUint64(ret, cms.k)
	// put m
	ret = binary.BigEndian.AppendUint64(ret, cms.m)
	// put valueMatrix
	for i := uint64(0); i < cms.k; i++ {
		for j := uint64(0); j < cms.m; j++ {
			ret = binary.BigEndian.AppendUint64(ret, cms.valueMatrix[i][j])
		}
	}
	// put hash functions (their seeds) 32-byte seed
	for _, hashFn := range cms.hashFunctions {
		ret = append(ret, hashFn.Seed...)
	}

	return ret
}

func Deserialize(byteArr []byte) *CountMinSketch {
	// get k
	k := binary.BigEndian.Uint64(byteArr[0:8])
	// get m
	m := binary.BigEndian.Uint64(byteArr[8:16])

	// move byteArr
	byteArr = byteArr[16:]

	// get valueMatrix
	valueMatrix := make([][]uint64, k)
	for i := uint64(0); i < k; i++ {
		valueMatrix[i] = make([]uint64, m)
		for j := uint64(0); j < m; j++ {
			valueMatrix[i][j] = binary.BigEndian.Uint64(byteArr[0:8])
			byteArr = byteArr[8:]
		}
	}

	// get hash function seeds
	hashFunctions := make([]hash.HashWithSeed, k)
	for i := uint64(0); i < k; i++ {
		seed := byteArr[0:32]
		hashFunctions[i].Seed = seed
		byteArr = byteArr[32:]
	}

	return &CountMinSketch{
		k:             k,
		m:             m,
		valueMatrix:   valueMatrix,
		hashFunctions: hashFunctions,
	}
}
