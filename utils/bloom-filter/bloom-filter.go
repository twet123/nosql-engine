package bloomfilter

import (
	"encoding/binary"
	"nosql-engine/packages/utils/hash"
	"os"

	"github.com/golang-collections/go-datastructures/bitarray"
)

type BloomFilter struct {
	m             uint // bitarray size
	k             uint // number of hash functions
	bits          bitarray.BitArray
	hashFunctions []hash.HashWithSeed
}

func New(expectedElements int, falsePositiveRate float64) *BloomFilter {
	tempM := CalculateM(expectedElements, falsePositiveRate)
	tempK := CalculateK(expectedElements, tempM)

	return &BloomFilter{
		m:             tempM,
		k:             tempK,
		bits:          bitarray.NewBitArray(uint64(tempM)),
		hashFunctions: hash.CreateHashFunctions(tempK),
	}
}

func (bf *BloomFilter) Add(key string) {
	for _, hashFunction := range bf.hashFunctions {
		index := hashFunction.Hash([]byte(key)) % uint64(bf.m)

		bf.bits.SetBit(index)
	}
}

func (bf *BloomFilter) Find(key string) bool {
	for _, hashFunction := range bf.hashFunctions {
		index := hashFunction.Hash([]byte(key)) % uint64(bf.m)

		bit, _ := bf.bits.GetBit(index)
		if !bit {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) MakeFile(name string) {
	file, err := os.Create(name)
	if err != nil {
		panic(err)
	}
	a := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(a, uint32(bf.m))
	file.Write(a)
	binary.LittleEndian.PutUint32(a, uint32(bf.k))
	file.Write(a)
	/*file.Write(bf.bits)
	for _, fn := range bf.fns {
		binary.LittleEndian.PutUint32(a, uint32(len(fn.Seed)))
		file.Write(a)
		file.Write(fn.Seed)
	}*/
	file.Close()
}
