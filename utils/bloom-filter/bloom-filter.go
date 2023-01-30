package bloomfilter

import (
	"encoding/binary"
	"nosql-engine/packages/utils/hash"
	"os"
)

type BloomFilter struct {
	m             uint // bitarray size
	k             uint // number of hash functions
	bits          []byte
	hashFunctions []hash.HashWithSeed
}

func New(expectedElements int, falsePositiveRate float64) *BloomFilter {
	tempM := CalculateM(expectedElements, falsePositiveRate)
	tempK := CalculateK(expectedElements, tempM)

	return &BloomFilter{
		m:             tempM,
		k:             tempK,
		bits:          make([]byte, tempM),
		hashFunctions: hash.CreateHashFunctions(tempK),
	}
}

func (bf *BloomFilter) Add(key string) {
	for _, hashFunction := range bf.hashFunctions {
		index := hashFunction.Hash([]byte(key)) % uint64(bf.m)

		bf.bits[index] = 1
	}
}

func (bf *BloomFilter) Find(key string) bool {
	for _, hashFunction := range bf.hashFunctions {
		index := hashFunction.Hash([]byte(key)) % uint64(bf.m)

		bit := bf.bits[index]
		if bit == 0 {
			return false
		}
	}

	return true
}

// File structure will be 4 bytes for m and k respectively, m bytes for bits and k slices of 32 bytes for seeds
func (bf *BloomFilter) MakeFile(path string, filename string) {
	_, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		os.MkdirAll(path, os.ModePerm)
	} else if err != nil {
		panic(err)
	}

	file, err := os.Create(path + filename)
	if err != nil {
		panic(err)
	}
	buff := make([]byte, 4)
	binary.LittleEndian.PutUint32(buff, uint32(bf.m))
	file.Write(buff)

	binary.LittleEndian.PutUint32(buff, uint32(bf.k))
	file.Write(buff)

	binary.Write(file, binary.LittleEndian, bf.bits)

	for _, fn := range bf.hashFunctions {
		buff = fn.Seed
		binary.Write(file, binary.LittleEndian, buff)
	}

	file.Close()
}

func NewFromFile(name string) *BloomFilter {
	file, err := os.Open(name)
	if err != nil {
		panic(err)
	}

	buff := make([]byte, 4)
	binary.Read(file, binary.LittleEndian, buff)
	m := binary.LittleEndian.Uint32(buff)

	binary.Read(file, binary.LittleEndian, buff)
	k := binary.LittleEndian.Uint32(buff)

	bits := make([]byte, m)
	binary.Read(file, binary.LittleEndian, bits)

	hashFunctions := make([]hash.HashWithSeed, k)
	buff = make([]byte, 32)

	for i := 0; i < int(k); i++ {
		binary.Read(file, binary.LittleEndian, buff)
		hashFunctions[i].Seed = buff
	}

	file.Close()

	return &BloomFilter{
		m:             uint(m),
		k:             uint(k),
		bits:          bits,
		hashFunctions: hashFunctions,
	}
}
