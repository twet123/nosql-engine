package bloomfilter

import (
	"encoding/binary"
	"io"
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
func (bf *BloomFilter) MakeFile(path string, filename string, mode string) uint64 {
	_, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		os.MkdirAll(path, os.ModePerm)
	} else if err != nil {
		panic(err)
	}
	var file *os.File
	var start int64
	if mode == "many" {
		file, err = os.Create(path + filename)
	} else {
		file, err = os.OpenFile(path+filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		start, _ = file.Seek(0, os.SEEK_END)
	}
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
	return uint64(start)
}

func NewFromFile(name string, fileOffset uint64) *BloomFilter {
	file, err := os.Open(name)
	if err != nil {
		panic(err)
	}
	file.Seek(int64(fileOffset), io.SeekStart)
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

func (bf *BloomFilter) Serialize() []byte {
	ret := make([]byte, 0)

	// put m
	ret = binary.BigEndian.AppendUint32(ret, uint32(bf.m))
	// put k
	ret = binary.BigEndian.AppendUint32(ret, uint32(bf.k))
	// put bits
	ret = append(ret, bf.bits...)
	// put hashFunctions
	for _, hashFn := range bf.hashFunctions {
		ret = append(ret, hashFn.Seed...)
	}

	return ret
}

func Deserialize(byteArr []byte) *BloomFilter {
	//get m
	m := binary.BigEndian.Uint32(byteArr[0:4])
	// get k
	k := binary.BigEndian.Uint32(byteArr[4:8])

	byteArr = byteArr[8:]

	// get bits
	bits := make([]byte, m)
	copy(bits, byteArr[0:m])

	byteArr = byteArr[m:]

	// get hashFunctions (seeds)
	hashFunctions := make([]hash.HashWithSeed, k)
	for i := uint32(0); i < k; i++ {
		seed := byteArr[0:32]
		hashFunctions[i].Seed = seed
		byteArr = byteArr[32:]
	}

	return &BloomFilter{
		m:             uint(m),
		k:             uint(k),
		bits:          bits,
		hashFunctions: hashFunctions,
	}
}
