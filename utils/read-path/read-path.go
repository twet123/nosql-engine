package read_path

import (
	"fmt"
	bloomfilter "nosql-engine/packages/utils/bloom-filter"
	"nosql-engine/packages/utils/memtable"
)

type DataStructure struct {
	CRC       uint32
	timestamp uint64
	tombstone byte
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte
}

func checkMemtable(mt *memtable.MemTable, key string) *DataStructure {
	found, element := mt.Find(key)
	if found {
		fmt.Println("sredi cache")
	}
	return nil
}

func checkBloomFilter(filepath string, key string) bool {
	bf := bloomfilter.NewFromFile(filepath)
	return bf.Find(key)
}

func checkIndex() {}
