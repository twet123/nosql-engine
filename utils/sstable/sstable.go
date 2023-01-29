package sstable

import (
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	"log"
	"math"
	bloomfilter "nosql-engine/packages/utils/bloom-filter"
	"nosql-engine/packages/utils/database"
	GTypes "nosql-engine/packages/utils/generic-types"
	"os"
	"strconv"

	"golang.org/x/exp/constraints"
)

var order = 1

type Stringable interface {
	string
}

type SSTable[K Stringable] struct {
	data    []GTypes.KeyVal[K, database.DatabaseElem]
	index   []GTypes.KeyVal[K, int]
	summary Summary[K]
	bf      bloomfilter.BloomFilter
	TOC     string
}

type Summary[K constraints.Ordered] struct {
	start   K
	stop    K
	indexes []GTypes.KeyVal[K, int]
}

func new[K Stringable](array []GTypes.KeyVal[K, database.DatabaseElem], count int) SSTable[K] {
	bf := bloomfilter.New(len(array), 0.01)
	index := make([]GTypes.KeyVal[K, int], 0)

	sumIndexes := make([]GTypes.KeyVal[K, int], 2)
	offset := 0
	for i := 0; i < len(array); i++ {
		if i == 0 || i == len(array)-1 || i%count == 0 {
			sumIndexes = append(sumIndexes, GTypes.KeyVal[K, int]{Key: array[i].Key, Value: offset})
			offset++
		}
	}

	sum := Summary[K]{start: array[0].Key, stop: array[len(array)-1].Key, indexes: sumIndexes}
	TOC := ""
	return SSTable[K]{bf: *bf, data: array, index: index, summary: sum, TOC: TOC}
}

func CreateSStable[K Stringable](array []GTypes.KeyVal[K, database.DatabaseElem], count int) {
	defineOrder()

	st := new(array, count)
	for offset, element := range array {
		key := element.Key

		st.index = append(st.index, GTypes.KeyVal[K, int]{Key: key, Value: offset})
		st.bf.Add(string(key))
	}
	createFiles(st)
}

func createFiles[K Stringable](st SSTable[K]) {
	name := "usertable-L0-" + strconv.Itoa(order) + "-"
	st.bf.MakeFile(name + "Filter.db")
	createDataFile(name, st)
	createIndexFile(name, st)
	createSummaryFile(name, st)
	createTOCFile(name)
}

func createDataFile[K Stringable](name string, st SSTable[K]) {
	file, err := os.Create(name + "Data.db")
	if err != nil {
		panic(err)
	}
	for _, element := range st.data {
		byteslice := make([]byte, 0)
		tmpbs := make([]byte, 8)

		binary.LittleEndian.PutUint64(tmpbs, uint64(element.Value.Timestamp))
		byteslice = append(byteslice, tmpbs...)

		byteslice = append(byteslice, element.Value.Tombstone)

		binary.LittleEndian.PutUint64(tmpbs, uint64(len(element.Key)))
		byteslice = append(byteslice, tmpbs...)

		binary.LittleEndian.PutUint64(tmpbs, uint64(len(element.Value.Value)))
		byteslice = append(byteslice, tmpbs...)

		byteslice = append(byteslice, element.Value.Value...)

		crc := CRC32(byteslice)

		crcslice := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcslice, uint32(crc))

		file.Write(crcslice)
		file.Write(byteslice)
	}
	file.Close()
}

func createIndexFile[K Stringable](name string, st SSTable[K]) {
	file, err := os.Create(name + "Index.db")
	if err != nil {
		panic(err)
	}
	byteslice := make([]byte, 0)
	for _, element := range st.index {
		byteslice = append(byteslice, []byte(element.Key)...)
		tmpbs := make([]byte, 8)

		binary.LittleEndian.PutUint64(tmpbs, uint64(element.Value))
		byteslice = append(byteslice, tmpbs...)
	}
	file.Write(byteslice)
	file.Close()
}

func createSummaryFile[K Stringable](name string, st SSTable[K]) {
	file, err := os.Create(name + "Summary.db")
	if err != nil {
		panic(err)
	}
	byteslice := make([]byte, 0)
	tmpbs := make([]byte, 8)

	byteslice = append(byteslice, []byte(st.summary.start)...)
	binary.LittleEndian.PutUint64(tmpbs, uint64(0))
	byteslice = append(byteslice, tmpbs...)

	byteslice = append(byteslice, []byte(st.summary.stop)...)
	binary.LittleEndian.PutUint64(tmpbs, uint64(len(st.index)-1))
	byteslice = append(byteslice, tmpbs...)

	for _, element := range st.summary.indexes {
		byteslice = append(byteslice, []byte(element.Key)...)
		binary.LittleEndian.PutUint64(tmpbs, uint64(element.Value))
		byteslice = append(byteslice, tmpbs...)
	}
	file.Write(byteslice)
	file.Close()
}

func createTOCFile[K Stringable](name string) {
	file, err := os.Create(name + "TOC.txt")
	if err != nil {
		panic(err)
	}
	file.WriteString(name + "Data.db\n")
	file.WriteString(name + "Index.db\n")
	file.WriteString(name + "Summary.db\n")
	file.WriteString(name + "Filter.db\n")

	file.Close()
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func defineOrder() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		var s string = file.Name()
		numbers := make([]int, 0)
		pos := 0
		for i := 13; ; i++ {
			if len(s) < 14 {
				break
			}
			if s[i] < '0' || s[i] > '9' {
				break
			}
			var a int = int(s[i]) - 48
			numbers = append(numbers, a)
			pos++
		}
		pos = int(math.Pow10(pos - 1))
		number := 0
		for _, a := range numbers {
			number += a * pos
			pos--
		}
		if order < number {
			order = pos
		}
	}
	order++
}
