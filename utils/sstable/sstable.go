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
	merkletree "nosql-engine/packages/utils/merkle-tree"
	"os"
	"strconv"
)

var order = 0

type SSTable struct {
	data    []GTypes.KeyVal[string, database.DatabaseElem]
	index   []GTypes.KeyVal[string, uint64]
	summary Summary
	bf      bloomfilter.BloomFilter
	TOC     string
}

type Summary struct {
	start   string
	stop    string
	indexes []GTypes.KeyVal[string, uint64]
}

func new(array []GTypes.KeyVal[string, database.DatabaseElem], count int) SSTable {
	bf := bloomfilter.New(len(array), 0.01)
	index := make([]GTypes.KeyVal[string, uint64], 0)

	sumIndexes := make([]GTypes.KeyVal[string, uint64], 0)
	if len(array) <= count {
		count = 1
	} else {
		count = (len(array) / count)
	}
	for i := 0; i < len(array); i++ {
		if i == 0 || i == len(array)-1 || (i+1)%count == 0 {
			sumIndexes = append(sumIndexes, GTypes.KeyVal[string, uint64]{Key: array[i].Key, Value: uint64(i)})
		}
	}

	sum := Summary{start: array[0].Key, stop: array[len(array)-1].Key, indexes: sumIndexes}
	TOC := ""
	return SSTable{bf: *bf, data: array, index: index, summary: sum, TOC: TOC}
}

func CreateSStable(array []GTypes.KeyVal[string, database.DatabaseElem], count int, prefix string) {
	defineOrder(prefix)

	st := new(array, count)
	for offset, element := range array {
		key := element.Key

		st.index = append(st.index, GTypes.KeyVal[string, uint64]{Key: key, Value: uint64(offset)})
		st.bf.Add(string(key))
	}
	createFiles(st, prefix)
}

func createFiles(st SSTable, prefix string) {
	name := prefix + "/usertable-L0-" + strconv.Itoa(order) + "-"
	st.bf.MakeFile(name + "Filter.db")
	arr := createDataFile(name, st)

	for i := range st.index {
		st.index[i].Value = arr[i]
	}

	arr = createIndexFile(name, st)
	for i := range st.summary.indexes {
		in := st.summary.indexes[i].Value
		st.summary.indexes[i].Value = arr[in]
	}
	createSummaryFile(name, st)
	createTOCFile(name)
}

func createDataFile(name string, st SSTable) []uint64 {
	file, err := os.Create(name + "Data.db")
	if err != nil {
		panic(err)
	}
	offsetstart := make([]uint64, 0)
	mtdata := make([][]byte, 0)
	for _, element := range st.data {
		byteslice := make([]byte, 0)
		tmpbs := make([]byte, 8)

		binary.LittleEndian.PutUint64(tmpbs, uint64(element.Value.Timestamp))
		byteslice = append(byteslice, tmpbs...)

		byteslice = append(byteslice, element.Value.Tombstone)

		binary.LittleEndian.PutUint64(tmpbs, uint64(len(element.Key)))
		byteslice = append(byteslice, tmpbs...)
		byteslice = append(byteslice, []byte(element.Key)...)

		binary.LittleEndian.PutUint64(tmpbs, uint64(len(element.Value.Value)))
		byteslice = append(byteslice, tmpbs...)
		byteslice = append(byteslice, element.Value.Value...)

		crc := CRC32(byteslice)

		crcslice := make([]byte, 4)
		binary.LittleEndian.PutUint32(crcslice, uint32(crc))

		mtelem := make([]byte, 0)
		mtelem = append(mtelem, crcslice...)
		mtelem = append(mtelem, byteslice...)

		mtdata = append(mtdata, mtelem)

		offset, _ := file.Seek(0, 1)
		offsetstart = append(offsetstart, uint64(offset))
		file.Write(crcslice)
		file.Write(byteslice)
	}
	file.Close()
	createMerkleFile(name, mtdata)
	return offsetstart
}

func createIndexFile(name string, st SSTable) []uint64 {
	file, err := os.Create(name + "Index.db")
	if err != nil {
		panic(err)
	}
	byteslice := make([]byte, 0)
	tmpbs := make([]byte, 8)

	sumoffsets := make([]uint64, 0)
	for _, element := range st.index {
		sumoffsets = append(sumoffsets, uint64(len(byteslice)))

		binary.LittleEndian.PutUint64(tmpbs, uint64(len([]byte(element.Key))))
		byteslice = append(byteslice, tmpbs...)

		byteslice = append(byteslice, []byte(element.Key)...)
		tmpbs = make([]byte, 8)

		binary.LittleEndian.PutUint64(tmpbs, uint64(element.Value))
		byteslice = append(byteslice, tmpbs...)
	}
	file.Write(byteslice)
	file.Close()
	return sumoffsets
}

func createSummaryFile(name string, st SSTable) {
	file, err := os.Create(name + "Summary.db")
	if err != nil {
		panic(err)
	}
	byteslice := make([]byte, 0)
	tmpbs := make([]byte, 8)

	binary.LittleEndian.PutUint64(tmpbs, uint64(len([]byte(st.summary.start))))
	byteslice = append(byteslice, tmpbs...)
	byteslice = append(byteslice, []byte(st.summary.start)...)

	binary.LittleEndian.PutUint64(tmpbs, uint64(len([]byte(st.summary.stop))))
	byteslice = append(byteslice, tmpbs...)
	byteslice = append(byteslice, []byte(st.summary.stop)...)

	for _, element := range st.summary.indexes {
		binary.LittleEndian.PutUint64(tmpbs, uint64(len([]byte(element.Key))))
		byteslice = append(byteslice, tmpbs...)
		byteslice = append(byteslice, []byte(element.Key)...)
		binary.LittleEndian.PutUint64(tmpbs, uint64(element.Value))
		byteslice = append(byteslice, tmpbs...)
	}
	file.Write(byteslice)
	file.Close()
}

func createMerkleFile(name string, bytes [][]byte) {
	file, err := os.Create(name + "Metadata.db")
	if err != nil {
		panic(err)
	}
	mt := merkletree.New(bytes)
	file.Write([]byte(mt.String()))
	file.Close()
}

func createTOCFile(name string) {
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

func defineOrder(prefix string) {
	files, err := ioutil.ReadDir("./" + prefix)
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
			order = number
		}
	}
	order++
	st := order
	st++
}
