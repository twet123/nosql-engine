package sstable

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"math"
	bloomfilter "nosql-engine/packages/utils/bloom-filter"
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	merkletree "nosql-engine/packages/utils/merkle-tree"
	"os"
	"strconv"
	"strings"
)

var order = 0

type SSTable struct {
	data    []GTypes.KeyVal[string, database_elem.DatabaseElem]
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

func new(array []GTypes.KeyVal[string, database_elem.DatabaseElem], count int) SSTable {
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

func CreateSStable(array []GTypes.KeyVal[string, database_elem.DatabaseElem], count int, prefix string) {
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
	name := "/usertable-L0-" + strconv.Itoa(order) + "-"
	st.bf.MakeFile(prefix, name+"Filter.db")

	name = prefix + name
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
	file.WriteString(name + "Metadata.db\n")

	file.Close()
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func defineOrder(prefix string) {
	files, err := os.ReadDir("./" + prefix)
	if os.IsNotExist(err) {
		os.MkdirAll(prefix, os.ModePerm)
	} else if err != nil {
		panic(err)
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

func readOrder(prefix string, levelNum uint64) []string {
	filespath := prefix
	files, err := ioutil.ReadDir("./" + filespath)
	if err != nil {
		log.Fatal(err)
	}
	arr := make([]string, 0)
	for i := 0; i < int(levelNum); i++ {
		tocs := findAllTOCPerLevel(i, files)
		tocs = sortTOCPerLevel(tocs)
		arr = append(arr, tocs...)
	}
	return arr
}

func findAllTOCPerLevel(level int, files []fs.FileInfo) []string {
	tocfiles := make([]string, 0)
	for _, file := range files {
		name := file.Name()
		if !strings.Contains(name, "TOC") {
			continue
		}
		if !strings.Contains(name, "L"+strconv.Itoa(level)) {
			continue
		}
		tocfiles = append(tocfiles, name)
	}
	return tocfiles
}

func sortTOCPerLevel(s []string) []string {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

func Find(key string, prefix string, levels uint64) (bool, *database_elem.DatabaseElem) {
	filespath := prefix
	arrToc := readOrder(prefix, levels)
	for _, name := range arrToc {
		fmap := readTOC(name, filespath)
		bf := bloomfilter.NewFromFile(fmap["filter"])
		found := bf.Find(key)
		if !found {
			continue
		}
		found, start, stop := checkSummary(key, fmap["summary"])
		if !found {
			continue
		}
		found, start = checkIndex(key, fmap["index"], start, stop)
		if !found {
			continue
		}
		deleted, dbel := readData(fmap["data"], start)
		if deleted {
			continue
		}
		return true, &dbel
	}
	return false, nil
}

func checkCRC(crc uint32, timestamp uint64, tombstone byte, key string, value []byte) bool {
	byteslice := make([]byte, 0)
	tmpbs := make([]byte, 8)

	binary.LittleEndian.PutUint64(tmpbs, uint64(timestamp))
	byteslice = append(byteslice, tmpbs...)

	byteslice = append(byteslice, tombstone)

	binary.LittleEndian.PutUint64(tmpbs, uint64(len(key)))
	byteslice = append(byteslice, tmpbs...)
	byteslice = append(byteslice, []byte(key)...)

	binary.LittleEndian.PutUint64(tmpbs, uint64(len(value)))
	byteslice = append(byteslice, tmpbs...)
	byteslice = append(byteslice, value...)

	return (crc == CRC32(byteslice))
}

func checkSummary(key string, filename string) (bool, uint64, uint64) { //returns range of index bytes where key may be
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	start := readKey(*file)
	stop := readKey(*file)
	if key < start || key > stop {
		return false, 0, 0
	}
	prevoffset := uint64(0)
	for {
		filekey := readKey(*file)
		offset := readUint64(*file)

		if filekey == key {
			return true, offset, offset
		}
		if filekey > key {
			return true, prevoffset, offset
		}
		prevoffset = offset
	}
}

func checkIndex(key string, filename string, start uint64, stop uint64) (bool, uint64) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	file.Seek(int64(start), io.SeekStart)
	for {
		pos, _ := file.Seek(0, io.SeekCurrent)
		if stop < uint64(pos) {
			return false, 0
		}
		filekey := readKey(*file)
		offset := readUint64(*file)
		if filekey == key {
			return true, offset
		}
		if filekey > key {
			return false, 0
		}
	}
}

func readData(filename string, offset uint64) (bool, database_elem.DatabaseElem) {
	readFile, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer readFile.Close()

	if err != nil {
		log.Fatal(err)
	}
	readFile.Seek(int64(offset), io.SeekStart)
	crc := readUint32(*readFile)
	timestamp := readUint64(*readFile)
	tombstone := readByte(*readFile)
	key := readKey(*readFile)
	length := readUint64(*readFile)
	value := readBytes(*readFile, length)
	equals := checkCRC(crc, timestamp, tombstone, key, value)
	if !equals {
		log.Fatal("crc not match values")
	}
	return tombstone == byte(1), database_elem.DatabaseElem{Tombstone: tombstone, Value: value, Timestamp: timestamp}
}

func readKey(f os.File) string {
	length := readUint64(f)
	buffer := make([]byte, length)
	f.Read(buffer)
	key := string(buffer[:])
	return key
}

func readUint64(f os.File) uint64 {
	buffer := make([]byte, 8)
	f.Read(buffer)
	number := binary.LittleEndian.Uint64(buffer)
	return number
}

func readUint32(f os.File) uint32 {
	buffer := make([]byte, 4)
	f.Read(buffer)
	number := binary.LittleEndian.Uint32(buffer)
	return number
}
func readByte(f os.File) byte {
	buffer := make([]byte, 1)
	f.Read(buffer)
	return buffer[0]
}

func readBytes(f os.File, length uint64) []byte {
	buffer := make([]byte, length)
	f.Read(buffer)
	return buffer
}

func readTOC(filename string, prefix string) map[string]string { //data, index, summary, filter
	readFile, err := os.Open(prefix + "/" + filename)

	if err != nil {
		log.Fatal(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	var fileLines []string

	for fileScanner.Scan() {
		fileLines = append(fileLines, fileScanner.Text())
	}

	readFile.Close()

	fmap := make(map[string]string)
	fmap["data"] = fileLines[0]
	fmap["index"] = fileLines[1]
	fmap["summary"] = fileLines[2]
	fmap["filter"] = fileLines[3]

	return fmap
}
