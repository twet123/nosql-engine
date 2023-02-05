package sstable

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"math"
	bloomfilter "nosql-engine/packages/utils/bloom-filter"
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	merkletree "nosql-engine/packages/utils/merkle-tree"
	"os"
	"sort"
	"strconv"
	"strings"
)

var order = 0

const (
	SINGLE = 0
	PREFIX = 1
	RANGE  = 2
)

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

func CreateSStable(array []GTypes.KeyVal[string, database_elem.DatabaseElem], count int, prefix string, level int, mode string) {
	defineOrder(prefix, level)

	st := new(array, count)
	for offset, element := range array {
		key := element.Key

		st.index = append(st.index, GTypes.KeyVal[string, uint64]{Key: key, Value: uint64(offset)})
		st.bf.Add(string(key))
	}
	createFiles(st, prefix, level, mode)
}

func createFiles(st SSTable, prefix string, level int, mode string) {
	name := "/usertable-L" + strconv.Itoa(level) + "-" + strconv.Itoa(order) + "-"

	if mode == "many" {
		st.bf.MakeFile(prefix, name+"Filter.db", mode)
	}

	nameWithoutPrefix := name
	name = prefix + name
	arr := createDataFile(name, st)

	for i := range st.index {
		st.index[i].Value = arr[i]
	}

	arr, indexOffset := createIndexFile(name, st, mode)

	for i := range st.summary.indexes {
		in := st.summary.indexes[i].Value
		st.summary.indexes[i].Value = arr[in] + indexOffset
	}
	summOffset := createSummaryFile(name, st, mode)
	var bfOffset uint64
	if mode == "one" {
		bfOffset = st.bf.MakeFile(prefix, nameWithoutPrefix+"Data.db", mode)
		appendFileOffsets(name, indexOffset, summOffset, bfOffset)
	}
	createTOCFile(name, mode)
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

func createIndexFile(name string, st SSTable, mode string) ([]uint64, uint64) {
	var file *os.File
	var err error
	var start int64 = 0
	if mode == "many" {
		file, err = os.Create(name + "Index.db")
	} else {
		file, err = os.OpenFile(name+"Data.db", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
		start, _ = file.Seek(0, os.SEEK_END)
	}
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
	return sumoffsets, uint64(start)
}

func createSummaryFile(name string, st SSTable, mode string) uint64 {
	var file *os.File
	var err error
	var start int64 = 0
	if mode == "many" {
		file, err = os.Create(name + "Summary.db")
	} else {
		file, err = os.OpenFile(name+"Data.db", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
		start, _ = file.Seek(0, os.SEEK_END)
	}
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
	return uint64(start)
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

func createTOCFile(name string, mode string) {
	file, err := os.Create(name + "TOC.txt")
	if err != nil {
		panic(err)
	}
	file.WriteString(name + "Data.db\n")
	if mode == "many" {
		file.WriteString(name + "Index.db\n")
		file.WriteString(name + "Summary.db\n")
		file.WriteString(name + "Filter.db\n")
	}
	file.WriteString(name + "Metadata.db\n")

	file.Close()
}

func appendFileOffsets(name string, indexOffset, summOffset, bfOffset uint64) {
	file, err := os.OpenFile(name+"Data.db", os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	byteslice := make([]byte, 0)
	tmpbs := make([]byte, 8)

	binary.LittleEndian.PutUint64(tmpbs, uint64(indexOffset))
	byteslice = append(byteslice, tmpbs...)

	binary.LittleEndian.PutUint64(tmpbs, uint64(summOffset))
	byteslice = append(byteslice, tmpbs...)

	binary.LittleEndian.PutUint64(tmpbs, uint64(bfOffset))
	byteslice = append(byteslice, tmpbs...)

	file.Write(byteslice)
	file.Close()
}

func readFileOffsets(filename string) (uint64, uint64, uint64) { //index, summary, bloomfilter
	readFile, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer readFile.Close()

	readFile.Seek(-24, os.SEEK_END)
	return readUint64(*readFile), readUint64(*readFile), readUint64(*readFile)
}

func ReadFileOffset(filename string) uint64 { //index
	readFile, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer readFile.Close()

	readFile.Seek(-24, os.SEEK_END)
	return readUint64(*readFile)
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func defineOrder(prefix string, level int) {
	files, err := os.ReadDir("./" + prefix)
	if os.IsNotExist(err) {
		os.MkdirAll(prefix, os.ModePerm)
	} else if err != nil {
		panic(err)
	}
	order = 0

	for _, file := range files {
		var s string = file.Name()
		numbers := make([]int, 0)
		pos := 0
		if !strings.Contains(s, "L"+strconv.Itoa(level)) {
			continue
		}
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
			pos /= 10
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
	files, err := os.ReadDir("./" + filespath)
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

func findAllTOCPerLevel(level int, files []fs.DirEntry) []string {
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
	sort.Slice(s, func(i, j int) bool {
		s1, s2 := s[i], s[j]
		n1, _ := strconv.Atoi(strings.Split(s1[13:], "-")[0])
		n2, _ := strconv.Atoi(strings.Split(s2[13:], "-")[0])
		return n1 > n2
	})
	return s
}

func Find(key string, prefix string, levels uint64, mode string) (bool, *database_elem.DatabaseElem) {
	filespath := prefix
	arrToc := readOrder(prefix, levels)

	for _, name := range arrToc {
		fmap := readTOC(name, filespath, mode)
		summOffset, bfOffset := uint64(0), uint64(0)
		if mode == "one" {
			_, summOffset, bfOffset = readFileOffsets(fmap["data"])
		}

		bf := bloomfilter.NewFromFile(fmap["filter"], bfOffset)
		found := bf.Find(key)
		if !found {
			continue
		}
		found, start, stop := checkSummary(key, fmap["summary"], summOffset)
		if !found {
			continue
		}
		found, start = checkIndex(key, fmap["index"], start, stop)
		if !found {
			continue
		}
		deleted, dbel := readData(fmap["data"], start)
		if deleted {
			return false, nil
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

func checkSummary(key string, filename string, fileOffset uint64) (bool, uint64, uint64) { //returns range of index bytes where key may be
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(fileOffset), io.SeekStart)
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
	defer file.Close()
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
	b, db, _ := readDataWithKey(filename, offset)
	return b, db
}

func readDataWithKey(filename string, offset uint64) (bool, database_elem.DatabaseElem, string) {
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
	return tombstone == byte(1), database_elem.DatabaseElem{Tombstone: tombstone, Value: value, Timestamp: timestamp}, key
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

func readTOC(filename, prefix, mode string) map[string]string { //data, index, summary, filter
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
	if mode == "many" {
		fmap["data"] = fileLines[0]
		fmap["index"] = fileLines[1]
		fmap["summary"] = fileLines[2]
		fmap["filter"] = fileLines[3]
	} else {
		fmap["data"] = fileLines[0]
		fmap["index"] = fileLines[0]
		fmap["summary"] = fileLines[0]
		fmap["filter"] = fileLines[0]
	}

	return fmap
}

func PrefixScan(key string, prefix string, levels uint64, mode string, logsPerPage, pageNumber uint64) map[string]database_elem.DatabaseElem {
	kvMap := make(map[string]database_elem.DatabaseElem)
	kvRet := make(map[string]database_elem.DatabaseElem)
	filespath := prefix
	arrToc := readOrder(prefix, levels)
	pageNumberCounter := 0

	for _, name := range arrToc {
		fmap := readTOC(name, filespath, mode)
		summOffset, _ := uint64(0), uint64(0)
		if mode == "one" {
			_, summOffset, _ = readFileOffsets(fmap["data"])
		}

		found, start, stop := checkPrefixSummary(key, fmap["summary"], summOffset)
		if !found {
			continue
		}
		offsets := checkPrefixIndex(key, fmap["index"], start, stop)
		for _, start := range offsets {
			deleted, dbel, key := readDataWithKey(fmap["data"], start)
			if deleted || isSpecialKey(key) {
				continue
			}
			_, ok := kvMap[key]
			if !ok {
				kvMap[key] = dbel
				kvRet[key] = dbel
				if len(kvRet) == int(logsPerPage) {
					if pageNumberCounter < int(pageNumber) {
						pageNumberCounter++
						for k := range kvRet {
							delete(kvRet, k)
						}
					} else {
						if pageNumberCounter == int(pageNumber) {
							return kvRet
						}
					}
				}
			}
		}
	}
	if pageNumberCounter < int(pageNumber) {
		for k := range kvRet {
			delete(kvRet, k)
		}
	}
	return kvRet
}

func checkPrefixSummary(key string, filename string, fileOffset uint64) (bool, uint64, uint64) { //returns range of index bytes where key may be
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(fileOffset), io.SeekStart)
	start := readKey(*file)
	stop := readKey(*file)
	if (key < start && !strings.HasPrefix(start, key)) || key > stop {
		return false, 0, 0
	}
	prevoffset := uint64(0)
	firstIter := true
	for {
		filekey := readKey(*file)
		offset := readUint64(*file)
		if firstIter {
			firstIter = false
			prevoffset = offset
		}

		if key == filekey {
			return true, offset, offset
		}
		if !strings.HasPrefix(filekey, key) && key > filekey {
			prevoffset = offset
		}
		if (!strings.HasPrefix(filekey, key) && key < filekey) || stop == filekey {
			return true, prevoffset, offset
		}
	}
}

func checkPrefixIndex(key string, filename string, start uint64, stop uint64) []uint64 {
	arr := make([]uint64, 0)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	file.Seek(int64(start), io.SeekStart)
	for {
		pos, _ := file.Seek(0, io.SeekCurrent)
		if stop < uint64(pos) {
			return arr
		}
		filekey := readKey(*file)
		offset := readUint64(*file)
		if strings.HasPrefix(filekey, key) {
			arr = append(arr, offset)
		}
	}
}

func RangeScan(key1, key2, prefix string, levels uint64, mode string, logsPerPage, pageNumber uint64) map[string]database_elem.DatabaseElem {
	kvMap := make(map[string]database_elem.DatabaseElem)
	kvRet := make(map[string]database_elem.DatabaseElem)
	pageNumberCounter := 0

	if key1 > key2 {
		return kvMap
	}

	filespath := prefix
	arrToc := readOrder(prefix, levels)

	for _, name := range arrToc {
		fmap := readTOC(name, filespath, mode)
		summOffset, _ := uint64(0), uint64(0)
		if mode == "one" {
			_, summOffset, _ = readFileOffsets(fmap["data"])
		}

		found, start, stop := checkRangeSummary(key1, key2, fmap["summary"], summOffset)
		if !found {
			continue
		}
		offsets := checkRangeIndex(key1, key2, fmap["index"], start, stop)
		for _, start := range offsets {
			deleted, dbel, key := readDataWithKey(fmap["data"], start)
			if deleted || isSpecialKey(key) {
				continue
			}
			_, ok := kvMap[key]
			if !ok {
				kvMap[key] = dbel
				kvRet[key] = dbel
				if len(kvRet) == int(logsPerPage) {
					if pageNumberCounter < int(pageNumber) {
						pageNumberCounter++
						for k := range kvRet {
							delete(kvRet, k)
						}
					}
					if pageNumberCounter == int(pageNumber) {
						return kvRet
					}
				}
			}
		}
	}
	if pageNumberCounter < int(pageNumber) {
		for k := range kvRet {
			delete(kvRet, k)
		}
	}
	return kvRet
}

func checkRangeSummary(key1, key2, filename string, fileOffset uint64) (bool, uint64, uint64) { //returns range of index bytes where key may be
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(fileOffset), io.SeekStart)
	start := readKey(*file)
	stop := readKey(*file)
	if start > key2 || stop < key1 {
		return false, 0, 0
	}
	prevoffset := uint64(0)
	firstIter := true
	for {
		filekey := readKey(*file)
		offset := readUint64(*file)
		if firstIter {
			firstIter = false
			prevoffset = offset
		}
		if filekey < key1 {
			prevoffset = offset
		}
		if filekey > key2 || stop == filekey {
			return true, prevoffset, offset
		}
	}
}

func checkRangeIndex(key1, key2, filename string, start uint64, stop uint64) []uint64 {
	arr := make([]uint64, 0)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	file.Seek(int64(start), io.SeekStart)
	for {
		pos, _ := file.Seek(0, io.SeekCurrent)
		if stop < uint64(pos) {
			return arr
		}
		filekey := readKey(*file)
		offset := readUint64(*file)
		if isInRange(filekey, key1, key2) {
			arr = append(arr, offset)
		}
	}
}

func isInRange(key, key1, key2 string) bool {
	if key1 <= key && key <= key2 {
		return true
	}
	return false
}

// offset:
//   - if file mode == "many" -> offset = readFile.seek(0, io.SeekEnd)
//   - if file mode == "one" -> call function ReadFileOffset(filename) before opening that file
//
// readFile - file pointer
func ReadRecord(readFile *os.File, offset uint64) (string, *database_elem.DatabaseElem) {

	current, _ := readFile.Seek(0, io.SeekCurrent)
	if current == int64(offset) {
		return "", nil
	}
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
	return key, &database_elem.DatabaseElem{Tombstone: tombstone, Value: value, Timestamp: timestamp}
}

func isSpecialKey(key string) bool {
	specials := make([]string, 5)
	specials[0] = "bf_"
	specials[1] = "cms_"
	specials[2] = "tb_"
	specials[3] = "hll_"
	specials[4] = "sh_"

	for _, spec := range specials {
		if strings.HasPrefix(key, spec) {
			return true
		}
	}
	return false
}

// filename: filename of the "Data file"
func getKeyRangeOne(filename string) (string, string) {
	_, summaryOffset, _ := readFileOffsets(filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(summaryOffset), io.SeekStart)
	start := readKey(*file)
	stop := readKey(*file)

	return start, stop
}

// filename is the filename of the "Summary file"
func getKeyRangeMany(filename string) (string, string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	start := readKey(*file)
	stop := readKey(*file)

	return start, stop
}

// if mode is "one" -> filename is the name of "Data file";
// if mode is "many" -> filename is the name of "Summary file"
func GetKeyRange(filename, mode string) (string, string) {
	if mode == "one" {
		return getKeyRangeOne(filename)
	}
	return getKeyRangeMany(filename)
}
