package compaction

import (
	"bufio"
	"encoding/binary"
	"io"
	bloomfilter "nosql-engine/packages/utils/bloom-filter"
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	"nosql-engine/packages/utils/sstable"
	"os"
	"strconv"
	"strings"
)

func NeedsCompaction(level uint64, prefix string, maxTables uint64, maxLevels uint64) (bool, []string) {
	if level >= maxLevels {
		return false, nil
	}

	dirs, err := os.ReadDir(prefix)

	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		panic(err)
	}

	resultingFiles := make([]string, 0)
	for _, dir := range dirs {
		tokens := strings.Split(dir.Name(), "-")

		if tokens[1] == "L"+strconv.Itoa(int(level)) && tokens[3] == "TOC.txt" {
			resultingFiles = append(resultingFiles, dir.Name())
		}
	}

	return len(resultingFiles) == int(maxTables), resultingFiles
}

func DoCompaction(level uint64, prefix string, maxTables uint64, maxLevels uint64, sstableMode string, summaryCount int) {
	res, files := NeedsCompaction(level, prefix, maxTables, maxLevels)
	if !res {
		return
	}

	dataFiles := make([]string, len(files))
	for i, tocFile := range files {
		file, err := os.Open(prefix + tocFile)

		if err != nil {
			panic(err)
		}

		dataFile := getDataFile(file)

		if dataFile != "" {
			dataFiles[i] = dataFile
		}

		file.Close()
	}

	newTableNum := getNextTableNum(level+1, prefix)
	sstable.DefineOrder(prefix, int(level+1))
	resFile, err := os.OpenFile(prefix+"usertable-L"+strconv.Itoa(int(level+1))+"-"+strconv.Itoa(int(newTableNum))+"-Data.db", os.O_WRONLY|os.O_CREATE, os.ModePerm)

	if err != nil {
		panic(err)
	}

	// initializing file pointers
	filePointers := make([]*os.File, len(dataFiles))
	filePointerEnds := make([]uint64, len(dataFiles))

	for i := range filePointers {
		fp, err := os.OpenFile(dataFiles[i], os.O_RDONLY|os.O_RDWR, os.ModePerm)

		if err != nil {
			panic(err)
		}

		offset, err := fp.Seek(0, io.SeekEnd)
		if err == nil {
			filePointerEnds[i] = uint64(offset)
		}

		fp.Seek(0, io.SeekStart)
		filePointers[i] = fp
	}

	// init min records
	minRecords := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], len(filePointers))

	for i, filePointer := range filePointers {
		var key string
		var value *database_elem.DatabaseElem
		if sstableMode == "one" {
			key, value = sstable.ReadRecord(filePointer, sstable.ReadFileOffset(filePointer.Name()))
		} else {
			key, value = sstable.ReadRecord(filePointer, filePointerEnds[i])
		}
		minRecords[i].Key = key
		if value != nil {
			minRecords[i].Value = *value
		}
	}

	// writing records to the new sstable until all the record sources are empty
	// and creating index
	mtData := make([][]byte, 0)
	index := make([]GTypes.KeyVal[string, uint64], 0)
	for checkRecords(minRecords) {
		recToWrite := whatToWrite(minRecords)

		recOffset, err := resFile.Seek(0, io.SeekCurrent)
		if err == nil {
			index = append(index, GTypes.KeyVal[string, uint64]{Key: recToWrite.Key, Value: uint64(recOffset)})
		}

		mtData = writeRecord(recToWrite, resFile, prefix, mtData)

		minRecords = nextRecords(recToWrite.Key, filePointers, filePointerEnds, minRecords, sstableMode)
	}

	// creating merkle file
	tokens := strings.Split(resFile.Name(), "-")
	sstable.CreateMerkleFile(strings.Join(tokens[0:3], "-")+"-", mtData)

	// creating bloom filter
	bf := bloomfilter.New(len(index), 0.01)

	for _, elem := range index {
		bf.Add(elem.Key)
	}

	// creating summary
	sumIndexes := make([]GTypes.KeyVal[string, uint64], 0)
	if len(index) <= summaryCount {
		summaryCount = 1
	} else {
		summaryCount = (len(index) / summaryCount)
	}
	for i := 0; i < len(index); i++ {
		if i == 0 || i == len(index)-1 || (i+1)%summaryCount == 0 {
			sumIndexes = append(sumIndexes, GTypes.KeyVal[string, uint64]{Key: index[i].Key, Value: uint64(i)})
		}
	}
	summary := sstable.Summary{Start: index[0].Key, Stop: index[len(index)-1].Key, Indexes: sumIndexes}

	// creating other structures
	values := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
	sstable.CreateFiles(sstable.SSTable{Data: values, Index: index, Summary: summary, Bf: *bf, TOC: ""}, prefix, int(level+1), sstableMode, true)

	// closing files
	for _, filePointer := range filePointers {
		filePointer.Close()
	}
	resFile.Close()

	// deleting previous level
	deleteLevel(filePointers, sstableMode)
}

func deleteLevel(filePointers []*os.File, sstableMode string) {
	files := make([]string, 0)
	files = append(files, "TOC.txt")
	files = append(files, "Metadata.db")

	if sstableMode == "many" {
		files = append(files, "Summary.db")
		files = append(files, "Index.db")
		files = append(files, "Filter.db")
	}

	for _, file := range filePointers {
		tokens := strings.Split(file.Name(), "-")
		os.Remove(file.Name())

		for _, lastToken := range files {
			tokens[3] = lastToken
			os.Remove(strings.Join(tokens, "-"))
		}
	}
}

func writeRecord(rec GTypes.KeyVal[string, database_elem.DatabaseElem], file *os.File, prefix string, mtData [][]byte) [][]byte {
	byteslice := make([]byte, 0)
	tmpbs := make([]byte, 8)

	binary.LittleEndian.PutUint64(tmpbs, uint64(rec.Value.Timestamp))
	byteslice = append(byteslice, tmpbs...)

	byteslice = append(byteslice, rec.Value.Tombstone)

	binary.LittleEndian.PutUint64(tmpbs, uint64(len(rec.Key)))
	byteslice = append(byteslice, tmpbs...)
	byteslice = append(byteslice, []byte(rec.Key)...)

	binary.LittleEndian.PutUint64(tmpbs, uint64(len(rec.Value.Value)))
	byteslice = append(byteslice, tmpbs...)
	byteslice = append(byteslice, rec.Value.Value...)

	crc := sstable.CRC32(byteslice)

	crcslice := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcslice, uint32(crc))

	mtelem := make([]byte, 0)
	mtelem = append(mtelem, crcslice...)
	mtelem = append(mtelem, byteslice...)

	mtData = append(mtData, mtelem)

	file.Write(crcslice)
	file.Write(byteslice)

	return mtData
}

func checkRecords(records []GTypes.KeyVal[string, database_elem.DatabaseElem]) bool {
	for _, rec := range records {
		if rec.Key != "" {
			return true
		}
	}

	return false
}

func whatToWrite(minRecords []GTypes.KeyVal[string, database_elem.DatabaseElem]) GTypes.KeyVal[string, database_elem.DatabaseElem] {
	minRecord := minRecords[0]

	for _, rec := range minRecords {
		if (minRecord.Key == "" || minRecord.Key > rec.Key) && rec.Key != "" {
			minRecord = rec
		} else if minRecord.Key == rec.Key && minRecord.Value.Timestamp < rec.Value.Timestamp && rec.Key != "" {
			minRecord = rec
		}
	}

	return minRecord
}

func nextRecords(minKey string, filePointers []*os.File, filePointerEnds []uint64, minRecords []GTypes.KeyVal[string, database_elem.DatabaseElem], sstableMode string) []GTypes.KeyVal[string, database_elem.DatabaseElem] {
	var file *os.File

	for i := range minRecords {
		if minRecords[i].Key == minKey {
			file = filePointers[i]
			var key string
			var value *database_elem.DatabaseElem

			if sstableMode == "one" {
				key, value = sstable.ReadRecord(file, sstable.ReadFileOffset(file.Name()))
			} else {
				key, value = sstable.ReadRecord(file, filePointerEnds[i])
			}

			minRecords[i].Key = key
			if value != nil {
				minRecords[i].Value = *value
			}
		}
	}

	return minRecords
}

func getDataFile(file *os.File) string {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Split(line, "-")[3] == "Data.db" {
			return line
		}
	}

	return ""
}

func getNextTableNum(level uint64, prefix string) uint64 {
	dirs, err := os.ReadDir(prefix)

	if os.IsNotExist(err) {
		return 0
	} else if err != nil {
		panic(err)
	}

	max := 0
	for _, dir := range dirs {
		tokens := strings.Split(dir.Name(), "-")

		if tokens[1] == "L"+strconv.Itoa(int(level)) {
			tableNum, err := strconv.Atoi(tokens[2])

			if err == nil && tableNum > max {
				max = tableNum
			}
		}
	}

	return uint64(max + 1)
}
