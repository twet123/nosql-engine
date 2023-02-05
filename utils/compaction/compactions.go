package compaction

import (
	"bufio"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	config2 "nosql-engine/packages/utils/config"
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	"nosql-engine/packages/utils/sstable"
	"os"
	"strconv"
	"strings"
)

func openFile(filepath string) *os.File {
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0700)
	if err != nil {
		panic(err)
	}
	return file
}

func levelFilter(tables []fs.FileInfo, level string) []string {
	var retList []string
	for _, table := range tables {
		var s string = table.Name()
		tableLvl := strings.Split(s, "-")[1]
		if tableLvl != ("L"+level) || !strings.Contains(s, "Data.db") {
			continue
		}
		retList = append(retList, table.Name())

	}
	return retList
}

func levelRangeFilter(tables []fs.FileInfo, level string, min, max string, mode string) []string {
	var retList []string
	for _, table := range tables {
		var s string = table.Name()
		tableLvl := strings.Split(s, "-")[1]
		if tableLvl != ("L"+level) || !strings.Contains(s, "Data.db") {
			continue
		}
		min1, max1 := sstable.GetKeyRange(s, mode)
		if min1 > max {
			continue
		}
		if max1 < min {
			continue
		}
		retList = append(retList, table.Name())
	}
	return retList
}

func getDataFileOrderNum(filename string) int {
	orderNum, err := strconv.Atoi(strings.Split(filename, "-")[2])
	if err != nil {
		panic(err)
	}
	return orderNum

}

// provjera da li je potrebna kompakcija datog nivoa
func NeedsCompaction(level int, files []fs.FileInfo, maxPerLevel uint64) bool {
	tables := levelFilter(files, strconv.Itoa(level))

	return len(tables) > int(maxPerLevel)
}

func NeedsCompactionLeveled(level int, files []fs.FileInfo) bool {
	config := config2.GetConfig()
	maxPerLevel := config.LsmLeveledComp[level]
	tables := levelFilter(files, strconv.Itoa(level))
	return len(tables) > int(maxPerLevel)
}

func MergeCompaction(level int, dirPath string) {
	config := config2.GetConfig()
	count := config.SummaryCount
	maxPerLevel := config.LsmMaxPerLevel
	maxLevels := config.LsmLevels
	mode := config.SSTableFiles

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	if !NeedsCompaction(level, files, maxPerLevel) {
		return
	}
	tables := levelFilter(files, strconv.Itoa(level)) //[]naziv_fajlova

	for len(tables) > 1 {

		//uzimamo dvije najmanje tabele i spajamo ih
		//sort.Sort(SortByOther(TwoSlices{filepath_slice: tables, reader_slice: readers}))
		table1, table2 := tables[0], tables[1]

		merged := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
		merged = mergeTwoTables(dirPath+"/"+table1, dirPath+"/"+table2, merged, mode)

		if len(tables) != 2 {
			sstable.CreateSStable(merged, int(count), dirPath, level, mode)
		} else {
			sstable.CreateSStable(merged, int(count), dirPath, level+1, mode)
		}

		deleteOldFiles(dirPath, table1, level)
		deleteOldFiles(dirPath, table2, level)

		files, err = ioutil.ReadDir(dirPath)
		if err != nil {
			panic(err)
		}

		tables = levelFilter(files, strconv.Itoa(level))
	}
	if level < int(maxLevels)-1 {
		//nema kompakcije na poslednjem nivou
		MergeCompaction(level+1, dirPath)
	}
}

func LeveledCompaction(level int, dirPath string) {
	config := config2.GetConfig()
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	if !NeedsCompactionLeveled(level, files) {
		return
	}
	tables := levelFilter(files, strconv.Itoa(level))
	nextTables := levelFilter(files, strconv.Itoa(level+1))

	if level == 0 {
		merged := make([]GTypes.KeyVal[string, database_elem.DatabaseElem], 0)
		for i := 0; i < len(tables); i++ {
			merged = mergeTwoTablesInMemory(merged, readWholeTable(tables[i], config.SSTableFiles))
			deleteOldFiles(dirPath, tables[i], level)
		}
		for i := 0; i < len(nextTables); i++ {
			merged = mergeTwoTablesInMemory(merged, readWholeTable(nextTables[i], config.SSTableFiles))
			deleteOldFiles(dirPath, nextTables[i], level+1)
		}
		step := uint64(len(merged)) / config.SSTableSize
		if uint64(len(merged))%config.SSTableSize > 0 {
			step += 1
		}
		for i := uint64(0); i < uint64(len(merged)); i += step {
			to := i + step
			if to > uint64(len(merged)) {
				to = uint64(len(merged))
			}
			sstable.CreateSStable(merged[i:to], int(config.SummaryCount), dirPath, level+1, config.SSTableFiles)
		}
	} else {
		for i := 0; i < len(tables); i++ {
			merged := readWholeTable(tables[i], config.SSTableFiles)
			deleteOldFiles(dirPath, tables[i], level)
			nextTables = levelRangeFilter(files, strconv.Itoa(level+1),
				merged[0].Key, merged[len(merged)-1].Key, config.SSTableFiles)

			for j := 0; j < len(nextTables); j++ {
				merged = mergeTwoTablesInMemory(merged, readWholeTable(nextTables[j], config.SSTableFiles))
				deleteOldFiles(dirPath, nextTables[j], level+1)
			}
			step := uint64(len(merged)) / config.SSTableSize
			if uint64(len(merged))%config.SSTableSize > 0 {
				step += 1
			}
			for j := uint64(0); j < uint64(len(merged)); j += step {
				to := j + step
				if to > uint64(len(merged)) {
					to = uint64(len(merged))
				}
				sstable.CreateSStable(merged[j:to], int(config.SummaryCount), dirPath, level+1, config.SSTableFiles)
			}
		}
	}

	if level+1 < len(config.LsmLeveledComp)-1 {
		LeveledCompaction(level+1, dirPath)
	}
}

func readWholeTable(path1 string, mode string) (logs []GTypes.KeyVal[string, database_elem.DatabaseElem]) {
	var offset1 int64

	table1 := openFile(path1)
	defer table1.Close()

	if mode == "many" {
		offset1, _ = table1.Seek(0, io.SeekEnd)
		table1.Seek(0, io.SeekStart)
	} else {
		offset1 = int64(sstable.ReadFileOffset(path1))
	}

	for {
		key1, val1 := sstable.ReadRecord(table1, uint64(offset1))
		logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key1, Value: *val1})

		if val1 == nil {
			break
		}
	}

	return
}

func mergeTwoTablesInMemory(t1, t2 []GTypes.KeyVal[string, database_elem.DatabaseElem]) (logs []GTypes.KeyVal[string, database_elem.DatabaseElem]) {
	i1 := 0
	i2 := 0
	k := 0

	for i1 < len(t1) && i2 < len(t2) {
		k, logs = compareLogs(t1[i1].Key, t2[i2].Key, &t1[i1].Value, &t2[i2].Value, logs)

		//citamo naredne logove
		if k == 1 {
			i1++
		} else if k == 2 {
			i2++
		} else if k == 0 {
			i1++
			i2++
		}
	}
	for ; i1 < len(t1); i1++ {
		logs = append(logs, t1[i1])
	}
	for ; i2 < len(t2); i2++ {
		logs = append(logs, t2[i2])
	}
	return
}

func mergeTwoTables(path1, path2 string, logs []GTypes.KeyVal[string, database_elem.DatabaseElem], mode string) []GTypes.KeyVal[string, database_elem.DatabaseElem] {
	i := 0
	var offset1, offset2 int64
	var k int

	table1 := openFile(path1)
	defer table1.Close()

	table2 := openFile(path2)
	defer table2.Close()

	if mode == "many" {
		offset1, _ = table1.Seek(0, io.SeekEnd)
		offset2, _ = table2.Seek(0, io.SeekEnd)
		table1.Seek(0, io.SeekStart)
		table2.Seek(0, io.SeekStart)
	} else {
		offset1 = int64(sstable.ReadFileOffset(path1))
		offset2 = int64(sstable.ReadFileOffset(path2))
	}

	key1, val1 := sstable.ReadRecord(table1, uint64(offset1))
	if val1 == nil {
		log.Fatal()
	}
	key2, val2 := sstable.ReadRecord(table2, uint64(offset2))
	if val2 == nil {
		log.Fatal()
	}

	for {
		k, logs = compareLogs(key1, key2, val1, val2, logs)

		//citamo naredne logove
		if k == 1 {
			key1, val1 = sstable.ReadRecord(table1, uint64(offset1))
		} else if k == 2 {
			key2, val2 = sstable.ReadRecord(table2, uint64(offset2))
		} else if k == 0 {
			key1, val1 = sstable.ReadRecord(table1, uint64(offset1))
			key2, val2 = sstable.ReadRecord(table2, uint64(offset2))
		}

		if val1 == nil && val2 != nil {
			//ako smo stigli do kraja prve tabele, upisujemo logove iz druge tabele
			logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key2, Value: *val2})
			i++
			logs = finishMerge(table2, offset2, logs)
			break
		} else if val1 != nil && val2 == nil {
			//ako smo stigli do kraja druge tabele, upisujemo logove iz prve tabele
			logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key1, Value: *val1})
			i++
			logs = finishMerge(table1, offset1, logs)
			break
		} else if val1 == nil && val2 == nil {
			//ako smo stigli do kraja obe tabele,kraj fje,
			//mozda bude trebalo nesto za index,summary,toc file
			break
		}

	}
	return logs

}

func finishMerge(table *os.File, offset int64, logs []GTypes.KeyVal[string, database_elem.DatabaseElem]) []GTypes.KeyVal[string, database_elem.DatabaseElem] {

	for {
		key, val := sstable.ReadRecord(table, uint64(offset))
		if val == nil {
			break
		}
		logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: *val})
	}
	return logs
}

func compareLogs(key1, key2 string, val1, val2 *database_elem.DatabaseElem, logs []GTypes.KeyVal[string, database_elem.DatabaseElem]) (int, []GTypes.KeyVal[string, database_elem.DatabaseElem]) {

	if key1 < key2 {
		logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key1, Value: *val1})
		return 1, logs

	} else if key1 > key2 {
		logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key2, Value: *val2})
		return 2, logs

	} else {
		//ako su kljucevi jednaki,gledamo koji log je noviji
		//ako je tombostone!=1 upisujemo ga u novu tabelu
		if val1.Timestamp > val2.Timestamp {

			logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key1, Value: *val1})
			return 0, logs
		} else {

			logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key2, Value: *val2})
			return 0, logs
		}
	}
}

// brise fajlove stare sstabele
func deleteOldFiles(prefix, table string, level int) {
	orderNum := getDataFileOrderNum(table)
	name := prefix + "/usertable-L" + strconv.Itoa(level) + "-" + strconv.Itoa(orderNum) + "-TOC.txt"
	tocFile, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	fileScanner := bufio.NewScanner(tocFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		err = os.Remove(fileScanner.Text())
		if err != nil {
			log.Fatal(err)
		}
	}

	tocFile.Close()

	err = os.Remove(name)
	if err != nil {
		log.Fatal(err)
	}
}

// func NeedsCompaction(level uint64, prefix string, maxTables uint64, maxLevels uint64) (bool, []string) {
// 	if level >= maxLevels {
// 		return false, nil
// 	}

// 	dirs, err := os.ReadDir(prefix)

// 	if os.IsNotExist(err) {
// 		return false, nil
// 	} else {
// 		panic(err)
// 	}

// 	resultingFiles := make([]string, 0)
// 	for _, dir := range dirs {
// 		tokens := strings.Split(dir.Name(), "-")

// 		if tokens[1] == "L"+strconv.Itoa(int(level)) && tokens[3] == "TOC.txt" {
// 			resultingFiles = append(resultingFiles, dir.Name())
// 		}
// 	}

// 	return len(resultingFiles) == int(maxTables), resultingFiles
// }

// func DoCompaction(level uint64, prefix string, maxTables uint64, maxLevels uint64) {
// 	res, files := NeedsCompaction(level, prefix, maxTables, maxLevels)
// 	if !res {
// 		return
// 	}

// 	dataFiles := make([]string, len(files))
// 	for i, tocFile := range files {
// 		file, err := os.Open(tocFile)

// 		if err != nil {
// 			panic(err)
// 		}

// 		dataFile := getDataFile(file)

// 		if dataFile != "" {
// 			dataFiles[i] = dataFile
// 		}

// 		file.Close()
// 	}

// 	newTableNum := getNextTableNum(level+1, prefix)
// 	resFile, err := os.OpenFile(prefix+"usertable-L"+strconv.Itoa(int(level+1))+"-"+strconv.Itoa(int(newTableNum))+"-Data.db", os.O_WRONLY|os.O_CREATE, os.ModePerm)

// 	if err != nil {
// 		panic(err)
// 	}

// 	filePointers := make([]*os.File, len(dataFiles))

// 	for i := range filePointers {
// 		fp, err := os.OpenFile(prefix + dataFiles[i], os.O_RDONLY, os.ModePerm)

// 		if err != nil {
// 			panic(err)
// 		}

// 		filePointers[i] = fp
// 	}

// 	minRecords := make([]generic_types.KeyVal[string, databaseelem.DatabaseElem], len(filePointers))

// 	for _, filePointer := range filePointers {
// 		filePointer.Close()
// 	}
// 	resFile.Close()
// }

// func getMin(records []generic_types.KeyVal[string, databaseelem.DatabaseElem]) []int {
// 	min := 0

// }

// func checkRecords(records []generic_types.KeyVal[string, databaseelem.DatabaseElem]) bool {
// 	for _, rec := range records {
// 		if rec.Key == "" {
// 			return false
// 		}
// 	}

// 	return true
// }

// func getDataFile(file *os.File) string {
// 	scanner := bufio.NewScanner(file)
// 	scanner.Split(bufio.ScanLines)

// 	for scanner.Scan() {
// 		line := scanner.Text()
// 		if strings.Split(line, "-")[3] == "Data.db" {
// 			return line
// 		}
// 	}

// 	return ""
// }

// func getNextTableNum(level uint64, prefix string) uint64 {
// 	dirs, err := os.ReadDir(prefix)

// 	if os.IsNotExist(err) {
// 		return 0
// 	} else {
// 		panic(err)
// 	}

// 	max := 0
// 	for _, dir := range dirs {
// 		tokens := strings.Split(dir.Name(), "-")

// 		if tokens[1] == "L"+strconv.Itoa(int(level)) {
// 			tableNum, err := strconv.Atoi(tokens[2])

// 			if err == nil && tableNum > max {
// 				max = tableNum
// 			}
// 		}
// 	}

// 	return uint64(max + 1)
// }
