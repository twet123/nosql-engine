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
			merged = mergeTwoTablesInMemory(merged, readWholeTable(dirPath+"/"+tables[i], config.SSTableFiles))
			deleteOldFiles(dirPath, tables[i], level)
		}
		for i := 0; i < len(nextTables); i++ {
			merged = mergeTwoTablesInMemory(merged, readWholeTable(dirPath+"/"+nextTables[i], config.SSTableFiles))
			deleteOldFiles(dirPath, nextTables[i], level+1)
		}
		n := uint64(len(merged)) / config.SSTableSize
		if uint64(len(merged))%config.SSTableSize > 0 {
			n += 1
		}
		for i := uint64(0); i < n; i++ {
			from := i * config.SSTableSize
			to := (i + 1) * config.SSTableSize
			if to > uint64(len(merged)) {
				to = uint64(len(merged))
				if from >= to {
					break
				}
			}
			sstable.CreateSStable(merged[from:to], int(config.SummaryCount), dirPath, level+1, config.SSTableFiles)
		}
	} else {
		for i := 0; i < len(tables); i++ {
			merged := readWholeTable(dirPath+"/"+tables[i], config.SSTableFiles)
			deleteOldFiles(dirPath, tables[i], level)
			files, err = ioutil.ReadDir(dirPath)
			if err != nil {
				panic(err)
			}
			nextTables = levelRangeFilter(dirPath, files, strconv.Itoa(level+1),
				merged[0].Key, merged[len(merged)-1].Key, config.SSTableFiles)

			for j := 0; j < len(nextTables); j++ {
				merged = mergeTwoTablesInMemory(merged, readWholeTable(dirPath+"/"+nextTables[j], config.SSTableFiles))
				deleteOldFiles(dirPath, nextTables[j], level+1)
			}
			n := uint64(len(merged)) / config.SSTableSize
			if uint64(len(merged))%config.SSTableSize > 0 {
				n += 1
			}
			for j := uint64(0); j < n; j++ {
				from := j * config.SSTableSize
				to := (j + 1) * config.SSTableSize
				if to > uint64(len(merged)) {
					to = uint64(len(merged))
					if from >= to {
						break
					}
				}
				sstable.CreateSStable(merged[from:to], int(config.SummaryCount), dirPath, level+1, config.SSTableFiles)
			}
		}
	}

	if level+1 < len(config.LsmLeveledComp)-1 {
		LeveledCompaction(level+1, dirPath)
	}
}

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

func levelRangeFilter(dir string, tables []fs.FileInfo, level string, min, max string, mode string) []string {
	var retList []string
	for _, table := range tables {
		var s string = table.Name()
		tableLvl := strings.Split(s, "-")[1]
		if mode == "one" {
			if tableLvl != ("L"+level) || !strings.Contains(s, "Data.db") {
				continue
			}
		}
		if mode == "many" {
			if tableLvl != ("L"+level) || !strings.Contains(s, "Summary.db") {
				continue
			}
		}
		min1, max1 := sstable.GetKeyRange(dir+"/"+s, mode)
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

func NeedsCompactionLeveled(level int, files []fs.FileInfo) bool {
	config := config2.GetConfig()
	maxPerLevel := config.LsmLeveledComp[level]
	tables := levelFilter(files, strconv.Itoa(level))
	return len(tables) > int(maxPerLevel)
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
		if val1 == nil {
			break
		}
		logs = append(logs, GTypes.KeyVal[string, database_elem.DatabaseElem]{Key: key1, Value: *val1})
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
