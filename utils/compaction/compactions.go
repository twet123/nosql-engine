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
	if len(tables) > int(maxPerLevel) {
		return true
	}
	return false
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
	return 0, logs
}

// brise fajlove stare sstabele
func deleteOldFiles(prefix, table string, level int) {
	var orderNum int
	orderNum = getDataFileOrderNum(table)
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
