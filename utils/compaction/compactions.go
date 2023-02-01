package compaction

import (
	"bufio"
	"encoding/binary"
	"io/fs"
	"io/ioutil"
	config2 "nosql-engine/packages/utils/config"
	db "nosql-engine/packages/utils/database"
	database_elem "nosql-engine/packages/utils/database-elem"
	GTypes "nosql-engine/packages/utils/generic-types"
	"nosql-engine/packages/utils/sstable"
	"os"
	"strconv"
	"strings"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
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

type LSM struct {
	maxLevel int
	dirPath  string
}

func NewLSM(maxLevel int, dirPath string) *LSM {
	return &LSM{maxLevel: maxLevel,
		dirPath: dirPath}
}

func openFile(filepath string) *os.File {
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0700)
	if err != nil {
		panic(err)
	}
	return file
}

func (lsm *LSM) CompactionST() {
	config := config2.GetConfig()

	for i := 0; i < int(config.LsmLevels)-1; i++ {
		files, err := ioutil.ReadDir(lsm.dirPath)
		if err != nil {
			panic(err)
		}

		tables := levelFilter(files, strconv.Itoa(i))

		x := 0
		y := 1
		for j := 0; j < (len(tables) / 2); j++ {
			orderNum := getDataFileOrderNum(tables[x])
			filepath1 := "/usertable-L" + strconv.Itoa(i) + "-" + strconv.Itoa(orderNum) + "-Data.db"
			orderNum = getDataFileOrderNum(tables[y])
			filepath2 := "/usertable-L" + strconv.Itoa(i) + "-" + strconv.Itoa(orderNum) + "-Data.db"

			mergeTables(filepath1, filepath2, i+1)

			deleteOldFiles(tables[x], i)
			deleteOldFiles(tables[y], i)
			x += 2
			y += 2
		}
	}

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

func mergeTables(filepath1, filepath2 string, level int) {
	config := config2.GetConfig()
	count := config.SummaryCount

	table1 := openFile(filepath1)
	defer table1.Close()

	table2 := openFile(filepath2)
	defer table2.Close()

	reader1 := bufio.NewReader(table1)
	reader2 := bufio.NewReader(table2)

	logs := make([]DataStructure, 0) //not final solution
	mergeTwoTables(reader1, reader2, logs)

	list := convertList(logs)
	sstable.CreateSStable(list, count, "files", level)

}

func mergeTwoTables(reader1, reader2 *bufio.Reader, logs []DataStructure) {
	i := 0
	var end bool = false
	var err1, err2 error

	e1, err := readLog(reader1)
	if err != nil {
		panic(err)
	}
	e2, err := readLog(reader2)
	if err != nil {
		panic(err)
	}

	for !end {
		if e1.key < e2.key {
			//writeLog(e1)
			logs = append(logs, e1)
			i++

			e1, err = readLog(reader1)

			//ako je doslo do kraja prve sstabele
			if err != nil {
				//writeLog(e2)
				logs = append(logs, e2)
				i++

				finishMerge(reader2, logs)
				break

			}
		} else if e1.key > e2.key {
			//writeLog(e2)
			logs = append(logs, e2)
			i++

			e2, err = readLog(reader2)
			//ako je doslo do kraja druge sstabele
			if err != nil {
				//writeLog(e1)
				logs = append(logs, e1)
				i++

				finishMerge(reader1, logs)
				break
			}
		} else {
			if e1.timestamp > e2.timestamp {
				if e1.tombstone == byte(0) {
					//writeLog(e1)
					logs = append(logs, e1)
					i++
				}
			} else {
				if e2.tombstone == byte(0) {
					//writeLog(e2)
					logs = append(logs, e2)
					i++
				}
			}
		}

		//citamo naredne logove iz obe tabele
		e1, err1 = readLog(reader1)
		e2, err2 = readLog(reader2)

		if err1 != nil && err2 == nil {
			//ako smo stigli do kraja prve tabele, upisujemo logove iz druge tabele
			logs = append(logs, e2)
			i++
			finishMerge(reader2, logs)
			break
		} else if err1 == nil && err2 != nil {
			//ako smo stigli do kraja druge tabele, upisujemo logove iz prve tabele
			logs = append(logs, e1)
			i++
			finishMerge(reader1, logs)
			break
		} else if err1 == nil && err2 == nil {
			//ako smo stigli do kraja obe tabele,kraj fje,
			//mozda bude trebalo nesto za index,summary,toc file
			break
		}

	}

}

func finishMerge(reader *bufio.Reader, logs []DataStructure) {

	for {
		e, err := readLog(reader)
		if err != nil {
			break
		}
		logs = append(logs, e)
	}
}

func readLog(reader *bufio.Reader) (DataStructure, error) {
	e := DataStructure{}

	err := binary.Read(reader, binary.LittleEndian, &e.CRC)
	if err != nil {
		return e, err
	}

	err = binary.Read(reader, binary.LittleEndian, &e.timestamp)
	if err != nil {
		return e, err
	}

	err = binary.Read(reader, binary.LittleEndian, &e.tombstone)
	if err != nil {
		return e, err
	}

	err = binary.Read(reader, binary.LittleEndian, &e.keySize)
	if err != nil {
		return e, err
	}

	err = binary.Read(reader, binary.LittleEndian, &e.valueSize)
	if err != nil {
		return e, err
	}

	key := make([]byte, e.keySize)
	err = binary.Read(reader, binary.LittleEndian, &key)
	if err != nil {
		return e, err
	}

	e.key = string(key)

	value := make([]byte, e.valueSize)
	err = binary.Read(reader, binary.LittleEndian, &value)
	if err != nil {
		return e, err
	}
	e.value = value

	return e, nil
}

func writeLog(e DataStructure) {
	//nije dovrseno, fale pokazivaci na fajlove koji nastanu prilikom kreiranja sstabele
	crc32 := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crc32, e.CRC)

	timestamp := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestamp, e.timestamp)

	tombstone := []byte{0}
	if e.tombstone == 1 {
		tombstone[0] = 1
	}

	keySize := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySize, e.keySize)

	valueSize := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(valueSize, e.valueSize)

	recordList := make([]byte, 0, CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+e.keySize+e.valueSize)
	recordList = append(recordList, crc32...)
	recordList = append(recordList, timestamp...)
	recordList = append(recordList, tombstone...)
	recordList = append(recordList, keySize...)
	recordList = append(recordList, valueSize...)
	recordList = append(recordList, []byte(e.key)...)
	recordList = append(recordList, e.value...)

}

func convertList(list []DataStructure) []GTypes.KeyVal[string, database_elem.DatabaseElem] {
	dbelems := make([]GTypes.KeyVal[string, db.DatabaseElem], 0)
	for i := 0; i < len(list); i++ {
		val := db.DatabaseElem{Tombstone: list[i].tombstone, Value: list[i].value, Timestamp: list[i].timestamp}
		dbelems = append(dbelems, GTypes.KeyVal[string, db.DatabaseElem]{Key: list[i].key, Value: val})
	}
	return dbelems
}

func deleteOldFiles(table string, level int) {
	var orderNum int
	orderNum = getDataFileOrderNum(table)
	name := "/usertable-L" + strconv.Itoa(level) + "-" + strconv.Itoa(orderNum) + "-"
	err := os.Remove(name + "Filter.db")
	if err != nil {
		panic(err)
	}
	err = os.Remove(name + "Index.db")
	if err != nil {
		panic(err)
	}
	err = os.Remove(name + "Summary.db")
	if err != nil {
		panic(err)
	}
	err = os.Remove(name + "Data.db")
	if err != nil {
		panic(err)
	}
	err = os.Remove(name + "Metadata.db")
	if err != nil {
		panic(err)
	}
	err = os.Remove(name + "TOC.txt")
	if err != nil {
		panic(err)
	}

}
