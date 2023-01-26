package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

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

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ////////////// strukture ///////////////////////
type WAL struct {
	// KVStore []*WALEntry
	path             string
	currentSegment   string
	numberOfSegments int
	lowWaterMark     uint32
	segmentCapacity  uint32
	numberOfEntries  uint32
}
type WALEntry struct {
	CRC       uint32
	timestamp uint64
	tombstone byte
	keySize   uint64
	valueSize uint64
	key       string
	value     []byte
}

// /////////////////////////////////////////////////

//funckije vezane za WALEntry strukturu////////////

func NewEntry(key string, value []byte, tombstone byte) *WALEntry {
	crc32 := CRC32((value))
	timestamp := time.Now().Unix()
	keySize := uint64(len([]byte(key)))
	valueSize := uint64(len(value))
	return &WALEntry{crc32, uint64(timestamp), tombstone, keySize, valueSize, key, value}
}

func (entry *WALEntry) Encode() []byte {
	crc32 := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(crc32, entry.CRC)

	timestamp := make([]byte, TIMESTAMP_SIZE)
	binary.LittleEndian.PutUint64(timestamp, entry.timestamp)

	tombstone := []byte{0}
	if entry.tombstone == 1 {
		tombstone[0] = 1
	}

	keySize := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(keySize, entry.keySize)

	valueSize := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(valueSize, entry.valueSize)

	recordList := make([]byte, 0, CRC_SIZE+TIMESTAMP_SIZE+TOMBSTONE_SIZE+KEY_SIZE_SIZE+VALUE_SIZE_SIZE+entry.keySize+entry.valueSize)
	recordList = append(recordList, crc32...)
	recordList = append(recordList, timestamp...)
	recordList = append(recordList, tombstone...)
	recordList = append(recordList, keySize...)
	recordList = append(recordList, valueSize...)
	recordList = append(recordList, []byte(entry.key)...)
	recordList = append(recordList, entry.value...)

	return recordList
}

func Decode(reader *bufio.Reader) (error, WALEntry) {
	entry := WALEntry{}
	err := binary.Read(reader, binary.LittleEndian, &entry.CRC)
	if err != nil {
		return err, entry
	}

	err = binary.Read(reader, binary.LittleEndian, &entry.timestamp)
	if err != nil {
		return err, entry
	}

	err = binary.Read(reader, binary.LittleEndian, &entry.tombstone)
	if err != nil {
		return err, entry
	}

	err = binary.Read(reader, binary.LittleEndian, &entry.keySize)
	if err != nil {
		return err, entry
	}

	err = binary.Read(reader, binary.LittleEndian, &entry.valueSize)
	if err != nil {
		return err, entry
	}

	key := make([]byte, entry.keySize)
	err = binary.Read(reader, binary.LittleEndian, &key)
	if err != nil {
		return err, entry
	}

	entry.key = string(key)

	value := make([]byte, entry.valueSize)
	err = binary.Read(reader, binary.LittleEndian, &value)
	if err != nil {
		return err, entry
	}
	entry.value = value

	return nil, entry

}

/////////////////////////////////////////////////////

// funkcije vezane za WAL strukturu///////////////////
func NewWAL(Path string, Capacity uint32, lwm uint32) *WAL {
	wal := &WAL{path: Path, segmentCapacity: Capacity, lowWaterMark: lwm}
	segments, err := ioutil.ReadDir(Path)
	if err != nil {
		panic(err)
	}

	wal.numberOfSegments = len(segments)
	if wal.numberOfSegments == 0 {
		wal.newSegment()
	} else {
		wal.currentSegment = Path + segments[wal.numberOfSegments-1].Name()
		wal.numberOfEntries = wal.getTotalEntries()

		if wal.numberOfEntries >= wal.segmentCapacity {
			wal.newSegment()
		}
	}

	return wal

}

func (w *WAL) getTotalEntries() uint32 {
	f, err := os.OpenFile(w.currentSegment, os.O_RDONLY, 0777)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	number := uint32(0)

	for {
		err, _ := Decode(reader)
		if err == nil {
			number++
		} else {
			break
		}
	}

	return number

}

func (w *WAL) newSegment() {
	newFile, err := os.Create(w.path + "log_" + strconv.Itoa(w.numberOfSegments+1) + ".bin")
	if err != nil {
		panic(err)
	}

	w.currentSegment = newFile.Name()
	w.numberOfEntries = 0
	w.numberOfSegments++
	newFile.Close()
}

func (w *WAL) putEntry(key string, value []byte, tombstone byte) bool {
	entry := NewEntry(key, value, tombstone)
	encodedEntry := entry.Encode()

	if w.numberOfEntries >= w.segmentCapacity {
		w.newSegment()
	}

	f, err := os.OpenFile(w.currentSegment, os.O_APPEND, 0777)
	if err != nil {
		return false
	}

	err = binary.Write(f, binary.LittleEndian, encodedEntry)
	if err != nil {
		return false
	}
	f.Close()
	w.numberOfEntries++
	return true

}

func (w *WAL) removeOldSegments() {
	segments, err := ioutil.ReadDir(w.path)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(segments))
	for i := 0; i < len(segments); i++ {
		if uint32(i) < w.lowWaterMark {
			fmt.Println(i)
			fmt.Println(w.path + segments[i].Name())
			err = os.Remove(w.path + segments[i].Name())
			if err != nil {
				panic(err)
			}
			w.numberOfSegments--

		} else {
			err := os.Rename(w.path+segments[i].Name(), w.path+"log_"+strconv.Itoa(i-int(w.lowWaterMark)+1)+".bin")
			if err != nil {
				panic(err)
			}
			w.currentSegment = w.path + "log_" + strconv.Itoa(i-int(w.lowWaterMark)+1) + ".bin"
		}
	}
	w.numberOfEntries = w.getTotalEntries()
}

func (w *WAL) emptyWAL() {
	segments, err := ioutil.ReadDir(w.path)
	if err != nil {
		panic(err)
	}
	//fmt.Println(len(segments))
	for i := 0; i < len(segments); i++ {
		//fmt.Println(w.path + segments[i].Name())
		err := os.Remove(w.path + segments[i].Name())
		if err != nil {
			panic(err)
		}
	}
	w.numberOfSegments = 0
	w.newSegment()
}
