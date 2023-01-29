package database

type DatabaseElem struct {
	Tombstone byte
	Value     []byte
	Timestamp uint64
}

// read/write path i metode
