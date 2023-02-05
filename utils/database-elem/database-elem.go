package databaseelem

type DatabaseElem struct {
	Tombstone byte
	Value     []byte
	Timestamp uint64
}
