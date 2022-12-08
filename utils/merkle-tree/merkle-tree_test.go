package merkletree

import (
	"strconv"
	"testing"
)

func TestMerkleTree(t *testing.T) {
	data := make([][]byte, 8)

	for i := 0; i < len(data); i++ {
		tempString := "string" + strconv.Itoa(i)

		data[i] = []byte(tempString)
	}

	// same input data, root hash should be the same
	mkTest1 := New(data)
	mkTest2 := New(data)

	if mkTest1.String() != mkTest2.String() {
		t.Fatalf("Merkle tree root hash different for same data")
	}

	// changing input data
	tempString := "string"
	data[0] = []byte(tempString)

	mkTest3 := New(data)

	if mkTest1.String() == mkTest3.String() {
		t.Fatalf("Merkle tree root hash same for different data")
	}

	// changing input data not to be of length 2^x, dummy data should be added
	data = make([][]byte, 6)

	for i := 0; i < len(data); i++ {
		tempString := "string" + strconv.Itoa(i)

		data[i] = []byte(tempString)
	}

	mkTest4 := New(data)

	if mkTest3.String() == mkTest4.String() {
		t.Fatalf("Merkle tree failed for dummy data insertion")
	}
}
