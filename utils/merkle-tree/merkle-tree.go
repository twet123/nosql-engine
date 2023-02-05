package merkletree

import (
	"crypto/sha1"
	"encoding/hex"
	"math"
)

type MerkleRoot struct {
	root *Node
}

func New(bytes [][]byte) *MerkleRoot {
	fileNum := len(bytes)

	if fileNum == 1 {
		tempHash := hash(bytes[0])
		rootNode := Node{data: tempHash[:], left: nil, right: nil}
		return &MerkleRoot{root: &rootNode}
	}

	// first level of nodes
	nodeLevel := make([]Node, fileNum)

	for i := 0; i < len(bytes); i++ {
		tempHash := hash(bytes[i])
		nodeLevel[i] = Node{data: tempHash[:], left: nil, right: nil}
	}

	// checking if a number is a power of two by binary operators
	if (fileNum & (fileNum - 1)) != 0 {
		nearestPower := math.Ceil(math.Log2(float64(fileNum)))
		emptyFileNum := math.Pow(2, nearestPower) - float64(fileNum)

		for i := 0; i < int(emptyFileNum); i++ {
			tempHash := hash([]byte{})
			nodeLevel = append(nodeLevel, Node{data: tempHash[:], left: nil, right: nil})
		}
	}

	nextNodeLevel := make([]Node, int(len(nodeLevel)/2))

	for len(nextNodeLevel) != 1 {
		for i := 0; i < len(nodeLevel); i += 2 {
			tempHash := hash(append(nodeLevel[i].data[:], nodeLevel[i+1].data...))
			nextNodeLevel[i/2] = Node{data: tempHash[:], left: &nodeLevel[i], right: &nodeLevel[i+1]}
		}

		nodeLevel = nextNodeLevel
		nextNodeLevel = make([]Node, int(len(nodeLevel)/2))
	}

	// calculating merkleroot, previous loop calcualted everything up to it
	tempHash := hash(append(nodeLevel[0].data[:], nodeLevel[1].data...))
	nextNodeLevel[0] = Node{data: tempHash[:], left: &nodeLevel[0], right: &nodeLevel[1]}

	return &MerkleRoot{
		root: &nextNodeLevel[0],
	}
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  []byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func hash(data []byte) [20]byte {
	return sha1.Sum(data)
}
