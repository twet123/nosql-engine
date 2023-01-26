package simhash

import (
	"bufio"
	"os"
	"testing"
)

func readFile(path string) string {
	dat, err := os.Open(path)

	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(dat)
	scanner.Split(bufio.ScanLines)

	fileContent := ""

	for scanner.Scan() {
		fileContent += scanner.Text()
	}

	dat.Close()
	return fileContent
}

func TestSimHash(t *testing.T) {
	bits := 8
	simHash := New(uint(bits))
	file1 := readFile("./tekst1.txt")
	file2 := readFile("./tekst2.txt")

	result := simHash.Compare(file1, file2)

	if result <= 0 {
		t.Fatalf("Impossible result, 8 bits")
	}

	bits = 32
	simHash = New(uint(bits))

	result = simHash.Compare(file1, file2)

	if result <= 0 {
		t.Fatalf("Impossible result, 32 bits")
	}
}
