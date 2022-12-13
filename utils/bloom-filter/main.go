package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func makeFile(bf BloomFilter) {
	file, err := os.Create("bloom.bin")
	if err != nil {
		panic(err)
	}
	a := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(a, uint32(bf.m))
	file.Write(a)
	binary.LittleEndian.PutUint32(a, uint32(bf.k))
	file.Write(a)
	file.Write(bf.niz)
	for _, fn := range bf.fns {
		binary.LittleEndian.PutUint32(a, uint32(len(fn.Seed)))
		file.Write(a)
		file.Write(fn.Seed)
	}
	file.Close()
}

func ReadFile() BloomFilter {
	file, err := os.Open("bloom.bin")
	if err != nil {
		panic(err)
	}
	bytes := make([]byte, 4)
	_, err = file.Read(bytes)
	m := binary.LittleEndian.Uint32(bytes)

	bytes = make([]byte, 4)
	_, err = file.Read(bytes)
	k := binary.LittleEndian.Uint32(bytes)

	niz := make([]byte, m)
	file.Read(niz)

	fns := make([]HashWithSeed, k)
	for i := 0; uint32(i) < k; i++ {
		bytes = make([]byte, 4)
		_, err = file.Read(bytes)
		duzina := binary.LittleEndian.Uint32(bytes)

		bytes = make([]byte, duzina)
		_, err = file.Read(bytes)
		fns[i] = HashWithSeed{Seed: bytes}
	}

	return BloomFilter{k: uint(k), m: uint(m), niz: niz, fns: fns}
}

func main() {
	/*m := CalculateM(100, 0.02)
	k := CalculateK(100, m)
	fns := CreateHashFunctions(k)
	niz := make([]byte, m, m)
	bf := BloomFilter{k: k, m: m, niz: niz, fns: fns}

	bf.Insert("djsakldad")
	bf.Insert("Balsa")

	tmp := bf.Search("djjaja")
	fmt.Println(tmp)
	tmp = bf.Search("Balsa")
	fmt.Println(tmp)

	fmt.Println(bf.niz)
	makeFile(bf)*/
	bf := ReadFile()

	tmp := bf.Search("djjaja")
	fmt.Println(tmp)
	tmp = bf.Search("Balsa")
	fmt.Println(tmp)

	//fmt.Println(bf.niz)
	//makeFile(bf)

}
