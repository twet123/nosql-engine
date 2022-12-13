package main

import "fmt"

func main() {
	m := CalculateM(0.1)
	k := CalculateK(0.1)
	mat := make([][]uint64, k)
	for i := range mat {
		mat[i] = make([]uint64, m)
	}
	fns := CreateHashFunctions(k)
	c := Cms{m, k, mat, fns}
	c.Insert("Balsa")
	c.Insert("Balsa")
	c.Insert("Balsa")
	c.Insert("Balsa")
	c.Insert("Balsa")
	c.Insert("Balsa")
	fmt.Println(c.mat)
}
