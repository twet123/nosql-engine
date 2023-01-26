package main

import (
	"fmt"
)

func main() {
	//fns := CreateHashFunctions(5)

	//buf := &bytes.Buffer{}
	//encoder := gob.NewEncoder(buf)
	//decoder := gob.NewDecoder(buf)

	//for _, fn := range fns {
	//data := []byte("hello")
	//fmt.Println(fn.Hash(data))
	//err := encoder.Encode(fn)
	//if err != nil {
	//panic(err)
	//}
	//dfn := &HashWithSeed{}
	//err = decoder.Decode(dfn)
	//if err != nil {
	//panic(err)
	//}
	//fmt.Println(dfn.Hash(data))
	//}

	bf := New(1024)
	//fmt.Println(bf.m)
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))
	bf.Add([]byte("sir"))
	bf.Add([]byte("madam"))
	bf.Add([]byte("io"))

	fmt.Println(bf.isThere([]byte("hello")))
	fmt.Println(bf.isThere([]byte("hi")))

}
