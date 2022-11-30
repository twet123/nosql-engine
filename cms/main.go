package main

import (
	"fmt"
)

func main() {
	cms := New(0.1, 0.1)
	//fmt.Println(cms.k)
	//fmt.Println(cms.m)
	cms.Add([]byte("hello"))
	cms.Add([]byte("hello"))
	cms.Add([]byte("hello"))
	cms.Add([]byte("hello1"))
	cms.Add([]byte("hi"))
	cms.Add([]byte("hi"))
	fmt.Println(cms.findMin([]byte("hello")))
	fmt.Println(cms.findMin([]byte("hello1")))
	fmt.Println(cms.findMin([]byte("hi")))
	fmt.Println(cms.findMin([]byte("hell")))
	fmt.Println(cms.bitMatrix)
}
