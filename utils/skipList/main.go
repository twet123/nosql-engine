package main

import "fmt"

func main() {
	var list = New()

	list.insert(1)
	list.insert(5)
	list.insert(-2)
	list.insert(12)
	list.insert(6)
	list.insert(3)
	list.insert(13)

	fmt.Println(list.query(5))
	fmt.Println(list.query(0))
	fmt.Println(list.query(13))
	fmt.Println(list.query(-2))
	fmt.Println(list.query(100))

	list.delete(1)
	list.delete(5)
	fmt.Println(list.delete(0))

	fmt.Println(list.query(5))
	fmt.Println(list.query(1))
	fmt.Println(list.query(13))

}
