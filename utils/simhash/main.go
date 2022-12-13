package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
)

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

func NapraviMapu(filename string) map[string]int {
	fileContent, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil
	}
	text := string(fileContent)
	var mapa map[string]int = make(map[string]int)
	text = regexp.MustCompile(`[^a-zA-Z0-9 ]+`).ReplaceAllString(text, "")
	res1 := strings.Split(text, " ")
	for _, st := range res1 {
		v, ok := mapa[st]
		if ok {
			mapa[st] = v + 1
		} else {
			mapa[st] = 1
		}
	}
	return mapa
}

func napraviHasheve(mapa map[string]int) []int {
	arr := make([]int, 256, 256)
	for key, element := range mapa {
		s := ToBinary(GetMD5Hash(key))
		i := 0
		for _, c := range s {
			deo := 1
			if c == '0' {
				deo = -1
			}
			arr[i] += deo * element
			i++
		}
	}
	i := 0
	for _, elem := range arr {
		if elem < 0 {
			arr[i] = 0
		} else {
			arr[i] = 1
		}
		i++
	}
	return arr
}

func Hemingvej(arr1, arr2 []int) int {
	brojac := 0
	for i := 0; i < len(arr1); i++ {
		if arr1[i] != arr2[i] {
			brojac++
		}
	}
	return brojac
}

func main() {
	//fmt.Println(GetMD5Hash("hello"))
	fmt.Println(len(ToBinary(GetMD5Hash("bakibb"))))
	mapa1 := NapraviMapu("./tekst1.txt")
	mapa2 := NapraviMapu("./tekst2.txt")
	arr := napraviHasheve(mapa1)
	arr2 := napraviHasheve(mapa2)
	//fmt.Println(arr)
	brojac := Hemingvej(arr, arr2)
	fmt.Println(brojac)
}
