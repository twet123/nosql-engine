package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func readFile(path string) []string {
	dat, err := os.Open(path)
	check(err)

	scanner := bufio.NewScanner(dat)
	scanner.Split(bufio.ScanLines)
	var txtlines []string

	for scanner.Scan() {
		txtlines = append(txtlines, scanner.Text())
	}
	dat.Close()
	return txtlines
}

func parseString(input string, words *[]string) {
	wordBreakDown := strings.Fields(input)
	for _, value := range wordBreakDown {
		//fmt.Println(value)
		*words = append(*words, value)
	}
}

func countWeight(weight map[string]int, words *[]string) {

	for _, value := range *words {
		if _, ok := weight[value]; ok {
			weight[value]++
		}
		if _, ok := weight[value]; !ok {
			weight[value] = 1
		}
	}
}

func main() {
	input := readFile("tekst1.txt")
	var words []string // lista parsiranih reci
	for _, value := range input {
		parseString(value, &words)
	}

	weight := make(map[string]int)
	countWeight(weight, &words)

	sh := New(words, 32)
	sh.simHashTokens(words, weight)
	fmt.Println(sh.hash)

	input1 := readFile("tekst2.txt")
	var words1 []string // lista parsiranih reci
	for _, value := range input1 {
		parseString(value, &words1)
	}

	weight1 := make(map[string]int)
	countWeight(weight1, &words1)

	sh1 := New(words1, 32)
	sh1.simHashTokens(words1, weight1)
	fmt.Println(sh1.hash)

	fmt.Println(sh1.hammingDistance(sh.hash, sh1.hash))

	words2 := []string{"abccsd"}
	words3 := []string{"mnmnmbv"}
	weight2 := make(map[string]int)
	countWeight(weight2, &words2)
	weight3 := make(map[string]int)
	countWeight(weight3, &words3)

	sh.simHashTokens(words2, weight2)
	fmt.Println(sh.hash)
	sh1.simHashTokens(words3, weight3)
	fmt.Println(sh1.hash)

	fmt.Println(sh1.hammingDistance(sh.hash, sh1.hash))

	//fmt.Println(words)
	//fmt.Println(GetMD5Hash("hello"))
	//fmt.Println(ToBinary(GetMD5Hash("hello")))
}
