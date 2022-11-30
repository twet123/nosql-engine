package main

import "fmt"

type Interface interface {
	simHashTokens(tokens []string)
}

type SimHash32 struct {
	hashbits uint
	hash     []int
}

func New(tokens []string, hashbitsValue uint) *SimHash32 {
	if hashbitsValue != 32 && hashbitsValue != 64 && hashbitsValue != 128 {
		fmt.Println("hashbits vrednost mora biti 32,64 ili 128")
		return nil
	}

	return &SimHash32{
		hashbits: hashbitsValue * 8,
	}
}

func (sh *SimHash32) simHashTokens(tokens []string, weight map[string]int) []int {
	bits := make([]int, sh.hashbits)
	for _, t := range tokens {
		v := ToBinary(GetMD5Hash(t))
		for i := sh.hashbits; i >= 1; i-- {
			if v[sh.hashbits-i] == '1' {
				bits[i-1] = bits[i-1] + weight[t]
			} else {
				bits[i-1] = bits[i-1] - weight[t]
			}
		}

	}

	for i := sh.hashbits; i >= 1; i-- {
		if bits[i-1] > 0 {
			bits[i-1] = 1
		} else {
			bits[i-1] = 0
		}
	}

	//fmt.Println(bits)
	//hash := uint64(0)
	//one := uint64(1)
	//for i := sh.hashbits; i > 1; i-- {
	//if bits[i-1] > 1 {
	//hash |= one
	//}
	//one = one << 1
	//}

	//sh.hash = hash
	sh.hash = bits
	return bits

}

func (sh *SimHash32) hammingDistance(hash1 []int, hash2 []int) uint64 {
	//hash1 ^= hash2
	var ans uint64 = 0

	for i, _ := range hash1 {
		hash1[i] ^= hash2[i]
		if hash1[i] == 1 {
			ans += 1
		}
	}
	//for hash1 > 0 {
	//ans += 1
	//hash1 &= hash1 - 1
	//}
	return ans
}
