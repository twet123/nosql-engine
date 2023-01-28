package simhash

import (
	"nosql-engine/packages/utils/hash"
	"strings"
)

type SimHash struct {
	bits uint
}

func New(bits uint) *SimHash {
	return &SimHash{
		bits: bits,
	}
}

func getWeightMap(tokens []string) map[string]int {
	weightMap := make(map[string]int)

	for _, token := range tokens {
		_, check := weightMap[token]

		if !check {
			weightMap[token] = 0
		} else {
			weightMap[token]++
		}
	}

	return weightMap
}

func (sh *SimHash) getFingerprint(tokens []string, weights map[string]int) string {
	sum := make([]int, sh.bits)

	for _, token := range tokens {
		tokenBin := hash.ToBinary(hash.GetMD5Hash(token))

		for i := 0; i < int(sh.bits); i++ {
			if tokenBin[i] == '1' {
				sum[i] += weights[token]
			} else {
				sum[i] -= weights[token]
			}
		}
	}

	fingerprint := ""
	for i := 0; i < int(sh.bits); i++ {
		if sum[i] > 0 {
			fingerprint += "1"
		} else {
			fingerprint += "0"
		}
	}

	return fingerprint
}

func (sh *SimHash) hammingDistance(bits1 string, bits2 string) uint {
	ans := 0

	// doing XOR manually, because bits are in a string
	for i := 0; i < int(sh.bits); i++ {
		if bits1[i] != bits2[i] {
			ans++
		}
	}

	return uint(ans)
}

func (sh *SimHash) Compare(data1 string, data2 string) uint {
	// lowercasing all the letters
	data1 = strings.ToLower(data1)
	data2 = strings.ToLower(data2)

	// removing unnecessary characters
	replacer := strings.NewReplacer(",", "", ".", "", "?", "", "!", "")
	data1 = replacer.Replace(data1)
	data2 = replacer.Replace(data2)

	// tokenizing
	tokens1 := strings.Split(data1, " ")
	tokens2 := strings.Split(data2, " ")

	// getting weights
	weights1 := getWeightMap(tokens1)
	weights2 := getWeightMap(tokens2)

	// getting fingerprints
	fingerprint1 := sh.getFingerprint(tokens1, weights1)
	fingerprint2 := sh.getFingerprint(tokens2, weights2)

	return sh.hammingDistance(fingerprint1, fingerprint2)
}
