package hll

import (
	"math"
	"nosql-engine/packages/utils/hash"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HLL struct {
	m   uint64  // size of reg (2^p)
	p   uint8   // precision
	reg []uint8 // reg
}

func New(p uint8) *HLL {
	if p < HLL_MIN_PRECISION || p > HLL_MAX_PRECISION {
		return nil
	}

	m := uint64(math.Exp2(float64(p)))
	reg := make([]uint8, m)

	return &HLL{
		m:   m,
		p:   p,
		reg: reg,
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

// convert a string containing 0 and 1 to integer
func toInt(number string) uint {
	// number to return
	ret := 0
	// multiplier 2^i
	mul := 1

	for i := 0; i < len(number); i++ {
		if number[len(number)-i-1] == '1' {
			ret += mul
		}

		mul *= 2
	}

	return uint(ret)
}

func trailingZeros(hash string) uint {
	cnt := 0

	for i := (len(hash) - 1); i >= 0; i-- {
		if hash[i] == '0' {
			cnt++
		} else {
			return uint(cnt)
		}
	}

	return uint(cnt)
}

func (hll *HLL) Add(key string) {
	bin := hash.ToBinary(hash.GetMD5Hash(key))

	bucket := toInt(bin[:hll.p])
	value := uint8(trailingZeros(bin))

	if value > hll.reg[bucket] {
		hll.reg[bucket] = value
	}
}

func (hll *HLL) Serialize() []byte {
	ret := make([]byte, 1)

	// put p in ret
	ret[0] = hll.p
	// we don't have to put m in the serialized array, because m is 2^p
	// put reg in ret
	ret = append(ret, hll.reg...)

	return ret
}

func Deserialize(byteArr []byte) *HLL {
	// get p
	p := byteArr[0]
	// get m
	m := uint64(math.Exp2(float64(p)))
	// get reg
	reg := make([]byte, m)
	copy(reg, byteArr[1:])

	return &HLL{
		p:   p,
		m:   m,
		reg: reg,
	}
}
