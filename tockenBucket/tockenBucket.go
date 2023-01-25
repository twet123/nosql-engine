package main

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	rateLimit          int64
	maxNumOfTokens     uint64
	currentNumOfTokens uint64
	timestamp          int64
}

func newTokenBucket(rateLimit int64, maxNumOfTokens uint64) *TokenBucket {
	return &TokenBucket{
		rateLimit:          rateLimit,
		maxNumOfTokens:     maxNumOfTokens,
		timestamp:          time.Now().Unix(),
		currentNumOfTokens: uint64(0),
	}
}

func (tb *TokenBucket) isReqValid() bool {
	now := time.Now().Unix()
	timeDifference := now - tb.timestamp

	//provjera da li je prosao odredjeni vremenski period
	//koji predstavlja limit
	if timeDifference > tb.rateLimit {
		//ako jeste resetujemo brojac tokena i timestamp postavljamo na now
		tb.currentNumOfTokens = uint64(0)
		tb.timestamp = now
	} else if tb.currentNumOfTokens >= tb.maxNumOfTokens {
		return false
	}

	tb.currentNumOfTokens++
	return true

}

// niz:  rateLimit-maxNumOfTokens-currentNumOfTokens-timestamp
func (tb *TokenBucket) toBytes() []byte {
	retVal := make([]byte, 32)
	binary.LittleEndian.PutUint64(retVal[0:], uint64(tb.rateLimit))
	binary.LittleEndian.PutUint64(retVal[8:], tb.maxNumOfTokens)
	binary.LittleEndian.PutUint64(retVal[16:], tb.currentNumOfTokens)
	binary.LittleEndian.PutUint64(retVal[24:], uint64(tb.timestamp))
	return retVal
}

func (tb *TokenBucket) fromBytes(bytes []byte) {
	tb.rateLimit = int64(binary.LittleEndian.Uint64(bytes[0:8]))
	tb.maxNumOfTokens = binary.LittleEndian.Uint64(bytes[8:16])
	tb.currentNumOfTokens = binary.LittleEndian.Uint64(bytes[16:24])
	tb.timestamp = int64(binary.LittleEndian.Uint64(bytes[24:32]))

}
