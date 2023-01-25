package main

import "time"

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
