package tokenbucket

import (
	"encoding/binary"
	"time"
)

type TokenBucket struct {
	tokens      uint64
	lrTimestamp uint64 // last reset
}

func New(tokens uint64) *TokenBucket {
	return &TokenBucket{
		tokens:      tokens,
		lrTimestamp: uint64(time.Now().Unix()),
	}
}

func (tb *TokenBucket) Refresh(maxTokens uint64) {
	tb.tokens = maxTokens
	tb.lrTimestamp = uint64(time.Now().Unix())
}

func (tb *TokenBucket) Check(maxTokens uint64, timeOffset uint64) bool {
	if tb.tokens > 0 {
		tb.tokens--
		return true
	} else if tb.lrTimestamp+timeOffset <= uint64(time.Now().Unix()) {
		tb.tokens = maxTokens - 1
		tb.lrTimestamp = uint64(time.Now().Unix())
		return true
	}

	return false
}

func (tb *TokenBucket) Serialize() []byte {
	ret := make([]byte, 0)

	ret = binary.BigEndian.AppendUint64(ret, tb.tokens)
	ret = binary.BigEndian.AppendUint64(ret, tb.lrTimestamp)

	return ret
}

func Deserialize(byteArr []byte) *TokenBucket {
	tokens := binary.BigEndian.Uint64(byteArr[0:8])
	timestamp := binary.BigEndian.Uint64(byteArr[8:])

	return &TokenBucket{
		tokens:      tokens,
		lrTimestamp: timestamp,
	}
}
