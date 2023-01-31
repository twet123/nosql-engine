package cache

import (
	"strconv"
	"testing"
)

func SlicesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func TestCache(t *testing.T) {
	cache := New[int, []byte](5)
	cache.Refer(5, []byte("balsa"))
	cache.Refer(6, []byte("teodor"))
	cache.Refer(7, []byte("vlada"))
	cache.Refer(8, []byte("danilo"))

	if !SlicesEqual(cache.Refer(7, nil), []byte("vlada")) {
		t.Fatalf("Cache doesn't contain good value for key 7")
	}

	cache.Refer(9, []byte("sasa"))

	if cache.Contains(15) {
		t.Fatalf("Failed, key 15 should not exists")
	}

	// res := cache.Refer(15, nil)
	// fmt.Println(res)

	for i := 5; i < 10; i++ {
		if !cache.Contains(i) {
			t.Fatalf("Cache failed for key " + strconv.Itoa(i))
		}
	}
}
