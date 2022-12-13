package main

import (
	"math/rand"
	"testing"
)

func TestSkipList_Insert_Delete_Big_Data(t *testing.T) {
	sl := makeSkipList(15)
	niz := make([]int, 0)
	for i := 0; i < 100; i++ {
		a := int(rand.Int31n(2000))
		niz = append(niz, a)
		sl.Insert(a)
	}

	for i := 0; i < len(niz); i++ {
		sl.Delete(niz[i])
	}
	if sl.size != 0 {
		t.Fatalf("Failed delete or insert")
	}
}

func TestSkipList_Search(t *testing.T) {
	sl := makeSkipList(15)
	if sl.Search(15) {
		t.Fatalf("Search failed")
	}

	sl.Insert(15)

	if !sl.Search(15) {
		t.Fatalf("Search failed")
	}
}
