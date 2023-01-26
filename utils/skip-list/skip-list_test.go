package skiplist

import (
	"testing"
)

func TestSkipList(t *testing.T) {
	maxHeight := 32

	skipList := New(maxHeight)

	skipList.Add("vlada", []byte("vladaVal"))
	skipList.Add("balsa", []byte("balsaVal"))
	skipList.Add("teodor", []byte("teodorVal"))

	if skipList.Find("vlada").key != "vlada" {
		t.Fatalf("Skip list failed for key 'vlada'")
	}
	if skipList.Find("balsa").key != "balsa" {
		t.Fatalf("Skip list failed for key 'balsa'")
	}
	if skipList.Find("teodor").key != "teodor" {
		t.Fatalf("Skip list failed for key 'teodor'")
	}
	if skipList.Find("empty") != nil {
		t.Fatalf("Skip list failed for key 'empty'")
	}
}
