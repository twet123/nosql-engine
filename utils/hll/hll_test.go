package hll

import (
	"fmt"
	"testing"
)

func TestHLL(t *testing.T) {
	hll := New(4)

	hll.Add("vlada")
	hll.Add("balsa")
	hll.Add("teodor")
	hll.Add("vlada")
	hll.Add("vlada")
	hll.Add("vlada")
	hll.Add("vlada")

	if hll.Estimate() <= 1 {
		t.Fatalf("HLL failed. " + fmt.Sprintf("%f", hll.Estimate()))
	}

	hll.Add("vlada1")
	hll.Add("teodor1")
	hll.Add("balsa1")
	hll.Add("test")
	hll.Add("test1234")
	hll.Add("abcdabcd")
	hll.Add("12345678")

	if hll.Estimate() <= 1 {
		t.Fatalf("HLL failed. " + fmt.Sprintf("%f", hll.Estimate()))
	}

	serialization := hll.Serialize()
	hll = Deserialize(serialization)

	if hll.Estimate() <= 1 {
		t.Fatalf("HLL serialization failed.")
	}
}
