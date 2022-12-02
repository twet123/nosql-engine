package bloomfilter

import "testing"

func TestBloomFilter(t *testing.T) {
	bloomFiler := New(50, 0.01)

	bloomFiler.Add("vlada")
	bloomFiler.Add("balsa")
	bloomFiler.Add("teodor")

	if !bloomFiler.Find("vlada") {
		t.Fatalf("Bloom filter failed for key 'vlada'")
	}
	if !bloomFiler.Find("balsa") {
		t.Fatalf("Bloom filter failed for key 'balsa'")
	}
	if !bloomFiler.Find("teodor") {
		t.Fatalf("Bloom filter failed for key 'teodor'")
	}
}
