package cms

import "testing"

func TestCountMinSketch(t *testing.T) {
	countMinSketch := New(0.1, 0.01)

	countMinSketch.Add("vlada")
	countMinSketch.Add("vlada")
	countMinSketch.Add("vlada")

	countMinSketch.Add("balsa")
	countMinSketch.Add("balsa")

	if countMinSketch.CountMin("vlada") < 3 {
		t.Fatalf("CountMinSketch failed for key 'vlada'")
	}
	if countMinSketch.CountMin("balsa") < 2 {
		t.Fatalf("CountMinSketch failed for key 'balsa'")
	}
	if countMinSketch.CountMin("teodor") > 0 {
		t.Fatalf("CountMinSketch failed for key 'teodor'")
	}

	serialization := countMinSketch.Serialize()
	countMinSketch = Deserialize(serialization)

	if countMinSketch.CountMin("vlada") < 3 {
		t.Fatalf("CountMinSketch failed for key 'vlada' after deserialization")
	}
	if countMinSketch.CountMin("balsa") < 2 {
		t.Fatalf("CountMinSketch failed for key 'balsa' after deserialization")
	}
	if countMinSketch.CountMin("teodor") > 0 {
		t.Fatalf("CountMinSketch failed for key 'teodor' after deserialization")
	}
}
