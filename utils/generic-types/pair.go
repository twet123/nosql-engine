package generic_types

import "fmt"

type KeyVal[K, V any] struct {
	Key   K
	Value V
}

func (receiver KeyVal[K, V]) String() string {
	return fmt.Sprint("{ ", receiver.Key, ": ", receiver.Value, " }")
}
