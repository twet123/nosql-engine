package slice_utils

func ConcatBothCopy[T any](slice1, slice2 []T) []T {
	var def []T
	return append(append(def, slice1...), slice2...)
}

func Copy[T any](slice1 []T) []T {
	var def []T
	return append(def, slice1...)
}

func InsertCopy[T any](slice1 []T, slice2 []T, element ...T) []T {
	var def []T
	return append(append(append(def, slice1...), element...), slice2...)
}

// Probably you can not avoid copying right slice with this approach
