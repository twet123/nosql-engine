package main

import (
	"container/list"
	"fmt"
)

type Pair[K comparable, V any] struct {
	key   K
	value V
}

type Cache[K comparable, V any] struct {
	lista   list.List
	size    int
	hashMap map[K]*list.Element
	last    *list.Element
}

func (cache *Cache[K, V]) Contains(key K) bool {
	_, ok := cache.hashMap[key]
	return ok
}

func (cache *Cache[K, V]) Refer(key K, value V) V {
	if !cache.Contains(key) {
		if cache.size == cache.lista.Len() {
			data := cache.last.Value.(Pair[K, V])
			delete(cache.hashMap, data.key)

			tmp_last := cache.last.Prev()

			cache.lista.Remove(cache.last)
			cache.last = tmp_last
		}
	} else {
		if cache.hashMap[key] == cache.last {
			cache.last = cache.last.Prev()
		}
		value = cache.hashMap[key].Value.(Pair[K, V]).value
		cache.lista.Remove(cache.hashMap[key])
	}
	cache.lista.PushFront(Pair[K, V]{key: key, value: value})
	if cache.lista.Len() == 1 {
		cache.last = cache.lista.Front()
	}

	cache.hashMap[key] = cache.lista.Front()

	return cache.lista.Front().Value.(Pair[K, V]).value
}

func (cache *Cache[K, V]) Display() {
	for i := cache.lista.Front(); i != nil; i = i.Next() {
		fmt.Println(i.Value)
	}
}

func New[K comparable, V any](size int) Cache[K, V] {
	return Cache[K, V]{lista: *list.New(), size: size, hashMap: make(map[K]*list.Element), last: nil}
}
