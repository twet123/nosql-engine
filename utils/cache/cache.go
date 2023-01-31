package cache

import (
	"container/list"
	"fmt"
	generic_types "nosql-engine/packages/utils/generic-types"
)

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
			data := cache.last.Value.(generic_types.KeyVal[K, V])
			delete(cache.hashMap, data.Key)

			tmp_last := cache.last.Prev()

			cache.lista.Remove(cache.last)
			cache.last = tmp_last
		}
	} else {
		if cache.hashMap[key] == cache.last {
			cache.last = cache.last.Prev()
		}
		value = cache.hashMap[key].Value.(generic_types.KeyVal[K, V]).Value
		cache.lista.Remove(cache.hashMap[key])
	}
	cache.lista.PushFront(generic_types.KeyVal[K, V]{Key: key, Value: value})
	if cache.lista.Len() == 1 {
		cache.last = cache.lista.Front()
	}

	cache.hashMap[key] = cache.lista.Front()

	return cache.lista.Front().Value.(generic_types.KeyVal[K, V]).Value
}

func (cache *Cache[K, V]) Display() {
	for i := cache.lista.Front(); i != nil; i = i.Next() {
		fmt.Println(i.Value)
	}
}

func New[K comparable, V any](size int) Cache[K, V] {
	return Cache[K, V]{lista: *list.New(), size: size, hashMap: make(map[K]*list.Element), last: nil}
}
