package cache

import (
	"container/list"
	"fmt"
	database_elem "nosql-engine/packages/utils/database-elem"
	generic_types "nosql-engine/packages/utils/generic-types"
)

type Cache struct {
	lista   list.List
	size    int
	hashMap map[string]*list.Element
	last    *list.Element
}

func (cache *Cache) Contains(key string) bool {
	_, ok := cache.hashMap[key]
	return ok
}

// if an element that is being deleted is found in cache, we have to delete it from cache
func (cache *Cache) Delete(key string) bool {
	if cache.Contains(key) {
		listElem := cache.hashMap[key]
		prevValue := listElem.Value.(generic_types.KeyVal[string, database_elem.DatabaseElem])
		prevValue.Value.Tombstone = 1
		listElem.Value = prevValue

		return true
	}

	return false
}

func (cache *Cache) Refer(key string, value database_elem.DatabaseElem) database_elem.DatabaseElem {
	if !cache.Contains(key) {
		if cache.size == cache.lista.Len() {
			data := cache.last.Value.(generic_types.KeyVal[string, database_elem.DatabaseElem])
			delete(cache.hashMap, data.Key)

			tmp_last := cache.last.Prev()

			cache.lista.Remove(cache.last)
			cache.last = tmp_last
		}
	} else {
		if cache.hashMap[key] == cache.last {
			cache.last = cache.last.Prev()
		}
		value = cache.hashMap[key].Value.(generic_types.KeyVal[string, database_elem.DatabaseElem]).Value
		cache.lista.Remove(cache.hashMap[key])
	}
	cache.lista.PushFront(generic_types.KeyVal[string, database_elem.DatabaseElem]{Key: key, Value: value})
	if cache.lista.Len() == 1 {
		cache.last = cache.lista.Front()
	}

	cache.hashMap[key] = cache.lista.Front()

	return cache.lista.Front().Value.(generic_types.KeyVal[string, database_elem.DatabaseElem]).Value
}

func (cache *Cache) Display() {
	for i := cache.lista.Front(); i != nil; i = i.Next() {
		fmt.Println(i.Value)
	}
}

func New(size int) Cache {
	return Cache{lista: *list.New(), size: size, hashMap: make(map[string]*list.Element), last: nil}
}
