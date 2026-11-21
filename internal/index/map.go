package index

import (
	"sync"
)

// MapIndex implements Indexer using a standard Go map protected by a sync.RWMutex.
type MapIndex struct {
	mu    sync.RWMutex
	items map[string]Item
}

// NewMapIndex creates and returns an initialized MapIndex.
func NewMapIndex() *MapIndex {
	return &MapIndex{
		items: make(map[string]Item),
	}
}

// Put inserts or updates an index entry for the given key.
func (m *MapIndex) Put(key string, item Item) {
	// TODO: Acquire write lock, update m.items[key], unlock
	panic("TODO: implement Put")
}

// Get finds an index entry for the given key.
func (m *MapIndex) Get(key string) (Item, bool) {
	// TODO: Acquire read lock, read m.items[key], unlock
	panic("TODO: implement Get")
}

// Delete removes an entry from the index.
func (m *MapIndex) Delete(key string) {
	// TODO: Acquire write lock, delete key from m.items, unlock
	panic("TODO: implement Delete")
}

// Keys returns a slice of all active keys in the index.
func (m *MapIndex) Keys() []string {
	// TODO: Acquire read lock, collect all keys into a []string, unlock
	panic("TODO: implement Keys")
}

// Size returns the count of active keys.
func (m *MapIndex) Size() int {
	// TODO: Acquire read lock, return len(m.items), unlock
	panic("TODO: implement Size")
}

// Close cleans up index resources.
func (m *MapIndex) Close() error {
	// TODO: clear items map if necessary
	return nil
}
