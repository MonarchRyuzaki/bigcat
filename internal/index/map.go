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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = item
}

// Get finds an index entry for the given key.
func (m *MapIndex) Get(key string) (Item, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	item, ok := m.items[key]
	return item, ok
}

// Delete removes an entry from the index.
func (m *MapIndex) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

// Keys returns a slice of all active keys in the index.
func (m *MapIndex) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, len(m.items))
	i := 0
	for k := range m.items {
		keys[i] = k
		i++
	}
	return keys
}

// Size returns the count of active keys.
func (m *MapIndex) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

// Close cleans up index resources by freeing the in-memory map.
// This allows Go's Garbage Collector to immediately reclaim RAM.
// It does NOT delete or touch any persistent data on disk!
func (m *MapIndex) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = nil
	return nil
}
