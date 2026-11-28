package index

import (
	"sync"
)

// MapIndex implements Indexer using a standard Go map protected by a sync.RWMutex.
type MapIndex struct {
	mu        sync.RWMutex
	items     map[string]Item
	liveBytes int64
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
	if old, exists := m.items[key]; exists {
		m.liveBytes -= int64(old.RecordSize)
	}
	m.items[key] = item
	m.liveBytes += int64(item.RecordSize)
}

// CompareAndSwap updates key to updated only if key currently exists in m.items
// and matches expected.FileID and expected.RecordOffset.
func (m *MapIndex) CompareAndSwap(key string, expected, updated Item) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.items == nil {
		return false
	}

	cur, ok := m.items[key]
	if !ok {
		return false
	}
	if cur.FileID != expected.FileID || cur.RecordOffset != expected.RecordOffset {
		return false
	}
	m.liveBytes -= int64(cur.RecordSize)
	m.items[key] = updated
	m.liveBytes += int64(updated.RecordSize)
	return true
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
	if old, exists := m.items[key]; exists {
		m.liveBytes -= int64(old.RecordSize)
		delete(m.items, key)
	}
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

// LiveBytes returns the total on-disk RecordSize (in bytes) of all active entries in the index.
func (m *MapIndex) LiveBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.liveBytes
}

// Close cleans up index resources by freeing the in-memory map.
// This allows Go's Garbage Collector to immediately reclaim RAM.
// It does NOT delete or touch any persistent data on disk!
func (m *MapIndex) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = nil
	m.liveBytes = 0
	return nil
}
