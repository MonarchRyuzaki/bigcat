package index

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func TestMapIndex_CRUD(t *testing.T) {
	idx := NewMapIndex()
	defer idx.Close()

	// 1. Initial empty state
	if s := idx.Size(); s != 0 {
		t.Fatalf("expected initial size 0, got %d", s)
	}

	_, exists := idx.Get("non_existent")
	if exists {
		t.Fatalf("expected non_existent key to return false")
	}

	// 2. Put and Get
	item1 := Item{
		RecordOffset: 100,
		RecordSize:   50,
		Timestamp:    123456,
	}
	idx.Put("user:1", item1)

	if s := idx.Size(); s != 1 {
		t.Fatalf("expected size 1, got %d", s)
	}

	retrieved, exists := idx.Get("user:1")
	if !exists {
		t.Fatalf("expected key user:1 to exist")
	}
	if retrieved != item1 {
		t.Fatalf("expected %+v, got %+v", item1, retrieved)
	}

	// 3. Overwrite existing key
	item2 := Item{
		RecordOffset: 500,
		RecordSize:   60,
		Timestamp:    123499,
	}
	idx.Put("user:1", item2)

	if s := idx.Size(); s != 1 {
		t.Fatalf("expected size to remain 1 after overwrite, got %d", s)
	}

	retrieved, exists = idx.Get("user:1")
	if !exists {
		t.Fatalf("expected key user:1 to exist after overwrite")
	}
	if retrieved != item2 {
		t.Fatalf("expected overwritten item %+v, got %+v", item2, retrieved)
	}

	// 4. Delete key
	idx.Delete("user:1")
	if s := idx.Size(); s != 0 {
		t.Fatalf("expected size 0 after delete, got %d", s)
	}

	_, exists = idx.Get("user:1")
	if exists {
		t.Fatalf("expected deleted key user:1 to return false")
	}

	// 5. Deleting non-existent key shouldn't panic
	idx.Delete("non_existent_key")
}

func TestMapIndex_Keys(t *testing.T) {
	idx := NewMapIndex()
	defer idx.Close()

	expectedKeys := []string{"alpha", "beta", "gamma", "delta"}
	for i, k := range expectedKeys {
		idx.Put(k, Item{RecordOffset: int64(i * 10), RecordSize: 10, Timestamp: uint64(i)})
	}

	keys := idx.Keys()
	if len(keys) != len(expectedKeys) {
		t.Fatalf("expected %d keys, got %d", len(expectedKeys), len(keys))
	}

	sort.Strings(keys)
	sort.Strings(expectedKeys)

	for i := range keys {
		if keys[i] != expectedKeys[i] {
			t.Errorf("key mismatch at index %d: expected %s, got %s", i, expectedKeys[i], keys[i])
		}
	}
}

func TestMapIndex_Close(t *testing.T) {
	idx := NewMapIndex()
	idx.Put("temp", Item{RecordOffset: 10, RecordSize: 20, Timestamp: 30})

	if err := idx.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	// After close, reading from map should safely return false without crashing
	_, exists := idx.Get("temp")
	if exists {
		t.Fatalf("expected false after Close()")
	}
	if s := idx.Size(); s != 0 {
		t.Fatalf("expected size 0 after Close(), got %d", s)
	}
}

func TestMapIndex_ConcurrentAccess(t *testing.T) {
	idx := NewMapIndex()
	defer idx.Close()

	var wg sync.WaitGroup
	numWorkers := 20
	opsPerWorker := 500

	// Concurrent writers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker-%d-key-%d", workerID, i%10)
				idx.Put(key, Item{
					RecordOffset: int64(i * 100),
					RecordSize:   uint32(i + 10),
					Timestamp:    uint64(i),
				})
			}
		}()
	}

	// Concurrent readers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("worker-%d-key-%d", workerID, i%10)
				idx.Get(key)
				_ = idx.Size()
			}
		}()
	}

	wg.Wait()

	// Ensure keys can still be enumerated safely
	keys := idx.Keys()
	if len(keys) == 0 {
		t.Fatalf("expected non-zero keys after concurrent writes")
	}
}

func BenchmarkMapIndex_Put(b *testing.B) {
	idx := NewMapIndex()
	defer idx.Close()

	item := Item{RecordOffset: 1024, RecordSize: 128, Timestamp: 1000}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx.Put("user_key", item)
	}
}

func BenchmarkMapIndex_Get(b *testing.B) {
	idx := NewMapIndex()
	defer idx.Close()

	item := Item{RecordOffset: 1024, RecordSize: 128, Timestamp: 1000}
	idx.Put("user_key", item)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Get("user_key")
	}
}

func BenchmarkMapIndex_ParallelGet(b *testing.B) {
	idx := NewMapIndex()
	defer idx.Close()

	item := Item{RecordOffset: 1024, RecordSize: 128, Timestamp: 1000}
	idx.Put("user_key", item)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = idx.Get("user_key")
		}
	})
}
