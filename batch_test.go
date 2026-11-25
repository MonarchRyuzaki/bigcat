package bigcat

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestWriteBatch_CRUDAndDeduplication(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Seed an initial key that will be deleted inside the batch
	if err := db.Put([]byte("to_delete"), []byte("old")); err != nil {
		t.Fatalf("seed Put failed: %v", err)
	}

	wb := NewWriteBatch()

	// Empty key validation
	if err := wb.Put(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Put, got %v", err)
	}
	if err := wb.Delete([]byte{}); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Delete, got %v", err)
	}

	// Stage puts, in-batch overwrites, put->delete, and delete->put
	_ = wb.Put([]byte("k1"), []byte("v1_old"))
	_ = wb.Put([]byte("k1"), []byte("v1_new")) // deduplicated overwrite
	_ = wb.Put([]byte("k2"), []byte("v2"))
	_ = wb.Delete([]byte("k2"))                // collapses to Delete
	_ = wb.Delete([]byte("k3"))
	_ = wb.Put([]byte("k3"), []byte("v3_revived")) // delete -> put revives
	_ = wb.Delete([]byte("to_delete"))

	if wb.Size() != 4 {
		t.Fatalf("expected deduplicated batch size 4, got %d", wb.Size())
	}

	if err := db.WriteBatch(wb); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// Verify active keys
	for k, expected := range map[string]string{
		"k1": "v1_new",
		"k3": "v3_revived",
	} {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get(%s) failed: %v", k, err)
		}
		if string(got) != expected {
			t.Fatalf("Get(%s) = %q, want %q", k, got, expected)
		}
	}

	// Verify deleted keys
	for _, k := range []string{"k2", "to_delete"} {
		if _, err := db.Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected ErrKeyNotFound for %s, got %v", k, err)
		}
	}
}

func TestWriteBatch_EmptyAndClear(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch(nil) failed: %v", err)
	}

	wb := NewWriteBatch()
	if err := db.WriteBatch(wb); err != nil {
		t.Fatalf("WriteBatch(empty) failed: %v", err)
	}

	_ = wb.Put([]byte("a"), []byte("1"))
	_ = wb.Put([]byte("b"), []byte("2"))
	wb.Clear()

	if wb.Size() != 0 {
		t.Fatalf("expected Size() == 0 after Clear(), got %d", wb.Size())
	}

	// Reuse after Clear
	_ = wb.Put([]byte("c"), []byte("3"))
	if err := db.WriteBatch(wb); err != nil {
		t.Fatalf("WriteBatch after Clear failed: %v", err)
	}

	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected cleared key 'a' not to be written, got err=%v", err)
	}
	got, err := db.Get([]byte("c"))
	if err != nil || string(got) != "3" {
		t.Fatalf("expected c='3', got %q (err=%v)", got, err)
	}
}

func TestWriteBatch_CrashRecoveryAndClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	wb := NewWriteBatch()
	_ = wb.Put([]byte("persist1"), []byte("val1"))
	_ = wb.Put([]byte("persist2"), []byte("val2"))
	_ = wb.Put([]byte("gone"), []byte("temp"))
	_ = wb.Delete([]byte("gone"))

	if err := db.WriteBatch(wb); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Attempting WriteBatch on closed DB must return ErrDatabaseClosed
	if err := db.WriteBatch(wb); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed, got %v", err)
	}

	// Reopen and verify log replay
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer func() { _ = db2.Close() }()

	if v, err := db2.Get([]byte("persist1")); err != nil || string(v) != "val1" {
		t.Fatalf("reopened persist1 mismatch: %q (err=%v)", v, err)
	}
	if v, err := db2.Get([]byte("persist2")); err != nil || string(v) != "val2" {
		t.Fatalf("reopened persist2 mismatch: %q (err=%v)", v, err)
	}
	if _, err := db2.Get([]byte("gone")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected gone to remain deleted after reopen, got %v", err)
	}
}

func TestWriteBatch_ConcurrentAndSync(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithSyncOnWrite(true))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	const workers = 20
	const keysPerBatch = 5
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wb := NewWriteBatch()
			for j := 0; j < keysPerBatch; j++ {
				k := []byte(fmt.Sprintf("w%d_k%d", id, j))
				v := []byte(fmt.Sprintf("w%d_v%d", id, j))
				_ = wb.Put(k, v)
			}
			if err := db.WriteBatch(wb); err != nil {
				t.Errorf("worker %d WriteBatch failed: %v", id, err)
			}
		}(w)
	}

	wg.Wait()

	for w := 0; w < workers; w++ {
		for j := 0; j < keysPerBatch; j++ {
			k := []byte(fmt.Sprintf("w%d_k%d", w, j))
			want := fmt.Sprintf("w%d_v%d", w, j)
			got, err := db.Get(k)
			if err != nil || string(got) != want {
				t.Fatalf("Get(%s) = %q (err=%v), want %q", k, got, err, want)
			}
		}
	}
}

func TestWriteBatch_DefensiveCopy(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	wb := NewWriteBatch()
	keyBuf := []byte("k1")
	valBuf := []byte("v1")
	if err := wb.Put(keyBuf, valBuf); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Mutate caller buffers immediately after staging
	copy(keyBuf, "XX")
	copy(valBuf, "YY")

	if err := db.WriteBatch(wb); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	got, err := db.Get([]byte("k1"))
	if err != nil || string(got) != "v1" {
		t.Fatalf("expected k1='v1' after caller buffer mutation, got %q (err=%v)", got, err)
	}
	if _, err := db.Get([]byte("XX")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected mutated key 'XX' not to exist, got err=%v", err)
	}
}

func TestWriteBatch_ConcurrentMixedAndRotation(t *testing.T) {
	dir := t.TempDir()
	// Small 512B file size forces frequent rotations during concurrent WriteBatch + Put + Delete
	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const workers = 12
	const rounds = 20
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wb := NewWriteBatch()
			for r := 0; r < rounds; r++ {
				wb.Clear()
				k1 := []byte(fmt.Sprintf("rot_%d_%d_a", id, r))
				k2 := []byte(fmt.Sprintf("rot_%d_%d_b", id, r))
				_ = wb.Put(k1, []byte("val_a_payload"))
				_ = wb.Put(k2, []byte("val_b_payload"))
				if r%3 == 0 {
					_ = wb.Delete(k1)
				}
				if err := db.WriteBatch(wb); err != nil {
					t.Errorf("worker %d WriteBatch round %d failed: %v", id, r, err)
					return
				}

				// Interleave single Put and Get
				singleK := []byte(fmt.Sprintf("single_%d_%d", id, r))
				if err := db.Put(singleK, []byte("sval")); err != nil {
					t.Errorf("worker %d Put failed: %v", id, err)
					return
				}
				if _, err := db.Get(k2); err != nil {
					t.Errorf("worker %d Get(%s) failed: %v", id, k2, err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and verify all rotated .data files replay cleanly without corruption
	db2, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Reopen after rotation failed: %v", err)
	}
	defer func() { _ = db2.Close() }()

	for w := 0; w < workers; w++ {
		for r := 0; r < rounds; r++ {
			k1 := []byte(fmt.Sprintf("rot_%d_%d_a", w, r))
			k2 := []byte(fmt.Sprintf("rot_%d_%d_b", w, r))
			if r%3 == 0 {
				if _, err := db2.Get(k1); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("expected %s deleted after reopen, got %v", k1, err)
				}
			} else {
				if v, err := db2.Get(k1); err != nil || string(v) != "val_a_payload" {
					t.Fatalf("expected %s='val_a_payload', got %q (err=%v)", k1, v, err)
				}
			}
			if v, err := db2.Get(k2); err != nil || string(v) != "val_b_payload" {
				t.Fatalf("expected %s='val_b_payload', got %q (err=%v)", k2, v, err)
			}
		}
	}
}

func BenchmarkDB_WriteBatch10(b *testing.B) {
	benchmarkWriteBatch(b, 10)
}

func BenchmarkDB_WriteBatch100(b *testing.B) {
	benchmarkWriteBatch(b, 100)
}

func benchmarkWriteBatch(b *testing.B, batchSize int) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	keys := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = []byte(fmt.Sprintf("batch_key_%03d", i))
	}
	val := []byte("bench_batch_value_payload")

	wb := NewWriteBatch()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		wb.Clear()
		for j := 0; j < batchSize; j++ {
			_ = wb.Put(keys[j], val)
		}
		if err := db.WriteBatch(wb); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDB_ParallelWriteBatch10(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	val := []byte("bench_parallel_batch_payload")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		wb := NewWriteBatch()
		var i int
		for pb.Next() {
			i++
			wb.Clear()
			for j := 0; j < 10; j++ {
				_ = wb.Put([]byte(fmt.Sprintf("pbatch_%d_%d", i, j)), val)
			}
			if err := db.WriteBatch(wb); err != nil {
				b.Error(err)
			}
		}
	})
}
