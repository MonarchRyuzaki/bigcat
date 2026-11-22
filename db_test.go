package bigcat

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"bigcat/internal/storage"
)

func TestDB_BasicCRUD(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// 1. Put and Get
	key := []byte("language")
	val := []byte("golang")
	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Get value mismatch: expected %s, got %s", val, got)
	}

	// 2. Overwrite key
	newVal := []byte("go 1.27")
	if err := db.Put(key, newVal); err != nil {
		t.Fatalf("Put overwrite failed: %v", err)
	}
	got2, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get overwritten failed: %v", err)
	}
	if !bytes.Equal(got2, newVal) {
		t.Fatalf("expected updated value %s, got %s", newVal, got2)
	}

	// 3. Delete key
	if err := db.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = db.Get(key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// 4. Deleting non-existent key
	err = db.Delete([]byte("missing_key"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound deleting missing key, got %v", err)
	}

	// 5. Empty key handling
	if err := db.Put(nil, []byte("val")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Put, got %v", err)
	}
	if _, err := db.Get(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Get, got %v", err)
	}
	if err := db.Delete(nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("expected ErrEmptyKey on Delete, got %v", err)
	}
}

func TestDB_CrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Session 1: Populate database
	db1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	numKeys := 100
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("key_%03d", i))
		v := []byte(fmt.Sprintf("val_%03d", i))
		if err := db1.Put(k, v); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Delete even keys between 0 and 20 (0, 2, 4, ... 18)
	for i := 0; i < 20; i += 2 {
		k := []byte(fmt.Sprintf("key_%03d", i))
		if err := db1.Delete(k); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Overwrite odd keys between 0 and 20 (1, 3, ... 19)
	for i := 1; i < 20; i += 2 {
		k := []byte(fmt.Sprintf("key_%03d", i))
		v := []byte(fmt.Sprintf("updated_val_%03d", i))
		if err := db1.Put(k, v); err != nil {
			t.Fatalf("Put update failed: %v", err)
		}
	}

	// Close database
	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Session 2: Reopen from disk (tests KeyDir reconstruction)
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Re-Open failed: %v", err)
	}
	defer db2.Close()

	// Verify deleted keys are not found
	for i := 0; i < 20; i += 2 {
		k := []byte(fmt.Sprintf("key_%03d", i))
		_, err := db2.Get(k)
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected key %s to remain deleted after recovery, got err: %v", k, err)
		}
	}

	// Verify overwritten keys have updated values
	for i := 1; i < 20; i += 2 {
		k := []byte(fmt.Sprintf("key_%03d", i))
		expectedVal := []byte(fmt.Sprintf("updated_val_%03d", i))
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("expected key %s to exist after recovery: %v", k, err)
		}
		if !bytes.Equal(got, expectedVal) {
			t.Fatalf("recovered value mismatch for %s: expected %s, got %s", k, expectedVal, got)
		}
	}

	// Verify remaining keys (20..99)
	for i := 20; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("key_%03d", i))
		expectedVal := []byte(fmt.Sprintf("val_%03d", i))
		got, err := db2.Get(k)
		if err != nil {
			t.Fatalf("expected key %s to exist: %v", k, err)
		}
		if !bytes.Equal(got, expectedVal) {
			t.Fatalf("value mismatch for %s: expected %s, got %s", k, expectedVal, got)
		}
	}
}

func TestDB_OperationsAfterClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("status")
	val := []byte("active")
	if err := db.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Operations after close should return ErrDatabaseClosed
	if err := db.Put(key, val); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Put, got %v", err)
	}
	if _, err := db.Get(key); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Get, got %v", err)
	}
	if err := db.Delete(key); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Delete, got %v", err)
	}
	if err := db.Sync(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on Sync, got %v", err)
	}
}

func TestDB_WithSyncOnWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithSyncOnWrite(true))
	if err != nil {
		t.Fatalf("Open with sync failed: %v", err)
	}
	defer db.Close()

	if err := db.Put([]byte("sync_k"), []byte("sync_v")); err != nil {
		t.Fatalf("Put with sync failed: %v", err)
	}

	got, err := db.Get([]byte("sync_k"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, []byte("sync_v")) {
		t.Fatalf("mismatch: expected sync_v, got %s", got)
	}
}

func TestDB_CorruptedRecordDetectionOnStartup(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := db.Put([]byte("corrupt_k"), []byte("corrupt_v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	db.Close()

	// Corrupt a byte in 00000.data
	filePath := filepath.Join(dir, storage.DataFileName(0))
	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read file failed: %v", err)
	}

	// Flip a bit in the value payload
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("write tampered file failed: %v", err)
	}

	// Re-open should detect CRC failure on startup
	_, err = Open(dir)
	if err == nil {
		t.Fatalf("expected error on startup with corrupted CRC, got nil")
	}
}

func TestDB_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	numWriters := 8
	numReaders := 8
	itemsPerWorker := 100

	// Concurrent Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				k := []byte(fmt.Sprintf("w:%d-key:%d", workerID, i%20))
				v := []byte(fmt.Sprintf("value-%d", i))
				_ = db.Put(k, v)
			}
		}()
	}

	// Concurrent Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		readerID := r
		go func() {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				k := []byte(fmt.Sprintf("w:%d-key:%d", readerID, i%20))
				_, _ = db.Get(k)
			}
		}()
	}

	wg.Wait()

	keys := db.Keys()
	if len(keys) == 0 {
		t.Fatalf("expected non-empty keys after concurrent operations")
	}
}

func TestDB_MultiFileRotationAndRecovery(t *testing.T) {
	dir := t.TempDir()

	// 1. Open with 512-byte max file size
	db1, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const numKeys = 40
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		v := []byte(fmt.Sprintf("rot_val_%03d_padding_data_to_fill_space", i))
		if err := db1.Put(k, v); err != nil {
			t.Fatalf("Put key %d failed: %v", i, err)
		}
	}

	// Verify all keys readable before close
	for i := 0; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		expected := []byte(fmt.Sprintf("rot_val_%03d_padding_data_to_fill_space", i))
		got, err := db1.Get(k)
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("Get key %d failed: err=%v, match=%v", i, err, bytes.Equal(got, expected))
		}
	}

	// Overwrite some keys (10..19)
	for i := 10; i < 20; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		newVal := []byte(fmt.Sprintf("OVERWRITTEN_%03d_value_data", i))
		if err := db1.Put(k, newVal); err != nil {
			t.Fatalf("Put overwrite %d failed: %v", i, err)
		}
	}

	// Delete some keys (0..9)
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		if err := db1.Delete(k); err != nil {
			t.Fatalf("Delete key %d failed: %v", i, err)
		}
	}

	// Close DB
	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2. Re-open DB (tests multi-file recovery, tombstone replay across files, and overwritten value replay)
	db2, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Re-Open failed: %v", err)
	}
	defer db2.Close()

	// Verify deleted keys (0..9) return ErrKeyNotFound
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		if _, err := db2.Get(k); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected deleted key %s to not be found, got %v", k, err)
		}
	}

	// Verify overwritten keys (10..19) return updated values
	for i := 10; i < 20; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		expected := []byte(fmt.Sprintf("OVERWRITTEN_%03d_value_data", i))
		got, err := db2.Get(k)
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("overwritten key %d mismatch: expected %s, got %s (err: %v)", i, expected, got, err)
		}
	}

	// Verify untouched keys (20..39) return original values
	for i := 20; i < numKeys; i++ {
		k := []byte(fmt.Sprintf("rot_key_%03d", i))
		expected := []byte(fmt.Sprintf("rot_val_%03d_padding_data_to_fill_space", i))
		got, err := db2.Get(k)
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("untouched key %d mismatch: expected %s, got %s (err: %v)", i, expected, got, err)
		}
	}

	// Verify writing to active file after recovery works
	newKey := []byte("brand_new_key_after_restart")
	newVal := []byte("fresh_data_in_active_file")
	if err := db2.Put(newKey, newVal); err != nil {
		t.Fatalf("Put new key after restart failed: %v", err)
	}
	got, err := db2.Get(newKey)
	if err != nil || !bytes.Equal(got, newVal) {
		t.Fatalf("Get new key mismatch: expected %s, got %s", newVal, got)
	}
}

func TestDB_TombstoneReplayLifecycleAcrossFiles(t *testing.T) {
	dir := t.TempDir()

	// Small threshold to force rotation between operations
	db1, err := Open(dir, WithMaxFileSize(100))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	key := []byte("lifecycle_key")

	// 1. Write initial key (file 0)
	if err := db1.Put(key, []byte("value_one_padding_to_exceed_file_size")); err != nil {
		t.Fatalf("Put 1 failed: %v", err)
	}

	// 2. Delete key (tombstone in rotated file)
	if err := db1.Delete(key); err != nil {
		t.Fatalf("Delete 1 failed: %v", err)
	}

	// 3. Re-insert key with new value
	if err := db1.Put(key, []byte("value_two_resurrected_key_payload")); err != nil {
		t.Fatalf("Put 2 failed: %v", err)
	}

	// 4. Delete key again (tombstone is final ground truth)
	if err := db1.Delete(key); err != nil {
		t.Fatalf("Delete 2 failed: %v", err)
	}

	if err := db1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen database from disk and verify the key is deleted
	db2, err := Open(dir, WithMaxFileSize(100))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	if _, err := db2.Get(key); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected key to remain deleted after lifecycle replay, got err: %v", err)
	}
}

func BenchmarkDB_Put(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("bench_key")
	val := []byte("bench_value_payload")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = db.Put(key, val)
	}
}

func BenchmarkDB_Get(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("bench_key")
	val := []byte("bench_value_payload")
	if err := db.Put(key, val); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(key)
	}
}
