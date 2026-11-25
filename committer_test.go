package bigcat

import (
	"fmt"
	"sync"
	"testing"

	"bigcat/internal/record"
)

func TestGroupCommitter_SingleTask(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	gc := newGroupCommitter(db, 32, 0)
	defer func() { _ = gc.Close() }()

	key := []byte("hello")
	val := []byte("world")
	rec := record.NewRecord(key, val)
	encoded, err := db.codec.Encode(rec)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	task := &writeTask{
		entries: []writeEntry{{
			key:       key,
			encoded:   encoded,
			isDelete:  false,
			timestamp: rec.Timestamp,
		}},
	}

	if err := gc.Submit(task); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("db.Get failed: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("expected %s, got %s", val, got)
	}
}

func TestGroupCommitter_ConcurrentBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithSyncOnWrite(true))
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	gc := newGroupCommitter(db, 16, 0)
	defer func() { _ = gc.Close() }()

	const numWriters = 50
	var wg sync.WaitGroup

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%04d", id))
			val := []byte(fmt.Sprintf("val-%04d", id))
			rec := record.NewRecord(key, val)
			encoded, err := db.codec.Encode(rec)
			if err != nil {
				t.Errorf("writer %d encode failed: %v", id, err)
				return
			}

			task := &writeTask{
				entries: []writeEntry{{
					key:       key,
					encoded:   encoded,
					isDelete:  false,
					timestamp: rec.Timestamp,
				}},
			}

			if err := gc.Submit(task); err != nil {
				t.Errorf("writer %d submit failed: %v", id, err)
				return
			}

			// Immediate read-after-write verification
			got, err := db.Get(key)
			if err != nil {
				t.Errorf("writer %d get failed: %v", id, err)
				return
			}
			if string(got) != string(val) {
				t.Errorf("writer %d expected %s, got %s", id, val, got)
			}
		}(i)
	}

	wg.Wait()
}

func TestGroupCommitter_CloseAndDrain(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	gc := newGroupCommitter(db, 8, 0)

	// Close committer
	if err := gc.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Submitting to closed committer should return ErrCommitterClosed
	task := &writeTask{
		entries: []writeEntry{{
			key:     []byte("k"),
			encoded: []byte("v"),
		}},
	}
	err = gc.Submit(task)
	if err != ErrCommitterClosed {
		t.Fatalf("expected ErrCommitterClosed, got: %v", err)
	}
}

func TestGroupCommitter_MaxBatchBytes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Small maxBatchBytes limit: 100 bytes
	gc := newGroupCommitter(db, 100, 100)
	defer func() { _ = gc.Close() }()

	// Submit multiple items
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k-%d", i))
		val := []byte(fmt.Sprintf("val-%d", i))
		rec := record.NewRecord(key, val)
		encoded, err := db.codec.Encode(rec)
		if err != nil {
			t.Fatalf("encode failed: %v", err)
		}

		task := &writeTask{
			entries: []writeEntry{{
				key:       key,
				encoded:   encoded,
				timestamp: rec.Timestamp,
			}},
		}
		if err := gc.Submit(task); err != nil {
			t.Fatalf("submit failed: %v", err)
		}

		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get failed: %v", err)
		}
		if string(got) != string(val) {
			t.Fatalf("expected %s, got %s", val, got)
		}
	}
}

func TestGroupCommitter_MultiEntryTask(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	recA := record.NewRecord([]byte("a"), []byte("1"))
	encA, _ := db.codec.Encode(recA)

	recB := record.NewRecord([]byte("b"), []byte("2"))
	encB, _ := db.codec.Encode(recB)

	delA := record.NewTombstone([]byte("a"))
	encDelA, _ := db.codec.Encode(delA)

	task := &writeTask{
		entries: []writeEntry{
			{key: []byte("a"), encoded: encA, timestamp: recA.Timestamp},
			{key: []byte("b"), encoded: encB, timestamp: recB.Timestamp},
			{key: []byte("a"), encoded: encDelA, isDelete: true, timestamp: delA.Timestamp},
		},
	}

	if err := db.committer.Submit(task); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// "b" should exist, "a" should be deleted
	gotB, err := db.Get([]byte("b"))
	if err != nil || string(gotB) != "2" {
		t.Fatalf("expected b='2', got %q (err=%v)", gotB, err)
	}
	if _, err := db.Get([]byte("a")); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for a, got %v", err)
	}

	expectedBytes := int64(len(encA) + len(encB) + len(encDelA))
	if gotSize := db.storage.ActiveFile().Size(); gotSize != expectedBytes {
		t.Fatalf("expected disk size %d, got %d", expectedBytes, gotSize)
	}
}
