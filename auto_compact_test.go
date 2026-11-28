package bigcat

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bigcat/internal/storage"
)

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func TestDB_AutoCompact_PeriodicTicker(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(
		dir,
		WithMaxFileSize(200),
		WithSyncOnWrite(false),
		WithAutoCompactInterval(20*time.Millisecond),
		WithCompactMinFiles(3),
		WithCompactDeadSpaceRatio(0.4),
	)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write 30 keys, then overwrite/delete > 50% across rotated files
	for i := 0; i < 30; i++ {
		if err := db.Put([]byte(fmt.Sprintf("tk_%02d", i)), []byte("initial_payload_value_01234")); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
	for i := 0; i < 15; i++ {
		if err := db.Put([]byte(fmt.Sprintf("tk_%02d", i)), []byte("updated_payload_value_56789")); err != nil {
			t.Fatalf("Overwrite %d failed: %v", i, err)
		}
	}
	for i := 15; i < 25; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("tk_%02d", i))); err != nil {
			t.Fatalf("Delete %d failed: %v", i, err)
		}
	}

	// Wait for background compaction ticker to produce companion .hint files without manual Merge()
	waitUntil(t, 3*time.Second, "periodic background compaction to produce .hint files", func() bool {
		sealed, _ := db.storage.SealedSnapshot()
		if len(sealed) == 0 {
			return false
		}
		_, err := os.Stat(filepath.Join(dir, storage.HintFileName(sealed[0])))
		return err == nil
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(200))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 15; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("tk_%02d", i)))
		if err != nil || !bytes.Equal(got, []byte("updated_payload_value_56789")) {
			t.Fatalf("tk_%02d mismatch: got=%q err=%v", i, got, err)
		}
	}
	for i := 15; i < 25; i++ {
		if _, err := db2.Get([]byte(fmt.Sprintf("tk_%02d", i))); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected deleted tk_%02d not found, got %v", i, err)
		}
	}
	for i := 25; i < 30; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("tk_%02d", i)))
		if err != nil || !bytes.Equal(got, []byte("initial_payload_value_01234")) {
			t.Fatalf("tk_%02d mismatch: got=%q err=%v", i, got, err)
		}
	}
}

func TestDB_AutoCompact_RotationDeadSpaceThreshold(t *testing.T) {
	dir := t.TempDir()
	// Ticker disabled (interval=0); relies solely on rotation notifications + dead-space ratio!
	db, err := Open(
		dir,
		WithMaxFileSize(180),
		WithSyncOnWrite(false),
		WithCompactMinFiles(3),
		WithCompactDeadSpaceRatio(0.45),
	)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 1. Insert unique keys across >= 4 sealed files (0% dead space -> must NOT auto-compact)
	for i := 0; i < 20; i++ {
		if err := db.Put([]byte(fmt.Sprintf("uk_%02d", i)), []byte("unique_payload_value_abcde")); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	sealed, _ := db.storage.SealedSnapshot()
	if len(sealed) < 3 {
		t.Fatalf("expected >= 3 sealed files, got %v", sealed)
	}
	if _, err := os.Stat(filepath.Join(dir, storage.HintFileName(sealed[0]))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("did not expect compaction with 0%% dead space, got err=%v", err)
	}

	// 2. Delete 14 keys (crosses 45% dead space) and rotate so notification fires
	for i := 0; i < 14; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("uk_%02d", i))); err != nil {
			t.Fatalf("Delete %d failed: %v", i, err)
		}
	}
	for i := 0; i < 6; i++ {
		if err := db.Put([]byte(fmt.Sprintf("trigger_rot_%d", i)), []byte("rotation_trigger_payload_xyz")); err != nil {
			t.Fatalf("Put trigger %d failed: %v", i, err)
		}
	}

	waitUntil(t, 3*time.Second, "rotation-triggered dead-space compaction", func() bool {
		s, _ := db.storage.SealedSnapshot()
		if len(s) == 0 {
			return false
		}
		_, err := os.Stat(filepath.Join(dir, storage.HintFileName(s[0])))
		return err == nil
	})

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestDB_AutoCompact_CloseDrainsInFlightMerge(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(
		dir,
		WithMaxFileSize(180),
		WithSyncOnWrite(false),
		WithAutoCompactInterval(10*time.Millisecond),
		WithCompactMinFiles(2),
	)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Pause compaction mid-copy using mergeFailpoint so Close() definitely races with active Merge()
	enteredMerge := make(chan struct{})
	releaseMerge := make(chan struct{})
	var enteredOnce sync.Once

	mergeFailpoint = func(point string) {
		if point == "stage_mid_copy" {
			enteredOnce.Do(func() {
				close(enteredMerge)
			})
			<-releaseMerge
		}
	}
	defer func() { mergeFailpoint = nil }()

	for i := 0; i < 15; i++ {
		if err := db.Put([]byte(fmt.Sprintf("dk_%02d", i)), []byte("drain_payload_value_012345")); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	select {
	case <-enteredMerge:
	case <-time.After(3 * time.Second):
		close(releaseMerge)
		t.Fatalf("timed out waiting for background compaction to enter stage_mid_copy")
	}

	// Call db.Close() while background Merge() is blocked mid-stage
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	// Verify Close() waits for the in-flight Merge() rather than closing storage underneath it
	select {
	case err := <-closeDone:
		t.Fatalf("Close() returned prematurely while Merge() was still running: err=%v", err)
	case <-time.After(40 * time.Millisecond):
	}

	close(releaseMerge)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() failed after releasing Merge(): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for Close() to finish draining")
	}

	db2, err := Open(dir, WithMaxFileSize(180))
	if err != nil {
		t.Fatalf("Reopen after Close drain failed: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 15; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("dk_%02d", i)))
		if err != nil || !bytes.Equal(got, []byte("drain_payload_value_012345")) {
			t.Fatalf("dk_%02d mismatch after drain+reopen: got=%q err=%v", i, got, err)
		}
	}
}

func TestDB_AutoCompact_ConcurrentTrafficUnderRace(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(
		dir,
		WithMaxFileSize(256),
		WithSyncOnWrite(false),
		WithAutoCompactInterval(15*time.Millisecond),
		WithCompactMinFiles(2),
		WithCompactDeadSpaceRatio(0.2),
	)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const numWriters = 4
	const keysPerWriter = 20
	expected := make([][]string, numWriters)
	for w := 0; w < numWriters; w++ {
		expected[w] = make([]string, keysPerWriter)
	}

	var stopReaders atomic.Bool
	var writerWg sync.WaitGroup
	var readerWg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		writerWg.Add(1)
		wid := w
		go func() {
			defer writerWg.Done()
			for step := 0; step < 80; step++ {
				idx := step % keysPerWriter
				key := []byte(fmt.Sprintf("aw%d_k%02d", wid, idx))
				if step%4 == 3 && expected[wid][idx] != "" {
					if err := db.Delete(key); err != nil {
						t.Errorf("Delete failed: %v", err)
						return
					}
					expected[wid][idx] = ""
				} else {
					val := fmt.Sprintf("auto_%d_%d_payload_pad", wid, step)
					if err := db.Put(key, []byte(val)); err != nil {
						t.Errorf("Put failed: %v", err)
						return
					}
					expected[wid][idx] = val
				}
			}
		}()
	}

	for r := 0; r < 3; r++ {
		readerWg.Add(1)
		rid := r
		go func() {
			defer readerWg.Done()
			for i := 0; !stopReaders.Load(); i++ {
				key := []byte(fmt.Sprintf("aw%d_k%02d", (rid+i)%numWriters, i%keysPerWriter))
				_, err := db.Get(key)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("concurrent Get(%s) failed: %v", key, err)
					return
				}
			}
		}()
	}

	writerWg.Wait()
	stopReaders.Store(true)
	readerWg.Wait()

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	for w := 0; w < numWriters; w++ {
		for k := 0; k < keysPerWriter; k++ {
			key := []byte(fmt.Sprintf("aw%d_k%02d", w, k))
			got, err := db2.Get(key)
			if expected[w][k] == "" {
				if !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("expected %s deleted, got=%q err=%v", key, got, err)
				}
			} else if err != nil || string(got) != expected[w][k] {
				t.Fatalf("key %s mismatch: got=%q want=%q err=%v", key, got, expected[w][k], err)
			}
		}
	}
}
