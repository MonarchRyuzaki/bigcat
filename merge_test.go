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

	"bigcat/internal/storage"
)

func dirDataSize(t *testing.T, dir string) (int, int64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) failed: %v", dir, err)
	}
	var count int
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, err := storage.ParseFileID(e.Name()); err == nil {
			info, err := e.Info()
			if err != nil {
				t.Fatalf("Info(%s) failed: %v", e.Name(), err)
			}
			count++
			total += info.Size()
		}
	}
	return count, total
}

func TestDB_Merge_NoOpAndClosed(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := db.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Only active file exists -> Merge should be a clean no-op
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge on single active file failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, mergeTempDirName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected .tmp dir not to exist after no-op Merge, got %v", err)
	}

	got, err := db.Get([]byte("k1"))
	if err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get after no-op Merge mismatch: got=%q err=%v", got, err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := db.Merge(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("expected ErrDatabaseClosed on closed DB, got %v", err)
	}
}

func TestDB_Merge_SpaceReclamationAndReopen(t *testing.T) {
	dir := t.TempDir()
	// Small MaxFileSize forces many rotations so sealed files accumulate quickly
	db, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// 1. Insert 80 keys
	for i := 0; i < 80; i++ {
		k := []byte(fmt.Sprintf("key_%03d", i))
		v := []byte(fmt.Sprintf("init_val_%03d_padding_bytes", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}

	// 2. Overwrite keys 0..39 three times (creates heavy dead space)
	for round := 1; round <= 3; round++ {
		for i := 0; i < 40; i++ {
			k := []byte(fmt.Sprintf("key_%03d", i))
			v := []byte(fmt.Sprintf("round_%d_val_%03d_padding", round, i))
			if err := db.Put(k, v); err != nil {
				t.Fatalf("Overwrite %d failed: %v", i, err)
			}
		}
	}

	// 3. Delete keys 40..69 (creates tombstones across sealed files)
	for i := 40; i < 70; i++ {
		k := []byte(fmt.Sprintf("key_%03d", i))
		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete %d failed: %v", i, err)
		}
	}

	// 4. Rotate once more, then update key_070 and delete key_071 in the ACTIVE file
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if err := db.Put([]byte("key_070"), []byte("active_file_override_070")); err != nil {
		t.Fatalf("Put active override failed: %v", err)
	}
	if err := db.Delete([]byte("key_071")); err != nil {
		t.Fatalf("Delete active override failed: %v", err)
	}

	filesBefore, bytesBefore := dirDataSize(t, dir)
	if filesBefore < 5 {
		t.Fatalf("expected multiple rotated files before Merge, got %d", filesBefore)
	}

	// 5. Run compaction
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, mergeTempDirName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected .tmp directory to be removed after Merge, got err=%v", err)
	}

	filesAfter, bytesAfter := dirDataSize(t, dir)
	if bytesAfter >= bytesBefore {
		t.Fatalf("expected disk usage reduction after Merge: before=%d, after=%d", bytesBefore, bytesAfter)
	}
	if filesAfter >= filesBefore {
		t.Fatalf("expected fewer data files after Merge: before=%d, after=%d", filesBefore, filesAfter)
	}

	verifyState := func(target *DB, stage string) {
		for i := 0; i < 40; i++ {
			k := []byte(fmt.Sprintf("key_%03d", i))
			want := []byte(fmt.Sprintf("round_3_val_%03d_padding", i))
			got, err := target.Get(k)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("[%s] key %s mismatch: got=%q want=%q err=%v", stage, k, got, want, err)
			}
		}
		for i := 40; i < 70; i++ {
			k := []byte(fmt.Sprintf("key_%03d", i))
			if _, err := target.Get(k); !errors.Is(err, ErrKeyNotFound) {
				t.Fatalf("[%s] deleted key %s resurrected or errored: err=%v", stage, k, err)
			}
		}
		if got, err := target.Get([]byte("key_070")); err != nil || !bytes.Equal(got, []byte("active_file_override_070")) {
			t.Fatalf("[%s] key_070 mismatch: got=%q err=%v", stage, got, err)
		}
		if _, err := target.Get([]byte("key_071")); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("[%s] active-deleted key_071 mismatch: err=%v", stage, err)
		}
		for i := 72; i < 80; i++ {
			k := []byte(fmt.Sprintf("key_%03d", i))
			want := []byte(fmt.Sprintf("init_val_%03d_padding_bytes", i))
			got, err := target.Get(k)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("[%s] untouched key %s mismatch: got=%q want=%q err=%v", stage, k, got, want, err)
			}
		}
	}

	verifyState(db, "live after Merge")

	// 6. Write more data after Merge, close, and reopen from disk
	if err := db.Put([]byte("post_merge"), []byte("still_working")); err != nil {
		t.Fatalf("Put after Merge failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Reopen after Merge failed: %v", err)
	}
	defer db2.Close()

	verifyState(db2, "reopened after Merge")
	if got, err := db2.Get([]byte("post_merge")); err != nil || !bytes.Equal(got, []byte("still_working")) {
		t.Fatalf("post_merge key mismatch after reopen: got=%q err=%v", got, err)
	}
}

func TestDB_Merge_ZeroSurvivingKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(128))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write keys across sealed files, then delete every single one
	for i := 0; i < 15; i++ {
		k := []byte(fmt.Sprintf("ephemeral_%02d", i))
		if err := db.Put(k, []byte("payload_value_123456")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	for i := 0; i < 15; i++ {
		k := []byte(fmt.Sprintf("ephemeral_%02d", i))
		if err := db.Delete(k); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Rotate so all keys + tombstones sit in sealed files and active file is empty
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge with zero surviving keys failed: %v", err)
	}

	// Only the active file should remain
	ids := db.storage.FileIDs()
	if len(ids) != 1 || ids[0] != db.storage.ActiveFile().FileID() {
		t.Fatalf("expected only active file to remain after empty Merge, got %v", ids)
	}
	if len(db.Keys()) != 0 {
		t.Fatalf("expected 0 keys after Merge, got %v", db.Keys())
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(128))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	if len(db2.Keys()) != 0 {
		t.Fatalf("expected 0 keys after reopen, got %v", db2.Keys())
	}
}

func TestDB_Merge_ShrunkMaxFileSizeCapsFileIDs(t *testing.T) {
	dir := t.TempDir()

	// Session 1: Large MaxFileSize -> 2 sealed files (0, 1) and active (2)
	db1, err := Open(dir, WithMaxFileSize(4096))
	if err != nil {
		t.Fatalf("Open 1 failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("k_%02d", i))
		v := bytes.Repeat([]byte{byte('a' + i%26)}, 80)
		if err := db1.Put(k, v); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
	if err := db1.storage.Rotate(); err != nil {
		t.Fatalf("Rotate 1 failed: %v", err)
	}
	if err := db1.Put([]byte("k_file1"), bytes.Repeat([]byte("z"), 80)); err != nil {
		t.Fatalf("Put file1 failed: %v", err)
	}
	if err := db1.storage.Rotate(); err != nil {
		t.Fatalf("Rotate 2 failed: %v", err)
	}
	if err := db1.Put([]byte("k_active"), []byte("active_val")); err != nil {
		t.Fatalf("Put active failed: %v", err)
	}
	activeID := db1.storage.ActiveFile().FileID()
	if err := db1.Close(); err != nil {
		t.Fatalf("Close 1 failed: %v", err)
	}

	// Session 2: Reopen with a much smaller MaxFileSize (128 bytes)
	db2, err := Open(dir, WithMaxFileSize(128))
	if err != nil {
		t.Fatalf("Open 2 failed: %v", err)
	}

	if err := db2.Merge(); err != nil {
		t.Fatalf("Merge with shrunk MaxFileSize failed: %v", err)
	}

	// Every compacted FileID must stay strictly below activeID
	sealed, curActive := db2.storage.SealedSnapshot()
	if curActive != activeID {
		t.Fatalf("expected active ID %d, got %d", activeID, curActive)
	}
	for _, id := range sealed {
		if id >= activeID {
			t.Fatalf("compacted FileID %d >= active FileID %d", id, activeID)
		}
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("Close 2 failed: %v", err)
	}

	// Reopen again and verify all keys survive intact
	db3, err := Open(dir, WithMaxFileSize(128))
	if err != nil {
		t.Fatalf("Open 3 failed: %v", err)
	}
	defer db3.Close()

	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("k_%02d", i))
		want := bytes.Repeat([]byte{byte('a' + i%26)}, 80)
		got, err := db3.Get(k)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Get(%s) mismatch: err=%v", k, err)
		}
	}
	if got, err := db3.Get([]byte("k_active")); err != nil || !bytes.Equal(got, []byte("active_val")) {
		t.Fatalf("Get(k_active) mismatch: got=%q err=%v", got, err)
	}
}

func TestDB_Merge_MultiRoundCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(200))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Round 1: Populate, delete half, rotate, and Merge
	for i := 0; i < 30; i++ {
		if err := db.Put([]byte(fmt.Sprintf("r_%02d", i)), []byte("round1_payload_value")); err != nil {
			t.Fatalf("Put r1 failed: %v", err)
		}
	}
	for i := 0; i < 15; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("r_%02d", i))); err != nil {
			t.Fatalf("Delete r1 failed: %v", err)
		}
	}
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate r1 failed: %v", err)
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge 1 failed: %v", err)
	}

	// Round 2: Overwrite some, delete more, add new keys, rotate, and Merge again
	for i := 15; i < 22; i++ {
		if err := db.Put([]byte(fmt.Sprintf("r_%02d", i)), []byte("round2_updated_payload")); err != nil {
			t.Fatalf("Put r2 update failed: %v", err)
		}
	}
	for i := 22; i < 25; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("r_%02d", i))); err != nil {
			t.Fatalf("Delete r2 failed: %v", err)
		}
	}
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate r2 failed: %v", err)
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge 2 failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(200))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 15; i++ {
		if _, err := db2.Get([]byte(fmt.Sprintf("r_%02d", i))); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected r_%02d deleted, got %v", i, err)
		}
	}
	for i := 15; i < 22; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("r_%02d", i)))
		if err != nil || !bytes.Equal(got, []byte("round2_updated_payload")) {
			t.Fatalf("expected r_%02d updated, got=%q err=%v", i, got, err)
		}
	}
	for i := 22; i < 25; i++ {
		if _, err := db2.Get([]byte(fmt.Sprintf("r_%02d", i))); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected r_%02d deleted, got %v", i, err)
		}
	}
	for i := 25; i < 30; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("r_%02d", i)))
		if err != nil || !bytes.Equal(got, []byte("round1_payload_value")) {
			t.Fatalf("expected r_%02d untouched, got=%q err=%v", i, got, err)
		}
	}
}

func TestDB_Merge_CrashRecovery(t *testing.T) {
	tests := []struct {
		name       string
		setupCrash func(t *testing.T, dir string, oldIDs, newIDs []uint32)
		wantMerged bool // true if recovery should roll forward to newIDs; false if roll back to oldIDs
	}{
		{
			name: "crash before COMMITTED rolls back and discards .tmp",
			setupCrash: func(t *testing.T, dir string, oldIDs, newIDs []uint32) {
				// Simulate partial/corrupt bytes in .tmp/ without COMMITTED marker
				tmpDir := filepath.Join(dir, mergeTempDirName)
				if err := os.WriteFile(filepath.Join(tmpDir, storage.DataFileName(newIDs[0])), []byte("partial_garbage"), 0644); err != nil {
					t.Fatal(err)
				}
			},
			wantMerged: false,
		},
		{
			name: "crash right after COMMITTED finishes deleting old files and moves new files",
			setupCrash: func(t *testing.T, dir string, oldIDs, newIDs []uint32) {
				tmpDir := filepath.Join(dir, mergeTempDirName)
				if err := commitMerge(tmpDir, oldIDs, newIDs); err != nil {
					t.Fatal(err)
				}
			},
			wantMerged: true,
		},
		{
			name: "crash mid-unlink after COMMITTED finishes remaining deletes and moves new files",
			setupCrash: func(t *testing.T, dir string, oldIDs, newIDs []uint32) {
				tmpDir := filepath.Join(dir, mergeTempDirName)
				if err := commitMerge(tmpDir, oldIDs, newIDs); err != nil {
					t.Fatal(err)
				}
				// Delete only the first old file, leaving the remaining old files on disk
				if err := os.Remove(filepath.Join(dir, storage.DataFileName(oldIDs[0]))); err != nil {
					t.Fatal(err)
				}
			},
			wantMerged: true,
		},
		{
			name: "crash mid-move after OLD_DELETED does not delete already-moved new file",
			setupCrash: func(t *testing.T, dir string, oldIDs, newIDs []uint32) {
				tmpDir := filepath.Join(dir, mergeTempDirName)
				if err := commitMerge(tmpDir, oldIDs, newIDs); err != nil {
					t.Fatal(err)
				}
				for _, id := range oldIDs {
					if err := os.Remove(filepath.Join(dir, storage.DataFileName(id))); err != nil {
						t.Fatal(err)
					}
				}
				if err := createMarkerAtomic(tmpDir, mergeOldDeletedMarker); err != nil {
					t.Fatal(err)
				}
				// Move ONLY the first compacted file newIDs[0] into dir, leaving newIDs[1] in .tmp/
				name0 := storage.DataFileName(newIDs[0])
				if err := os.Rename(filepath.Join(tmpDir, name0), filepath.Join(dir, name0)); err != nil {
					t.Fatal(err)
				}
			},
			wantMerged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(dir, WithMaxFileSize(180))
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}

			// Populate sealed files with 6 live keys + 2 deleted keys, sized so compacted
			// output spans >= 2 files while active file holds an uncompacted key.
			for i := 0; i < 8; i++ {
				k := []byte(fmt.Sprintf("ckey_%02d", i))
				v := bytes.Repeat([]byte{byte('A' + i)}, 40)
				if err := db.Put(k, v); err != nil {
					t.Fatalf("Put %d failed: %v", i, err)
				}
			}
			for i := 0; i < 2; i++ {
				if err := db.Delete([]byte(fmt.Sprintf("ckey_%02d", i))); err != nil {
					t.Fatalf("Delete %d failed: %v", i, err)
				}
			}
			if err := db.storage.Rotate(); err != nil {
				t.Fatalf("Rotate failed: %v", err)
			}
			if err := db.Put([]byte("active_k"), []byte("active_v")); err != nil {
				t.Fatalf("Put active failed: %v", err)
			}

			oldIDs, activeStartID := db.storage.SealedSnapshot()
			if len(oldIDs) < 2 {
				t.Fatalf("expected >= 2 sealed files, got oldIDs=%v active=%d", oldIDs, activeStartID)
			}

			tmpDir := filepath.Join(dir, mergeTempDirName)
			_, newIDs, err := db.stageMergedFiles(tmpDir, oldIDs, activeStartID)
			if err != nil {
				t.Fatalf("stageMergedFiles failed: %v", err)
			}
			if len(newIDs) < 2 {
				t.Fatalf("expected >= 2 staged files for test setup, got newIDs=%v", newIDs)
			}

			// Close DB without running Phase 3/4 so we can inject the simulated crash state
			if err := db.Close(); err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			tt.setupCrash(t, dir, oldIDs, newIDs)

			// Reopen DB -> recoverMergeState must heal the interrupted state cleanly
			db2, err := Open(dir, WithMaxFileSize(180))
			if err != nil {
				t.Fatalf("Reopen after crash failed: %v", err)
			}
			defer db2.Close()

			if _, err := os.Stat(tmpDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("expected .tmp to be cleaned up after recovery, got err=%v", err)
			}

			var wantFiles []uint32
			if tt.wantMerged {
				wantFiles = append(wantFiles, newIDs...)
			} else {
				wantFiles = append(wantFiles, oldIDs...)
			}
			wantFiles = append(wantFiles, activeStartID)

			gotFiles := db2.storage.FileIDs()
			if len(gotFiles) != len(wantFiles) {
				t.Fatalf("FileIDs after recovery = %v, want %v", gotFiles, wantFiles)
			}
			for i := range gotFiles {
				if gotFiles[i] != wantFiles[i] {
					t.Fatalf("FileIDs after recovery = %v, want %v", gotFiles, wantFiles)
				}
			}

			for i := 0; i < 2; i++ {
				if _, err := db2.Get([]byte(fmt.Sprintf("ckey_%02d", i))); !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("expected deleted ckey_%02d not found, got %v", i, err)
				}
			}
			for i := 2; i < 8; i++ {
				k := []byte(fmt.Sprintf("ckey_%02d", i))
				want := bytes.Repeat([]byte{byte('A' + i)}, 40)
				got, err := db2.Get(k)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("Get(%s) mismatch after recovery: err=%v", k, err)
				}
			}
			if got, err := db2.Get([]byte("active_k")); err != nil || !bytes.Equal(got, []byte("active_v")) {
				t.Fatalf("Get(active_k) mismatch after recovery: got=%q err=%v", got, err)
			}
		})
	}
}

func TestDB_Merge_ConcurrentReadsWritesAndMerges(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	const numWriters = 4
	const keysPerWriter = 25
	const opsPerWriter = 120

	// Each writer owns a disjoint key partition so final ground-truth verification is exact.
	expected := make([][]string, numWriters)
	for w := 0; w < numWriters; w++ {
		expected[w] = make([]string, keysPerWriter)
	}

	var stopReaders atomic.Bool
	var wg sync.WaitGroup

	// Writers performing Put, Delete, and WriteBatch concurrently with Merge
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		writerID := w
		go func() {
			defer wg.Done()
			for step := 0; step < opsPerWriter; step++ {
				idx := step % keysPerWriter
				key := []byte(fmt.Sprintf("w%d_k%02d", writerID, idx))

				switch step % 5 {
				case 4:
					if expected[writerID][idx] != "" {
						if err := db.Delete(key); err != nil {
							t.Errorf("Delete(%s) failed: %v", key, err)
							return
						}
						expected[writerID][idx] = ""
					}
				case 3:
					nextIdx := (idx + 1) % keysPerWriter
					nextKey := []byte(fmt.Sprintf("w%d_k%02d", writerID, nextIdx))
					v1 := fmt.Sprintf("batch_%d_%d_a", writerID, step)
					v2 := fmt.Sprintf("batch_%d_%d_b", writerID, step)
					b := NewWriteBatch()
					_ = b.Put(key, []byte(v1))
					_ = b.Put(nextKey, []byte(v2))
					if err := db.WriteBatch(b); err != nil {
						t.Errorf("WriteBatch failed: %v", err)
						return
					}
					expected[writerID][idx] = v1
					expected[writerID][nextIdx] = v2
				default:
					val := fmt.Sprintf("val_%d_%d_payload_pad", writerID, step)
					if err := db.Put(key, []byte(val)); err != nil {
						t.Errorf("Put(%s) failed: %v", key, err)
						return
					}
					expected[writerID][idx] = val
				}
			}
		}()
	}

	// Concurrent readers verifying Get never hits ErrFileNotFound or CRC mismatch mid-Merge
	var auxWg sync.WaitGroup
	for r := 0; r < 4; r++ {
		auxWg.Add(1)
		readerID := r
		go func() {
			defer auxWg.Done()
			for i := 0; !stopReaders.Load(); i++ {
				k := []byte(fmt.Sprintf("w%d_k%02d", (readerID+i)%numWriters, i%keysPerWriter))
				_, err := db.Get(k)
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("concurrent Get(%s) unexpected error: %v", k, err)
					return
				}
				if i%20 == 0 {
					_ = db.Keys()
				}
			}
		}()
	}

	// Two concurrent callers triggering Merge() repeatedly
	for m := 0; m < 2; m++ {
		auxWg.Add(1)
		go func() {
			defer auxWg.Done()
			for !stopReaders.Load() {
				if err := db.Merge(); err != nil {
					t.Errorf("concurrent Merge failed: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
	stopReaders.Store(true)
	auxWg.Wait()

	// Final Merge + full ground-truth verification (live and after reopen)
	if err := db.Merge(); err != nil {
		t.Fatalf("final Merge failed: %v", err)
	}

	checkAll := func(target *DB, label string) {
		for w := 0; w < numWriters; w++ {
			for k := 0; k < keysPerWriter; k++ {
				key := []byte(fmt.Sprintf("w%d_k%02d", w, k))
				want := expected[w][k]
				got, err := target.Get(key)
				if want == "" {
					if !errors.Is(err, ErrKeyNotFound) {
						t.Fatalf("[%s] expected %s deleted, got=%q err=%v", label, key, got, err)
					}
				} else {
					if err != nil || string(got) != want {
						t.Fatalf("[%s] key %s mismatch: got=%q want=%q err=%v", label, key, got, want, err)
					}
				}
			}
		}
	}

	checkAll(db, "live")
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(512))
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer db2.Close()

	checkAll(db2, "reopened")
}
