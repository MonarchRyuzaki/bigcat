package bigcat

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"bigcat/internal/index"
	"bigcat/internal/storage"
)

func TestDB_HintFiles_GenerationBitForBitParityAndFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(256), WithSyncOnWrite(false))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Populate keys across multiple rotated files with overwrites and deletes
	for i := 0; i < 60; i++ {
		if err := db.Put([]byte(fmt.Sprintf("hkey_%03d", i)), []byte(fmt.Sprintf("v1_%03d_padding_bytes", i))); err != nil {
			t.Fatalf("Put %d failed: %v", i, err)
		}
	}
	for i := 0; i < 30; i++ {
		if err := db.Put([]byte(fmt.Sprintf("hkey_%03d", i)), []byte(fmt.Sprintf("v2_%03d_updated_payload", i))); err != nil {
			t.Fatalf("Overwrite %d failed: %v", i, err)
		}
	}
	for i := 30; i < 45; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("hkey_%03d", i))); err != nil {
			t.Fatalf("Delete %d failed: %v", i, err)
		}
	}

	// Also put an active-file key after rotating so active file has no .hint
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if err := db.Put([]byte("active_unmerged"), []byte("active_val")); err != nil {
		t.Fatalf("Put active failed: %v", err)
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	sealedIDs, activeID := db.storage.SealedSnapshot()
	if len(sealedIDs) == 0 {
		t.Fatalf("expected sealed compacted files after Merge")
	}
	for _, id := range sealedIDs {
		hintPath := filepath.Join(dir, storage.HintFileName(id))
		if _, err := os.Stat(hintPath); err != nil {
			t.Fatalf("expected companion hint file %s to exist: %v", hintPath, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, storage.HintFileName(activeID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("did not expect .hint file for active file %d, got err=%v", activeID, err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 1. Reopen using .hint files (fast path) and snapshot KeyDir items
	dbHint, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Open with hints failed: %v", err)
	}
	hintItems := make(map[string]index.Item)
	for _, k := range dbHint.Keys() {
		item, ok := dbHint.index.Get(k)
		if !ok {
			t.Fatalf("missing key %s in hint KeyDir", k)
		}
		hintItems[k] = item
	}
	if err := dbHint.Close(); err != nil {
		t.Fatalf("Close dbHint failed: %v", err)
	}

	// 2. Delete all .hint files to force raw .data scan fallback and verify bit-for-bit KeyDir parity
	for _, id := range sealedIDs {
		if err := os.Remove(filepath.Join(dir, storage.HintFileName(id))); err != nil {
			t.Fatalf("Remove hint %d failed: %v", id, err)
		}
	}

	dbRaw, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Open fallback raw scan failed: %v", err)
	}
	defer dbRaw.Close()

	rawKeys := dbRaw.Keys()
	if len(rawKeys) != len(hintItems) {
		t.Fatalf("key count mismatch: hint=%d raw=%d", len(hintItems), len(rawKeys))
	}
	for _, k := range rawKeys {
		rawItem, ok := dbRaw.index.Get(k)
		if !ok {
			t.Fatalf("missing key %s in raw KeyDir", k)
		}
		if hintItems[k] != rawItem {
			t.Fatalf("KeyDir bit-for-bit mismatch for %s: hint=%+v raw=%+v", k, hintItems[k], rawItem)
		}
	}

	// Verify actual values and deleted tombstones remain accurate
	for i := 0; i < 30; i++ {
		want := []byte(fmt.Sprintf("v2_%03d_updated_payload", i))
		got, err := dbRaw.Get([]byte(fmt.Sprintf("hkey_%03d", i)))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Get(hkey_%03d) mismatch: got=%q err=%v", i, got, err)
		}
	}
	for i := 30; i < 45; i++ {
		if _, err := dbRaw.Get([]byte(fmt.Sprintf("hkey_%03d", i))); !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("expected deleted hkey_%03d not found, got %v", i, err)
		}
	}
}

func TestDB_HintFiles_CorruptHintDetectedOnStartup(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(200), WithSyncOnWrite(false))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k_%02d", i)), []byte("payload_value_12345")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	sealed, _ := db.storage.SealedSnapshot()
	if len(sealed) == 0 {
		t.Fatalf("expected at least 1 sealed file")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Tamper with a byte in the companion .hint file
	hintPath := filepath.Join(dir, storage.HintFileName(sealed[0]))
	rawHint, err := os.ReadFile(hintPath)
	if err != nil {
		t.Fatalf("ReadFile hint failed: %v", err)
	}
	rawHint[len(rawHint)-1] ^= 0xFF
	if err := os.WriteFile(hintPath, rawHint, 0644); err != nil {
		t.Fatalf("WriteFile tampered hint failed: %v", err)
	}

	if _, err := Open(dir, WithMaxFileSize(200)); err == nil {
		t.Fatalf("expected Open to fail on corrupted .hint CRC, got nil")
	}
}

func TestDB_HintFiles_OrphanHintsCleanedAcrossMultiMergeAndZeroKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(160), WithSyncOnWrite(false))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := 0; i < 16; i++ {
		if err := db.Put([]byte(fmt.Sprintf("ok_%02d", i)), []byte("value_padding_0123456789")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate 1 failed: %v", err)
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge 1 failed: %v", err)
	}

	sealedAfter1, _ := db.storage.SealedSnapshot()
	if len(sealedAfter1) < 2 {
		t.Fatalf("expected >= 2 sealed files after Merge 1, got %v", sealedAfter1)
	}

	// Delete ALL keys, rotate, and Merge again -> zero surviving keys, all .hint files must be gone
	for i := 0; i < 16; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("ok_%02d", i))); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate 2 failed: %v", err)
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge 2 failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	for _, id := range sealedAfter1 {
		if _, err := os.Stat(filepath.Join(dir, storage.HintFileName(id))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected old hint %d to be deleted after zero-key Merge, err=%v", id, err)
		}
	}
}

func TestDB_HintFiles_CrashMidRenameMovesHintOnRecovery(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, WithMaxFileSize(256), WithSyncOnWrite(false))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	for i := 0; i < 6; i++ {
		if err := db.Put([]byte(fmt.Sprintf("ck_%d", i)), []byte("val_payload_1234")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := db.storage.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	oldIDs, activeID := db.storage.SealedSnapshot()
	tmpDir := filepath.Join(dir, mergeTempDirName)
	_, newIDs, err := db.stageMergedFiles(tmpDir, oldIDs, activeID)
	if err != nil {
		t.Fatalf("stageMergedFiles failed: %v", err)
	}
	if len(newIDs) == 0 {
		t.Fatalf("expected non-empty newIDs")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := commitMerge(tmpDir, oldIDs, newIDs); err != nil {
		t.Fatalf("commitMerge failed: %v", err)
	}
	for _, id := range oldIDs {
		_ = os.Remove(filepath.Join(dir, storage.DataFileName(id)))
		_ = os.Remove(filepath.Join(dir, storage.HintFileName(id)))
	}
	if err := createMarkerAtomic(tmpDir, mergeOldDeletedMarker); err != nil {
		t.Fatalf("createMarkerAtomic failed: %v", err)
	}

	// Simulate crash AFTER moving newIDs[0].data but BEFORE moving newIDs[0].hint
	dataName := storage.DataFileName(newIDs[0])
	if err := os.Rename(filepath.Join(tmpDir, dataName), filepath.Join(dir, dataName)); err != nil {
		t.Fatalf("Rename .data failed: %v", err)
	}

	db2, err := Open(dir, WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Recovery Open failed: %v", err)
	}
	defer db2.Close()

	// Both .hint and .data must exist in dir, and .tmp must be gone
	if _, err := os.Stat(filepath.Join(dir, storage.HintFileName(newIDs[0]))); err != nil {
		t.Fatalf("expected recovered .hint file to exist: %v", err)
	}
	if _, err := os.Stat(tmpDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected .tmp removed, got %v", err)
	}
	for i := 0; i < 6; i++ {
		got, err := db2.Get([]byte(fmt.Sprintf("ck_%d", i)))
		if err != nil || !bytes.Equal(got, []byte("val_payload_1234")) {
			t.Fatalf("Get(ck_%d) mismatch: got=%q err=%v", i, got, err)
		}
	}
}

func setupColdStartupBenchDir(b *testing.B, withHints bool) string {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(dir, WithMaxFileSize(4*1024*1024), WithSyncOnWrite(false))
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}

	// 4,000 keys x 4KB values (~16MB) across multiple rotated files
	val := bytes.Repeat([]byte("v"), 4096)
	batch := NewWriteBatch()
	for i := 0; i < 4000; i++ {
		_ = batch.Put([]byte(fmt.Sprintf("bench_cold_key_%05d", i)), val)
		if batch.Size() == 100 {
			if err := db.WriteBatch(batch); err != nil {
				b.Fatalf("WriteBatch failed: %v", err)
			}
			batch.Clear()
		}
	}
	if err := db.storage.Rotate(); err != nil {
		b.Fatalf("Rotate failed: %v", err)
	}
	if withHints {
		if err := db.Merge(); err != nil {
			b.Fatalf("Merge failed: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatalf("Close failed: %v", err)
	}
	return dir
}

func BenchmarkDB_ColdStartup_RawScan(b *testing.B) {
	dir := setupColdStartupBenchDir(b, false)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db, err := Open(dir, WithMaxFileSize(4*1024*1024))
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		_ = db.Close()
		b.StartTimer()
	}
}

func BenchmarkDB_ColdStartup_WithHints(b *testing.B) {
	dir := setupColdStartupBenchDir(b, true)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db, err := Open(dir, WithMaxFileSize(4*1024*1024))
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		_ = db.Close()
		b.StartTimer()
	}
}
