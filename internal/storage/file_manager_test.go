package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

func TestParseFileID_And_DataFileName(t *testing.T) {
	tests := []struct {
		name      string
		id        uint32
		fileName  string
		expectErr bool
	}{
		{name: "zero ID", id: 0, fileName: "00000.data", expectErr: false},
		{name: "small ID", id: 42, fileName: "00042.data", expectErr: false},
		{name: "large ID", id: 99999, fileName: "99999.data", expectErr: false},
		{name: "non-padded ID parses correctly", id: 123, fileName: "123.data", expectErr: false},
		{name: "invalid extension", id: 0, fileName: "00001.txt", expectErr: true},
		{name: "non-numeric name", id: 0, fileName: "notanumber.data", expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.expectErr {
				formatted := DataFileName(tt.id)
				if tt.id <= 99999 && formatted != tt.fileName && tt.fileName == fmt.Sprintf(DataFileFormat, tt.id) {
					t.Errorf("expected DataFileName(%d) = %s, got %s", tt.id, tt.fileName, formatted)
				}
			}

			parsedID, err := ParseFileID(tt.fileName)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error parsing %s, got nil", tt.fileName)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error parsing %s: %v", tt.fileName, err)
				}
				if parsedID != tt.id {
					t.Errorf("expected parsed ID %d, got %d", tt.id, parsedID)
				}
			}
		})
	}
}

func TestFileManager_OpenEmptyDir(t *testing.T) {
	dir := t.TempDir()

	fm, err := OpenFileManager(dir, 1024)
	if err != nil {
		t.Fatalf("failed to open file manager on empty dir: %v", err)
	}
	defer fm.Close()

	if fm.ActiveFile() == nil {
		t.Fatalf("expected active file to be created, got nil")
	}
	if fm.ActiveFile().FileID() != 0 {
		t.Fatalf("expected active file ID 0, got %d", fm.ActiveFile().FileID())
	}

	ids := fm.FileIDs()
	if len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("expected FileIDs [0], got %v", ids)
	}
}

func TestFileManager_WriteAndAutoRotation(t *testing.T) {
	dir := t.TempDir()

	// Max file size: 100 bytes
	fm, err := OpenFileManager(dir, 100)
	if err != nil {
		t.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	// Write 60 bytes -> fits in 00000.data
	chunk1 := bytes.Repeat([]byte("a"), 60)
	fID1, off1, err := fm.Write(chunk1)
	if err != nil {
		t.Fatalf("Write 1 failed: %v", err)
	}
	if fID1 != 0 || off1 != 0 {
		t.Fatalf("expected fID=0, off=0, got fID=%d, off=%d", fID1, off1)
	}

	// Write 50 bytes -> 60 + 50 = 110 > 100 -> triggers auto-rotation to 00001.data!
	chunk2 := bytes.Repeat([]byte("b"), 50)
	fID2, off2, err := fm.Write(chunk2)
	if err != nil {
		t.Fatalf("Write 2 failed: %v", err)
	}
	if fID2 != 1 || off2 != 0 {
		t.Fatalf("expected auto-rotation to fID=1, off=0, got fID=%d, off=%d", fID2, off2)
	}

	// Write another 40 bytes -> fits in 00001.data at offset 50
	chunk3 := bytes.Repeat([]byte("c"), 40)
	fID3, off3, err := fm.Write(chunk3)
	if err != nil {
		t.Fatalf("Write 3 failed: %v", err)
	}
	if fID3 != 1 || off3 != 50 {
		t.Fatalf("expected fID=1, off=50, got fID=%d, off=%d", fID3, off3)
	}

	// Verify FileIDs
	ids := fm.FileIDs()
	expectedIDs := []uint32{0, 1}
	if !slices.Equal(ids, expectedIDs) {
		t.Fatalf("expected FileIDs %v, got %v", expectedIDs, ids)
	}

	// Read back via ReadAt across both files
	read1, err := fm.ReadAt(fID1, off1, uint32(len(chunk1)))
	if err != nil || !bytes.Equal(read1, chunk1) {
		t.Fatalf("ReadAt chunk1 failed: err=%v, match=%v", err, bytes.Equal(read1, chunk1))
	}

	read2, err := fm.ReadAt(fID2, off2, uint32(len(chunk2)))
	if err != nil || !bytes.Equal(read2, chunk2) {
		t.Fatalf("ReadAt chunk2 failed: err=%v, match=%v", err, bytes.Equal(read2, chunk2))
	}

	read3, err := fm.ReadAt(fID3, off3, uint32(len(chunk3)))
	if err != nil || !bytes.Equal(read3, chunk3) {
		t.Fatalf("ReadAt chunk3 failed: err=%v, match=%v", err, bytes.Equal(read3, chunk3))
	}
}

func TestFileManager_ManualRotation(t *testing.T) {
	dir := t.TempDir()

	fm, err := OpenFileManager(dir, 1024)
	if err != nil {
		t.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	if _, _, err := fm.Write([]byte("file zero")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := fm.Rotate(); err != nil {
		t.Fatalf("Rotate failed: %v", err)
	}

	if fm.ActiveFile().FileID() != 1 {
		t.Fatalf("expected active file ID 1 after Rotate, got %d", fm.ActiveFile().FileID())
	}

	if _, _, err := fm.Write([]byte("file one")); err != nil {
		t.Fatalf("Write after rotate failed: %v", err)
	}

	ids := fm.FileIDs()
	if !slices.Equal(ids, []uint32{0, 1}) {
		t.Fatalf("expected FileIDs [0, 1], got %v", ids)
	}
}

func TestFileManager_ReopenDirectory(t *testing.T) {
	dir := t.TempDir()

	// 1. Create multiple files
	fm1, err := OpenFileManager(dir, 50)
	if err != nil {
		t.Fatalf("OpenFileManager 1 failed: %v", err)
	}

	chunkA := []byte("data in file 0")
	chunkB := []byte("data in file 1, rotating again")
	chunkC := []byte("data in active file 2")

	f1, _, _ := fm1.Write(chunkA)
	_ = fm1.Rotate()
	f2, _, _ := fm1.Write(chunkB)
	_ = fm1.Rotate()
	f3, _, _ := fm1.Write(chunkC)

	if err := fm1.Close(); err != nil {
		t.Fatalf("Close 1 failed: %v", err)
	}

	// 2. Reopen existing directory
	fm2, err := OpenFileManager(dir, 1024)
	if err != nil {
		t.Fatalf("OpenFileManager 2 failed: %v", err)
	}
	defer fm2.Close()

	// Check active file is f3 (ID 2)
	if fm2.ActiveFile().FileID() != f3 {
		t.Fatalf("expected active file ID %d, got %d", f3, fm2.ActiveFile().FileID())
	}

	// Check all IDs recognized in order
	ids := fm2.FileIDs()
	if !slices.Equal(ids, []uint32{f1, f2, f3}) {
		t.Fatalf("expected FileIDs [%d, %d, %d], got %v", f1, f2, f3, ids)
	}

	// Verify reads from older immutable files (f1 and f2)
	readA, err := fm2.ReadAt(f1, 0, uint32(len(chunkA)))
	if err != nil || !bytes.Equal(readA, chunkA) {
		t.Fatalf("ReadAt older file 1 failed: err=%v, match=%v", err, bytes.Equal(readA, chunkA))
	}

	readB, err := fm2.ReadAt(f2, 0, uint32(len(chunkB)))
	if err != nil || !bytes.Equal(readB, chunkB) {
		t.Fatalf("ReadAt older file 2 failed: err=%v, match=%v", err, bytes.Equal(readB, chunkB))
	}

	// Verify active file is writable (opened with readOnly = false)
	moreChunk := []byte("more data appended to active file 2")
	fID, _, err := fm2.Write(moreChunk)
	if err != nil {
		t.Fatalf("Write to reopened active file failed: %v", err)
	}
	if fID != f3 {
		t.Fatalf("expected write to go to active file %d, got %d", f3, fID)
	}
}

func TestFileManager_ReadAt_Errors(t *testing.T) {
	dir := t.TempDir()

	fm, err := OpenFileManager(dir, 1024)
	if err != nil {
		t.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	// Non-existent file ID
	_, err = fm.ReadAt(999, 0, 10)
	if err != ErrFileNotFound {
		t.Fatalf("expected ErrFileNotFound, got %v", err)
	}
}

func TestFileManager_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()

	// Small threshold to force multiple concurrent rotations
	fm, err := OpenFileManager(dir, 256)
	if err != nil {
		t.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	const numWriters = 8
	const writesPerWorker = 50
	const numReaders = 16

	type recordLoc struct {
		fID uint32
		off int64
		len uint32
		val []byte
	}

	var mu sync.Mutex
	var writtenRecords []recordLoc

	var wg sync.WaitGroup

	// Start writers
	wg.Add(numWriters)
	for w := 0; w < numWriters; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				payload := []byte(fmt.Sprintf("worker-%d-write-%d-padding-data", workerID, i))
				fID, off, err := fm.Write(payload)
				if err != nil {
					t.Errorf("concurrent Write failed: %v", err)
					return
				}

				mu.Lock()
				writtenRecords = append(writtenRecords, recordLoc{
					fID: fID,
					off: off,
					len: uint32(len(payload)),
					val: payload,
				})
				mu.Unlock()
			}
		}(w)
	}

	// Start readers concurrently reading while writers append and rotate
	stopReaders := make(chan struct{})
	var readerWg sync.WaitGroup
	readerWg.Add(numReaders)
	for r := 0; r < numReaders; r++ {
		go func() {
			defer readerWg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					mu.Lock()
					count := len(writtenRecords)
					var rec recordLoc
					if count > 0 {
						rec = writtenRecords[count-1]
					}
					mu.Unlock()

					if rec.len > 0 {
						data, err := fm.ReadAt(rec.fID, rec.off, rec.len)
						if err != nil {
							t.Errorf("concurrent ReadAt failed: %v", err)
							return
						}
						if !bytes.Equal(data, rec.val) {
							t.Errorf("concurrent ReadAt data mismatch: expected %q, got %q", rec.val, data)
							return
						}
					}
				}
			}
		}()
	}

	// Wait for writers to finish
	wg.Wait()
	close(stopReaders)
	readerWg.Wait()

	// Verify all written records can be read back accurately
	for _, rec := range writtenRecords {
		data, err := fm.ReadAt(rec.fID, rec.off, rec.len)
		if err != nil {
			t.Fatalf("final ReadAt verification failed for fID=%d off=%d: %v", rec.fID, rec.off, err)
		}
		if !bytes.Equal(data, rec.val) {
			t.Fatalf("final ReadAt data mismatch: expected %q, got %q", rec.val, data)
		}
	}
}

func BenchmarkFileManager_Write(b *testing.B) {
	dir := b.TempDir()
	// 64MB rotation threshold
	fm, err := OpenFileManager(dir, 64*1024*1024)
	if err != nil {
		b.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	payload := bytes.Repeat([]byte("v"), 128)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, _, err := fm.Write(payload); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkFileManager_ReadAt(b *testing.B) {
	dir := b.TempDir()
	// 512-byte threshold to create multiple older files
	fm, err := OpenFileManager(dir, 512)
	if err != nil {
		b.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	payload := bytes.Repeat([]byte("r"), 128)
	const count = 50
	type loc struct {
		fID uint32
		off int64
	}
	locs := make([]loc, count)
	for i := 0; i < count; i++ {
		fID, off, err := fm.Write(payload)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		locs[i] = loc{fID: fID, off: off}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		l := locs[i%count]
		if _, err := fm.ReadAt(l.fID, l.off, uint32(len(payload))); err != nil {
			b.Fatalf("ReadAt failed: %v", err)
		}
	}
}

func TestFileManager_SealedSnapshot(t *testing.T) {
	dir := t.TempDir()
	fm, err := OpenFileManager(dir, 1024)
	if err != nil {
		t.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	// Single active file (0) -> no sealed files
	sealed, active := fm.SealedSnapshot()
	if len(sealed) != 0 || active != 0 {
		t.Fatalf("expected sealed=[], active=0; got sealed=%v, active=%d", sealed, active)
	}

	// Rotate 3 times -> olderFiles=[0, 1, 2], active=3
	for i := 0; i < 3; i++ {
		if err := fm.Rotate(); err != nil {
			t.Fatalf("Rotate %d failed: %v", i, err)
		}
	}

	sealed, active = fm.SealedSnapshot()
	if active != 3 {
		t.Fatalf("expected active=3, got %d", active)
	}
	if !slices.Equal(sealed, []uint32{0, 1, 2}) {
		t.Fatalf("expected sealed=[0, 1, 2], got %v", sealed)
	}
}

func TestFileManager_CloseAndDeleteOlderFiles_And_AdoptOlderFiles(t *testing.T) {
	dir := t.TempDir()
	fm, err := OpenFileManager(dir, 50)
	if err != nil {
		t.Fatalf("OpenFileManager failed: %v", err)
	}
	defer fm.Close()

	// Create files 0, 1, 2 (sealed) and 3 (active)
	for i := 0; i < 4; i++ {
		if _, _, err := fm.Write(bytes.Repeat([]byte{byte('a' + i)}, 40)); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	sealed, active := fm.SealedSnapshot()
	if active != 3 || !slices.Equal(sealed, []uint32{0, 1, 2}) {
		t.Fatalf("unexpected snapshot: sealed=%v active=%d", sealed, active)
	}

	// Delete 0, 1, 2 plus a missing ID 99 (must succeed idempotently and delete all 3)
	if err := fm.CloseAndDeleteOlderFiles([]uint32{0, 1, 2, 99}); err != nil {
		t.Fatalf("CloseAndDeleteOlderFiles failed: %v", err)
	}

	for _, id := range []uint32{0, 1, 2} {
		if _, err := os.Stat(filepath.Join(dir, DataFileName(id))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("expected file %d to be deleted, stat err=%v", id, err)
		}
		if _, err := fm.ReadAt(id, 0, 10); !errors.Is(err, ErrFileNotFound) {
			t.Fatalf("expected ErrFileNotFound for deleted file %d, got %v", id, err)
		}
	}

	// Active file 3 must remain intact and readable
	gotActive, err := fm.ReadAt(3, 0, 40)
	if err != nil || !bytes.Equal(gotActive, bytes.Repeat([]byte("d"), 40)) {
		t.Fatalf("active file 3 corrupted or unreadable: err=%v", err)
	}

	// Create replacement 00000.data externally and adopt it
	replacement := []byte("compacted_replacement_payload")
	if err := os.WriteFile(filepath.Join(dir, DataFileName(0)), replacement, 0644); err != nil {
		t.Fatalf("write replacement failed: %v", err)
	}

	// Adopting [0, 999] should fail and roll back cleanly without registering 0
	if err := fm.AdoptOlderFiles([]uint32{0, 999}); err == nil {
		t.Fatalf("expected AdoptOlderFiles to fail for missing ID 999")
	}
	if _, err := fm.ReadAt(0, 0, 5); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("expected failed AdoptOlderFiles to roll back ID 0, got %v", err)
	}

	// Adopting valid [0] should register it cleanly
	if err := fm.AdoptOlderFiles([]uint32{0}); err != nil {
		t.Fatalf("AdoptOlderFiles([0]) failed: %v", err)
	}
	gotAdopted, err := fm.ReadAt(0, 0, uint32(len(replacement)))
	if err != nil || !bytes.Equal(gotAdopted, replacement) {
		t.Fatalf("ReadAt on adopted file 0 mismatch: err=%v, got=%q", err, gotAdopted)
	}
}

