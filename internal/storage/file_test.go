package storage

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenDataFile_NewAndExisting(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "001.data")

	// 1. Open new file (should be empty, offset 0)
	df, err := OpenDataFile(filePath)
	if err != nil {
		t.Fatalf("failed to open new data file: %v", err)
	}

	if s := df.Size(); s != 0 {
		t.Fatalf("expected initial size 0, got %d", s)
	}

	// Write 100 bytes
	payload := bytes.Repeat([]byte("a"), 100)
	off, err := df.Write(payload)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if off != 0 {
		t.Fatalf("expected initial write offset 0, got %d", off)
	}
	if err := df.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 2. Re-open existing file (should detect existing offset = 100)
	df2, err := OpenDataFile(filePath)
	if err != nil {
		t.Fatalf("failed to re-open existing data file: %v", err)
	}
	defer df2.Close()

	if s := df2.Size(); s != 100 {
		t.Fatalf("expected re-opened file to have size 100, got %d", s)
	}

	// Next write should append at offset 100
	off2, err := df2.Write([]byte("more data"))
	if err != nil {
		t.Fatalf("second Write failed: %v", err)
	}
	if off2 != 100 {
		t.Fatalf("expected second write offset 100, got %d", off2)
	}
}

func TestWriteAndReadAt(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "002.data")

	df, err := OpenDataFile(filePath)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}
	defer df.Close()

	chunks := [][]byte{
		[]byte("hello world"),
		[]byte("bigcat bitcask storage engine"),
		[]byte("another piece of data"),
	}

	offsets := make([]int64, len(chunks))

	// Write all chunks
	for i, chunk := range chunks {
		off, err := df.Write(chunk)
		if err != nil {
			t.Fatalf("Write chunk %d failed: %v", i, err)
		}
		offsets[i] = off
	}

	// Verify sequential offsets
	var expectedOff int64 = 0
	for i, chunk := range chunks {
		if offsets[i] != expectedOff {
			t.Errorf("expected offset %d for chunk %d, got %d", expectedOff, i, offsets[i])
		}
		expectedOff += int64(len(chunk))
	}

	// Read each chunk back via ReadAt (in reverse order to verify random access)
	for i := len(chunks) - 1; i >= 0; i-- {
		readBuf, err := df.ReadAt(offsets[i], uint32(len(chunks[i])))
		if err != nil {
			t.Fatalf("ReadAt chunk %d failed: %v", i, err)
		}
		if !bytes.Equal(readBuf, chunks[i]) {
			t.Fatalf("ReadAt chunk %d data mismatch: expected %q, got %q", i, chunks[i], readBuf)
		}
	}
}

func TestReadAt_OutOfBounds(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "003.data")

	df, err := OpenDataFile(filePath)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}
	defer df.Close()

	if _, err := df.Write([]byte("short")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read beyond EOF
	_, err = df.ReadAt(100, 10)
	if err == nil {
		t.Fatalf("expected error reading beyond EOF, got nil")
	}
	if err != io.EOF {
		t.Logf("ReadAt beyond EOF returned: %v (acceptable)", err)
	}
}

func TestDataFile_SyncAndClose(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "004.data")

	df, err := OpenDataFile(filePath)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}

	if _, err := df.Write([]byte("durability test")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := df.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	if err := df.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestDataFile_ConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "concurrent.data")

	df, err := OpenDataFile(filePath)
	if err != nil {
		t.Fatalf("OpenDataFile failed: %v", err)
	}
	defer df.Close()

	var wg sync.WaitGroup
	numWriters := 10
	writesPerWorker := 50

	// Concurrent writes
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				data := []byte(fmt.Sprintf("w:%d-msg:%d\n", workerID, i))
				off, err := df.Write(data)
				if err != nil {
					t.Errorf("concurrent write failed: %v", err)
					return
				}

				// Immediately read back the written chunk
				readBack, err := df.ReadAt(off, uint32(len(data)))
				if err != nil {
					t.Errorf("concurrent read back failed: %v", err)
					return
				}
				if !bytes.Equal(readBack, data) {
					t.Errorf("concurrent data mismatch: expected %s, got %s", data, readBack)
					return
				}
			}
		}()
	}

	wg.Wait()

	expectedSize := int64(numWriters * writesPerWorker)
	if df.Size() == 0 || df.Size() < expectedSize {
		t.Fatalf("expected non-zero size > %d, got %d", expectedSize, df.Size())
	}
}

func BenchmarkDataFile_Write(b *testing.B) {
	tempDir := b.TempDir()
	filePath := filepath.Join(tempDir, "bench_write.data")

	df, err := OpenDataFile(filePath)
	if err != nil {
		b.Fatalf("OpenDataFile failed: %v", err)
	}
	defer df.Close()

	payload := bytes.Repeat([]byte("x"), 256)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := df.Write(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDataFile_ReadAt(b *testing.B) {
	tempDir := b.TempDir()
	filePath := filepath.Join(tempDir, "bench_read.data")

	df, err := OpenDataFile(filePath)
	if err != nil {
		b.Fatalf("OpenDataFile failed: %v", err)
	}
	defer df.Close()

	payload := bytes.Repeat([]byte("x"), 256)
	off, err := df.Write(payload)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := df.ReadAt(off, 256)
		if err != nil {
			b.Fatal(err)
		}
	}
}
