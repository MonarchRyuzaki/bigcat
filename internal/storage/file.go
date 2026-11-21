package storage

import (
	"os"
	"sync"
)

// DataFile represents an append-only file managed by BigCat.
// Writes always append to the end of the file; reads can happen concurrently at any offset.
type DataFile struct {
	mu     sync.RWMutex
	file   *os.File
	offset int64 // Current end of file / write offset
}

// OpenDataFile opens or creates a data file at the specified path.
func OpenDataFile(path string) (*DataFile, error) {
	// DONE:
	// 1. Open file with os.O_CREATE|os.O_RDWR|os.O_APPEND
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	// 2. Query file stat to get the initial offset (file size)
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close() // Guard against file descriptor leak
		return nil, err
	}
	offset := stat.Size()
	// 3. Return initialized *DataFile
	return &DataFile{
		file:   file,
		offset: offset,
	}, nil
}

// Write appends raw bytes to the end of the data file and returns the offset
// where the bytes were written.
func (df *DataFile) Write(data []byte) (offset int64, err error) {
	// DONE:
	// 1. Acquire write lock
	df.mu.Lock()
	defer df.mu.Unlock()
	// 2. Write data to file
	n, err := df.file.Write(data)
	if err != nil {
		return 0, err
	}
	// 3. Update df.offset
	off := df.offset
	df.offset += int64(n)
	// 4. Return the offset where this record began
	return off, nil
}

// ReadAt reads length bytes starting at the given offset.
// This operation does not move the file write pointer and is safe for concurrent calls.
func (df *DataFile) ReadAt(offset int64, length uint32) ([]byte, error) {
	// DONE:
	// 1. Allocate buffer of size length
	buffer := make([]byte, length)
	// 2. Call df.file.ReadAt(buf, offset)
	_, err := df.file.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	// 3. Return buffer
	return buffer, nil
}

// Sync commits the current contents of the file to stable storage (fsync).
func (df *DataFile) Sync() error {
	// DONE: Call df.file.Sync()
	err := df.file.Sync()
	return err
}

// Size returns the current size (offset) of the data file.
func (df *DataFile) Size() int64 {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.offset
}

// Close flushes and closes the underlying OS file.
func (df *DataFile) Close() error {
	df.mu.Lock()
	defer df.mu.Unlock()
	return df.file.Close()
}
