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
	// TODO:
	// 1. Open file with os.O_CREATE|os.O_RDWR|os.O_APPEND
	// 2. Query file stat to get the initial offset (file size)
	// 3. Return initialized *DataFile
	panic("TODO: implement OpenDataFile")
}

// Write appends raw bytes to the end of the data file and returns the offset
// where the bytes were written.
func (df *DataFile) Write(data []byte) (offset int64, err error) {
	// TODO:
	// 1. Acquire write lock
	// 2. Write data to file
	// 3. Update df.offset
	// 4. Return the offset where this record began
	panic("TODO: implement Write")
}

// ReadAt reads length bytes starting at the given offset.
// This operation does not move the file write pointer and is safe for concurrent calls.
func (df *DataFile) ReadAt(offset int64, length uint32) ([]byte, error) {
	// TODO:
	// 1. Allocate buffer of size length
	// 2. Call df.file.ReadAt(buf, offset)
	// 3. Return buffer
	panic("TODO: implement ReadAt")
}

// Sync commits the current contents of the file to stable storage (fsync).
func (df *DataFile) Sync() error {
	// TODO: Call df.file.Sync()
	panic("TODO: implement Sync")
}

// Size returns the current size (offset) of the data file.
func (df *DataFile) Size() int64 {
	df.mu.RLock()
	defer df.mu.RUnlock()
	return df.offset
}

// Close flushes and closes the underlying OS file.
func (df *DataFile) Close() error {
	// TODO: Call df.file.Close()
	panic("TODO: implement Close")
}
