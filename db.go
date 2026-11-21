package bigcat

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"

	"bigcat/internal/index"
	"bigcat/internal/record"
	"bigcat/internal/storage"
)

const (
	// defaultDataFileName is the filename of the single active data file for Phase 1.
	defaultDataFileName = "data.db"
)

// DB represents a BigCat key-value storage engine instance.
// It coordinates an append-only DataFile on disk with an in-memory KeyDir index.
type DB struct {
	mu       sync.RWMutex
	options  Options
	active   *storage.DataFile
	index    index.Indexer
	codec    record.Codec
	isClosed bool
}

// Open initializes and opens a BigCat storage engine instance in the specified directory.
// On startup, it scans the active data file sequentially to rebuild the in-memory KeyDir.
func Open(dirPath string, opts ...Option) (*DB, error) {
	// 1. Initialize Options (apply defaults and user-provided functional options)
	options := DefaultOptions(dirPath)
	for _, opt := range opts {
		opt(&options)
	}

	// 2. Ensure data directory exists with standard directory permissions (0755)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	// 3. Open the active data file in dirPath (e.g., data.db) via storage.OpenDataFile
	dataFilePath := filepath.Join(dirPath, defaultDataFileName)
	dataFile, err := storage.OpenDataFile(dataFilePath)
	if err != nil {
		return nil, err
	}

	// 4. Initialize Indexer and Codec
	idx := index.NewMapIndex()
	cdc := record.NewBinaryCodec()

	db := &DB{
		options: options,
		active:  dataFile,
		index:   idx,
		codec:   cdc,
	}

	// 5. Build in-memory KeyDir by sequentially scanning the data file from offset 0
	if err := db.loadIndexFromDisk(dataFilePath); err != nil {
		_ = dataFile.Close()
		_ = idx.Close()
		return nil, err
	}

	return db, nil
}

// loadIndexFromDisk scans dataFilePath from byte 0 to EOF to recover the in-memory KeyDir index.
// It wraps the file in a buffered reader (bufio.Reader) to minimize disk syscalls.
func (db *DB) loadIndexFromDisk(dataFilePath string) error {
	file, err := os.Open(dataFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Use a 256KB buffer so sequential reads happen in userspace memory
	reader := bufio.NewReaderSize(file, 256*1024)
	var currentOffset int64 = 0

	for {
		rec, err := db.codec.Decode(reader)
		if err == io.EOF {
			// Clean end of file reached
			break
		}
		if err != nil {
			return err
		}

		// Calculate the total size of the record on disk:
		// HeaderSize (21 bytes) + len(key) + len(value)
		recordSize := uint32(record.HeaderSize + len(rec.Key) + len(rec.Value))

		// If it's a tombstone, remove the key from KeyDir
		if rec.Type == record.RecordTypeTombstone {
			db.index.Delete(string(rec.Key))
		} else if rec.Type == record.RecordTypeNormal {
			// Populate or overwrite index with the latest active offset
			db.index.Put(string(rec.Key), index.Item{
				RecordOffset: currentOffset,
				RecordSize:   recordSize,
				Timestamp:    rec.Timestamp,
			})
		}

		// Advance offset to the start of the next record
		currentOffset += int64(recordSize)
	}

	return nil
}

// Put writes a key-value pair to the database.
// It serializes the record, appends it to disk, and updates the in-memory index.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return ErrDatabaseClosed
	}

	// 1. Construct normal record
	rec := record.NewRecord(key, value)

	// 2. Encode record with Little Endian and CRC32
	encoded, err := db.codec.Encode(rec)
	if err != nil {
		return err
	}

	// 3. Append to disk log
	offset, err := db.active.Write(encoded)
	if err != nil {
		return err
	}

	// 4. Update in-memory index with offset and record size
	db.index.Put(string(key), index.Item{
		RecordOffset: offset,
		RecordSize:   uint32(len(encoded)),
		Timestamp:    rec.Timestamp,
	})

	// 5. Optionally flush to disk if SyncOnWrite is configured
	if db.options.SyncOnWrite {
		if err := db.active.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves the value for the given key.
// It looks up the record location in KeyDir, executes a single ReadAt syscall for
// the entire record, and verifies CRC in-memory via DecodeBytes.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.mu.RLock()
	if db.isClosed {
		db.mu.RUnlock()
		return nil, ErrDatabaseClosed
	}
	db.mu.RUnlock()

	// 1. Lookup in-memory index
	item, exists := db.index.Get(string(key))
	if !exists {
		return nil, ErrKeyNotFound
	}

	// 2. Read the entire record in exactly 1 pread syscall
	data, err := db.active.ReadAt(item.RecordOffset, item.RecordSize)
	if err != nil {
		return nil, err
	}

	// 3. Decode and verify CRC in contiguous memory
	rec, err := db.codec.DecodeBytes(data)
	if err != nil {
		return nil, err
	}

	// 4. Check for tombstone safety
	if rec.Type == record.RecordTypeTombstone {
		return nil, ErrKeyNotFound
	}

	return rec.Value, nil
}

// Delete removes a key by appending a tombstone record to disk
// and evicting the key from the in-memory index.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return ErrDatabaseClosed
	}

	// 1. Check if key currently exists
	if _, exists := db.index.Get(string(key)); !exists {
		return ErrKeyNotFound
	}

	// 2. Create and encode tombstone record
	tombstone := record.NewTombstone(key)
	encoded, err := db.codec.Encode(tombstone)
	if err != nil {
		return err
	}

	// 3. Append tombstone record to disk
	if _, err := db.active.Write(encoded); err != nil {
		return err
	}

	// 4. Evict key from in-memory index
	db.index.Delete(string(key))

	// 5. Optionally flush to disk
	if db.options.SyncOnWrite {
		if err := db.active.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// Keys returns all active keys currently stored in the database.
func (db *DB) Keys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return nil
	}
	return db.index.Keys()
}

// Sync flushes all pending in-memory file buffers to stable disk storage.
func (db *DB) Sync() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.isClosed {
		return ErrDatabaseClosed
	}
	return db.active.Sync()
}

// Close flushes data to disk and closes the active file and index.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return nil
	}

	db.isClosed = true

	// Sync and close active data file
	if err := db.active.Sync(); err != nil {
		_ = db.active.Close()
		return err
	}
	if err := db.active.Close(); err != nil {
		return err
	}

	// Clear in-memory index
	return db.index.Close()
}
