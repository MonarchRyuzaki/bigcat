package bigcat

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"bigcat/internal/index"
	"bigcat/internal/record"
	"bigcat/internal/storage"
)

// DB represents a BigCat key-value storage engine instance.
// It coordinates append-only multi-file storage on disk with an in-memory KeyDir index.
type DB struct {
	mu        sync.RWMutex
	options   Options
	storage   *storage.FileManager
	index     index.Indexer
	codec     record.Codec
	committer *GroupCommitter
	isClosed  bool
}

// Open initializes and opens a BigCat storage engine instance in the specified directory.
// On startup, it scans all data files sequentially in ascending order to rebuild the in-memory KeyDir.
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

	// 3. Initialize multi-file storage manager
	fileManager, err := storage.OpenFileManager(dirPath, options.MaxFileSize)
	if err != nil {
		return nil, err
	}

	// 4. Initialize Indexer and Codec
	idx := index.NewMapIndex()
	cdc := record.NewBinaryCodec()

	db := &DB{
		options: options,
		storage: fileManager,
		index:   idx,
		codec:   cdc,
	}

	// 5. Build in-memory KeyDir by sequentially scanning all data files from offset 0
	if err := db.loadIndexFromDisk(); err != nil {
		_ = fileManager.Close()
		_ = idx.Close()
		return nil, err
	}

	// 6. Start background group committer worker
	db.committer = newGroupCommitter(db, DefaultMaxBatchSize, DefaultMaxBatchBytes)

	return db, nil
}

// loadIndexFromDisk scans all managed data files in ascending order to recover the in-memory KeyDir index.
// It wraps each file in a 256KB buffered reader to minimize disk syscalls during startup.
func (db *DB) loadIndexFromDisk() error {
	// FileIDs() returns all file IDs sorted in ascending order (e.g. 00000, 00001, ...)
	fileIDs := db.storage.FileIDs()

	for _, fileID := range fileIDs {
		filePath := filepath.Join(db.options.DirPath, storage.DataFileName(fileID))
		file, err := os.Open(filePath)
		if err != nil {
			return err
		}

		if err := db.scanDataFile(fileID, file); err != nil {
			_ = file.Close()
			return err
		}

		_ = file.Close()
	}

	return nil
}

// scanDataFile sequentially reads records from a single data file and updates the in-memory index.
func (db *DB) scanDataFile(fileID uint32, file *os.File) error {
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
			// Populate or overwrite index with the latest active offset and fileID
			db.index.Put(string(rec.Key), index.Item{
				FileID:       fileID,
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
// It serializes the record outside the lock and submits it to the group committer
// for coalesced, low-latency disk commitment.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	db.mu.RLock()
	if db.isClosed {
		db.mu.RUnlock()
		return ErrDatabaseClosed
	}
	db.mu.RUnlock()

	// 1. Construct and encode record outside the mutex.
	// Serialization and CRC32 calculation run in parallel across CPU cores with zero lock contention.
	rec := record.NewRecord(key, value)
	encoded, err := db.codec.Encode(rec)
	if err != nil {
		return err
	}

	// 2. Submit write task to the group committer
	task := &writeTask{
		key:       key,
		encoded:   encoded,
		isDelete:  false,
		timestamp: rec.Timestamp,
	}

	if err := db.committer.Submit(task); err != nil {
		if errors.Is(err, ErrCommitterClosed) {
			return ErrDatabaseClosed
		}
		return err
	}

	return nil
}

// Get retrieves the value for the given key.
// It looks up the record location in KeyDir, executes a single ReadAt syscall for
// the entire record from the corresponding data file, and verifies CRC in-memory via DecodeBytes.
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

	// 2. Read the entire record in exactly 1 pread syscall from the target file (active or older)
	data, err := db.storage.ReadAt(item.FileID, item.RecordOffset, item.RecordSize)
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

	db.mu.RLock()
	if db.isClosed {
		db.mu.RUnlock()
		return ErrDatabaseClosed
	}
	db.mu.RUnlock()

	// Fast-path: Check if key currently exists in index before encoding or writing
	if _, exists := db.index.Get(string(key)); !exists {
		return ErrKeyNotFound
	}

	// 1. Construct and encode tombstone record outside the lock
	tombstone := record.NewTombstone(key)
	encoded, err := db.codec.Encode(tombstone)
	if err != nil {
		return err
	}

	// 2. Submit tombstone task to the group committer
	task := &writeTask{
		key:       key,
		encoded:   encoded,
		isDelete:  true,
		timestamp: tombstone.Timestamp,
	}

	if err := db.committer.Submit(task); err != nil {
		if errors.Is(err, ErrCommitterClosed) {
			return ErrDatabaseClosed
		}
		return err
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
	return db.storage.Sync()
}

// Close flushes data to disk and closes all open data files and index.
func (db *DB) Close() error {
	// 1. Stop committer and drain any pending writes
	if db.committer != nil {
		if err := db.committer.Close(); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return nil
	}

	db.isClosed = true

	// 2. Sync and close all data files via FileManager
	if err := db.storage.Close(); err != nil {
		return err
	}

	// 3. Clear in-memory index
	return db.index.Close()
}
