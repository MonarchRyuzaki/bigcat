package bigcat

import (
	"sync"

	"bigcat/internal/index"
	"bigcat/internal/record"
	"bigcat/internal/storage"
)

// DB represents a BigCat key-value storage engine instance.
type DB struct {
	mu      sync.RWMutex
	options Options
	active  *storage.DataFile
	index   index.Indexer
	codec   record.Codec
	isClosed bool
}

// Open initializes and opens a BigCat storage engine instance in the specified directory.
// On startup, it scans the active data file sequentially to rebuild the in-memory KeyDir.
func Open(dirPath string, opts ...Option) (*DB, error) {
	// TODO:
	// 1. Initialize Options (use DefaultOptions and apply opts)
	// 2. Ensure dirPath exists (os.MkdirAll)
	// 3. Open the active data file in dirPath (e.g. data.db) via storage.OpenDataFile
	// 4. Initialize Indexer (index.NewMapIndex) and Codec (record.NewBinaryCodec)
	// 5. Build in-memory KeyDir by sequentially scanning the data file from offset 0
	// 6. Return *DB
	panic("TODO: implement Open")
}

// Put writes a key-value pair to the database.
// It creates a new normal record, appends it to the active file, and updates the index.
func (db *DB) Put(key, value []byte) error {
	// TODO:
	// 1. Validate key (non-empty)
	// 2. Create record.NewRecord(key, value)
	// 3. Encode record via db.codec.Encode
	// 4. Append encoded bytes to active file via db.active.Write
	// 5. Update db.index with new index.Item
	// 6. If db.options.SyncOnWrite is true, call db.active.Sync()
	panic("TODO: implement Put")
}

// Get retrieves the value associated with the specified key.
// It looks up the record offset in the index, reads the record from disk,
// verifies the CRC checksum, and returns the value.
func (db *DB) Get(key []byte) ([]byte, error) {
	// TODO:
	// 1. Validate key
	// 2. Lookup key in db.index.Get(string(key)) -> return ErrKeyNotFound if missing
	// 3. Read record bytes from disk via db.active.ReadAt(item.RecordOffset, item.RecordSize)
	// 4. Decode record via db.codec.Decode
	// 5. Check if record is tombstone -> return ErrKeyNotFound
	// 6. Return record.Value
	panic("TODO: implement Get")
}

// Delete marks a key as deleted by appending a tombstone record to disk
// and removing the key from the in-memory index.
func (db *DB) Delete(key []byte) error {
	// TODO:
	// 1. Validate key
	// 2. Check if key exists in db.index; if not, return ErrKeyNotFound
	// 3. Create tombstone record: record.NewTombstone(key)
	// 4. Encode and append tombstone record to active file
	// 5. Evict key from db.index
	// 6. If db.options.SyncOnWrite is true, call db.active.Sync()
	panic("TODO: implement Delete")
}

// Close flushes data to disk and closes active file and index.
func (db *DB) Close() error {
	// TODO:
	// 1. Acquire write lock
	// 2. Mark db.isClosed = true
	// 3. Sync and close db.active
	// 4. Close db.index
	panic("TODO: implement Close")
}
