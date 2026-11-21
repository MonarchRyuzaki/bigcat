package index

// Item stores metadata in memory for an active key on disk.
// In Bitcask, keeping this in RAM ensures every read requires only one disk seek.
type Item struct {
	// RecordOffset is the absolute byte offset in the data file where the record begins.
	RecordOffset int64

	// RecordSize is the total length in bytes of the record (HeaderSize + KeySize + ValueSize).
	// Storing this allows reading the exact record without reading the header first.
	RecordSize uint32

	// Timestamp is the Unix timestamp when this key was written.
	Timestamp uint64
}

// Indexer is the pluggable interface for in-memory indexing.
// Default is a concurrent hash map, but could be replaced by a SkipList or B-Tree
// if range queries / prefix scans are needed later.
type Indexer interface {
	// Put inserts or updates the index item for the given key.
	Put(key string, item Item)

	// Get retrieves the index item for a given key. Returns false if not found.
	Get(key string) (Item, bool)

	// Delete removes a key from the index.
	Delete(key string)

	// Keys returns all active keys currently stored in the index.
	Keys() []string

	// Size returns the total number of keys in the index.
	Size() int

	// Close releases any resources associated with the index.
	Close() error
}
