package bigcat

import "errors"

var (
	// ErrKeyNotFound is returned when a requested key does not exist or has been deleted.
	ErrKeyNotFound = errors.New("key not found")

	// ErrCorruptedRecord is returned when a record fails CRC verification or has invalid bytes.
	ErrCorruptedRecord = errors.New("data corruption detected: CRC mismatch")

	// ErrDatabaseClosed is returned when an operation is attempted on a closed DB instance.
	ErrDatabaseClosed = errors.New("database is closed")

	// ErrEmptyKey is returned when an empty slice is passed as a key.
	ErrEmptyKey = errors.New("key cannot be empty")
)
