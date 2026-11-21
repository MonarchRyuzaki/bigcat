// record has the logic of BitCask binary serialization
package record

import "time"

// RecordType represents the state of a record on disk.
type RecordType uint8

const (
	// RecordTypeNormal represents an active key-value pair.
	RecordTypeNormal RecordType = 0x01

	// RecordTypeTombstone represents a deleted key (deletion marker).
	// A tombstone entry informs index rebuilders to remove this key from memory.
	RecordTypeTombstone RecordType = 0x02
)

const (
	// HeaderSize is the fixed size of the binary record header in bytes:
	// - CRC32:       4 bytes (uint32)
	// - RecordType:  1 byte  (uint8)
	// - Timestamp:   8 bytes (uint64, Unix nanoseconds or seconds)
	// - KeySize:     4 bytes (uint32)
	// - ValueSize:   4 bytes (uint32)
	// Total: 4 + 1 + 8 + 4 + 4 = 21 bytes
	crc32FieldSize      = 4
	recordTypeFieldSize = 1
	timestampFieldSize  = 8
	keySize             = 4
	ValueSize           = 4
	CRC32Offset         = 0
	RecordTypeOffset    = CRC32Offset + crc32FieldSize
	TimestampOffset     = RecordTypeOffset + recordTypeFieldSize
	KeySizeOffset       = TimestampOffset + timestampFieldSize
	ValueSizeOffset     = KeySizeOffset + keySize
	HeaderSize          = 21
)

// Record represents a single key-value entry stored in BigCat.
type Record struct {
	Type      RecordType
	Timestamp uint64
	Key       []byte
	Value     []byte
}

// NewRecord creates a standard (non-deleted) record with the provided key and value.
func NewRecord(key, value []byte) *Record {
	// DONE: initialize Record with current timestamp and RecordTypeNormal
	return &Record{
		Type:      RecordTypeNormal,
		Timestamp: uint64(time.Now().UnixNano()),
		Key:       key,
		Value:     value,
	}
}

// NewTombstone creates a deletion marker record for the given key.
func NewTombstone(key []byte) *Record {
	// DONE: initialize Record with current timestamp, RecordTypeTombstone, and empty value
	return &Record{
		Type:      RecordTypeTombstone,
		Timestamp: uint64(time.Now().UnixNano()),
		Key:       key,
	}
}
