package record

import (
	"io"
)

// Codec defines how records are encoded to and decoded from byte streams.
// Decoupling this allows experimenting with different serialization formats
// (e.g., LittleEndian binary, Protobuf, custom compression) without modifying DB logic.
type Codec interface {
	// Encode serializes a Record into on-disk binary format including CRC checksum.
	Encode(r *Record) ([]byte, error)

	// Decode reads a single record from a stream, verifies its CRC checksum,
	// and returns the deserialized Record.
	Decode(r io.Reader) (*Record, error)
}

// BinaryCodec implements the standard Bitcask-style binary serialization:
// [CRC: 4B] [Type: 1B] [Timestamp: 8B] [KeySize: 4B] [ValueSize: 4B] [Key] [Value]
// All integers are packed using Little Endian.
type BinaryCodec struct{}

// NewBinaryCodec creates an instance of BinaryCodec.
func NewBinaryCodec() *BinaryCodec {
	return &BinaryCodec{}
}

// Encode serializes a Record into a byte slice.
// It calculates CRC32 over [Type + Timestamp + KeySize + ValueSize + Key + Value]
// and prepends the CRC as the first 4 bytes.
func (c *BinaryCodec) Encode(r *Record) ([]byte, error) {
	// TODO:
	// 1. Calculate total buffer size: HeaderSize + len(key) + len(value)
	// 2. Pack Type, Timestamp, KeySize, ValueSize, Key, Value using binary.LittleEndian
	// 3. Compute crc32.ChecksumIEEE over payload bytes (bytes after CRC)
	// 4. Pack CRC32 into the first 4 bytes
	panic("TODO: implement Encode")
}

// Decode reads the header from the reader, parses lengths, reads the remaining payload,
// and verifies the CRC32 checksum before returning the Record.
func (c *BinaryCodec) Decode(r io.Reader) (*Record, error) {
	// TODO:
	// 1. Read HeaderSize (21 bytes) using io.ReadFull
	// 2. Unpack CRC, Type, Timestamp, KeySize, ValueSize
	// 3. Read Key (KeySize bytes) and Value (ValueSize bytes)
	// 4. Re-calculate CRC32 over the read payload and compare against the unpacked CRC
	// 5. If mismatch, return an error (e.g., ErrCorruptedRecord)
	// 6. Return parsed *Record
	panic("TODO: implement Decode")
}
