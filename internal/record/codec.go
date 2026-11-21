package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// ErrCRCValidationFailed is returned when the decoded data does not match the stored checksum.
var ErrCRCValidationFailed = errors.New("data corruption detected: CRC mismatch")

// Codec defines how records are encoded to and decoded from byte streams.
// Decoupling this allows experimenting with different serialization formats
// (e.g., LittleEndian binary, Protobuf, custom compression) without modifying DB logic.
type Codec interface {
	// Encode serializes a Record into on-disk binary format including CRC checksum.
	Encode(r *Record) ([]byte, error)

	// Decode reads a single record from a stream, verifies its CRC checksum,
	// and returns the deserialized Record. Used for sequential scans.
	Decode(r io.Reader) (*Record, error)

	// DecodeBytes parses and verifies a Record directly from a contiguous byte slice.
	// Used for single-seek Get operations where the record size is already known.
	DecodeBytes(data []byte) (*Record, error)
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
	keyLen := len(r.Key)
	valLen := len(r.Value)
	bufferSize := HeaderSize + keyLen + valLen

	// Allocate a single contiguous byte buffer for header + payload
	buffer := make([]byte, bufferSize)

	// 1. Pack 1-byte RecordType (no endianness needed for single byte)
	buffer[RecordTypeOffset] = byte(r.Type)

	// 2. Pack metadata integers using Little Endian
	binary.LittleEndian.PutUint64(buffer[TimestampOffset:], r.Timestamp)
	binary.LittleEndian.PutUint32(buffer[KeySizeOffset:], uint32(keyLen))
	binary.LittleEndian.PutUint32(buffer[ValueSizeOffset:], uint32(valLen))

	// 3. Copy variable-length Key and Value payload into the buffer
	keyStart := HeaderSize
	copy(buffer[keyStart:], r.Key)

	valStart := keyStart + keyLen
	copy(buffer[valStart:], r.Value)

	// 4. Compute CRC32 (IEEE) checksum over all bytes following the CRC field:
	// [RecordType + Timestamp + KeySize + ValueSize + Key + Value]
	checksum := crc32.ChecksumIEEE(buffer[RecordTypeOffset:])

	// 5. Prepend the computed CRC32 checksum into the first 4 bytes
	binary.LittleEndian.PutUint32(buffer[CRC32Offset:], checksum)

	return buffer, nil
}

// Decode reads a single record from the reader:
// 1. Reads the fixed 21-byte header to learn field lengths and expected CRC.
// 2. Reads the variable-length Key and Value payload.
// 3. Re-computes the CRC32 checksum over header fields (sans CRC) + payload.
// 4. Returns the validated Record, or ErrCRCValidationFailed if bytes are corrupted.
func (c *BinaryCodec) Decode(r io.Reader) (*Record, error) {
	// 1. Read fixed 21-byte header
	headerBuf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err // Returns io.EOF or io.ErrUnexpectedEOF
	}

	// 2. Unpack header fields using Little Endian
	expectedCRC := binary.LittleEndian.Uint32(headerBuf[CRC32Offset:])
	recordType := RecordType(headerBuf[RecordTypeOffset])
	timestamp := binary.LittleEndian.Uint64(headerBuf[TimestampOffset:])
	keySize := binary.LittleEndian.Uint32(headerBuf[KeySizeOffset:])
	valueSize := binary.LittleEndian.Uint32(headerBuf[ValueSizeOffset:])

	// 3. Allocate and read the variable-length payload (Key + Value)
	payloadSize := keySize + valueSize
	payloadBuf := make([]byte, payloadSize)
	if _, err := io.ReadFull(r, payloadBuf); err != nil {
		return nil, err
	}

	// 4. Verify data integrity by re-computing CRC32
	// We stream the header bytes (excluding CRC) and the payload bytes into crc32 hasher
	h := crc32.NewIEEE()
	h.Write(headerBuf[RecordTypeOffset:]) // header bytes after CRC (17 bytes)
	h.Write(payloadBuf)                   // key and value bytes
	actualCRC := h.Sum32()

	if actualCRC != expectedCRC {
		return nil, ErrCRCValidationFailed
	}

	// 5. Slice key and value from payload buffer
	key := payloadBuf[:keySize]
	value := payloadBuf[keySize:]

	return &Record{
		Type:      recordType,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}, nil
}

// DecodeBytes decodes and verifies a record directly from a pre-read byte slice.
// This is optimized for Get(key) operations where KeyDir already knows the exact RecordSize,
// allowing BigCat to read the entire record in a single ReadAt syscall and decode it in-memory
// with zero extra heap allocations for key/value slices.
func (c *BinaryCodec) DecodeBytes(data []byte) (*Record, error) {
	// 1. Ensure buffer has at least the minimum header size
	if len(data) < HeaderSize {
		return nil, io.ErrUnexpectedEOF
	}

	// 2. Unpack header fields using Little Endian
	expectedCRC := binary.LittleEndian.Uint32(data[CRC32Offset:])
	recordType := RecordType(data[RecordTypeOffset])
	timestamp := binary.LittleEndian.Uint64(data[TimestampOffset:])
	keySize := binary.LittleEndian.Uint32(data[KeySizeOffset:])
	valueSize := binary.LittleEndian.Uint32(data[ValueSizeOffset:])

	// 3. Ensure the byte slice contains the entire declared payload
	totalExpectedSize := HeaderSize + int(keySize) + int(valueSize)
	if len(data) < totalExpectedSize {
		return nil, io.ErrUnexpectedEOF
	}

	// 4. Verify CRC32 checksum across all bytes after CRC field in one shot.
	// Since data is already contiguous, crc32.ChecksumIEEE is called directly without hasher allocations!
	actualCRC := crc32.ChecksumIEEE(data[RecordTypeOffset:totalExpectedSize])
	if actualCRC != expectedCRC {
		return nil, ErrCRCValidationFailed
	}

	// 5. Zero-copy sub-slicing of key and value directly from data buffer
	keyStart := HeaderSize
	valStart := keyStart + int(keySize)

	return &Record{
		Type:      recordType,
		Timestamp: timestamp,
		Key:       data[keyStart:valStart],
		Value:     data[valStart:totalExpectedSize],
	}, nil
}
