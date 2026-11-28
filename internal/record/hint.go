package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	hintCRC32Size        = 4
	hintTimestampSize    = 8
	hintKeySizeFieldSize = 4
	hintValSizeFieldSize = 4
	hintOffsetFieldSize  = 8

	HintCRC32Offset     = 0
	HintTimestampOffset = HintCRC32Offset + hintCRC32Size
	HintKeySizeOffset   = HintTimestampOffset + hintTimestampSize
	HintValueSizeOffset = HintKeySizeOffset + hintKeySizeFieldSize
	HintRecordOffOffset = HintValueSizeOffset + hintValSizeFieldSize

	// HintHeaderSize is the fixed 28-byte binary header for .hint entries:
	// [CRC32 (4B)] [Timestamp (8B)] [KeySize (4B)] [ValueSize (4B)] [RecordOffset (8B)]
	HintHeaderSize = HintRecordOffOffset + hintOffsetFieldSize
)

// ErrInvalidHintRecord is returned when a hint entry has invalid metadata (e.g., negative offset).
var ErrInvalidHintRecord = errors.New("invalid hint record")

// HintRecord stores the compact index metadata written into companion .hint files
// during compaction so cold startup can reconstruct KeyDir without scanning value payloads.
type HintRecord struct {
	Timestamp    uint64
	ValueSize    uint32
	RecordOffset int64
	Key          []byte
}

// HintCodec defines serialization for .hint file index entries with integrity verification.
type HintCodec interface {
	EncodeHint(h *HintRecord) ([]byte, error)
	DecodeHint(r io.Reader) (*HintRecord, error)
}

// EncodeHint serializes a HintRecord into Little-Endian binary format:
// [CRC32 (4B)] [Timestamp (8B)] [KeySize (4B)] [ValueSize (4B)] [RecordOffset (8B)] [Key]
// CRC32 (IEEE) is computed over [Timestamp + KeySize + ValueSize + RecordOffset + Key].
func (c *BinaryCodec) EncodeHint(h *HintRecord) ([]byte, error) {
	if h == nil || h.RecordOffset < 0 {
		return nil, ErrInvalidHintRecord
	}

	keyLen := len(h.Key)
	buf := make([]byte, HintHeaderSize+keyLen)

	binary.LittleEndian.PutUint64(buf[HintTimestampOffset:], h.Timestamp)
	binary.LittleEndian.PutUint32(buf[HintKeySizeOffset:], uint32(keyLen))
	binary.LittleEndian.PutUint32(buf[HintValueSizeOffset:], h.ValueSize)
	binary.LittleEndian.PutUint64(buf[HintRecordOffOffset:], uint64(h.RecordOffset))
	copy(buf[HintHeaderSize:], h.Key)

	checksum := crc32.ChecksumIEEE(buf[HintTimestampOffset:])
	binary.LittleEndian.PutUint32(buf[HintCRC32Offset:], checksum)

	return buf, nil
}

// DecodeHint reads a single HintRecord from r, verifies its CRC32 checksum, and returns it.
// Returns io.EOF at a clean stream boundary or io.ErrUnexpectedEOF on truncated entries.
func (c *BinaryCodec) DecodeHint(r io.Reader) (*HintRecord, error) {
	var headerBuf [HintHeaderSize]byte
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return nil, err
	}

	expectedCRC := binary.LittleEndian.Uint32(headerBuf[HintCRC32Offset:])
	timestamp := binary.LittleEndian.Uint64(headerBuf[HintTimestampOffset:])
	keySize := binary.LittleEndian.Uint32(headerBuf[HintKeySizeOffset:])
	valueSize := binary.LittleEndian.Uint32(headerBuf[HintValueSizeOffset:])
	rawOffset := binary.LittleEndian.Uint64(headerBuf[HintRecordOffOffset:])

	keyBuf := make([]byte, keySize)
	if _, err := io.ReadFull(r, keyBuf); err != nil {
		return nil, err
	}

	actualCRC := crc32.Update(0, crc32.IEEETable, headerBuf[HintTimestampOffset:])
	actualCRC = crc32.Update(actualCRC, crc32.IEEETable, keyBuf)
	if actualCRC != expectedCRC {
		return nil, ErrCRCValidationFailed
	}

	recordOffset := int64(rawOffset)
	if recordOffset < 0 {
		return nil, ErrInvalidHintRecord
	}

	return &HintRecord{
		Timestamp:    timestamp,
		ValueSize:    valueSize,
		RecordOffset: recordOffset,
		Key:          keyBuf,
	}, nil
}
