package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
	"time"
)

func TestBinaryCodec_HintRoundTrip(t *testing.T) {
	codec := NewBinaryCodec()

	tests := []struct {
		name string
		hint *HintRecord
	}{
		{
			name: "standard hint record",
			hint: &HintRecord{
				Timestamp:    uint64(time.Now().UnixNano()),
				ValueSize:    256,
				RecordOffset: 4096,
				Key:          []byte("user:1001"),
			},
		},
		{
			name: "empty key and zero offset",
			hint: &HintRecord{
				Timestamp:    123456789,
				ValueSize:    0,
				RecordOffset: 0,
				Key:          []byte{},
			},
		},
		{
			name: "binary key with null bytes",
			hint: &HintRecord{
				Timestamp:    987654321,
				ValueSize:    1024 * 1024,
				RecordOffset: 1 << 34, // > 4GB offset
				Key:          []byte("\x00\xff\x01binary_key\x00"),
			},
		},
		{
			name: "unicode key",
			hint: &HintRecord{
				Timestamp:    uint64(time.Now().UnixNano()),
				ValueSize:    64,
				RecordOffset: 512,
				Key:          []byte("🐾_hint_キー_✨"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := codec.EncodeHint(tt.hint)
			if err != nil {
				t.Fatalf("EncodeHint failed: %v", err)
			}

			expectedLen := HintHeaderSize + len(tt.hint.Key)
			if len(encoded) != expectedLen {
				t.Fatalf("encoded length = %d, want %d", len(encoded), expectedLen)
			}

			decoded, err := codec.DecodeHint(bytes.NewReader(encoded))
			if err != nil {
				t.Fatalf("DecodeHint failed: %v", err)
			}

			if decoded.Timestamp != tt.hint.Timestamp {
				t.Errorf("Timestamp = %d, want %d", decoded.Timestamp, tt.hint.Timestamp)
			}
			if decoded.ValueSize != tt.hint.ValueSize {
				t.Errorf("ValueSize = %d, want %d", decoded.ValueSize, tt.hint.ValueSize)
			}
			if decoded.RecordOffset != tt.hint.RecordOffset {
				t.Errorf("RecordOffset = %d, want %d", decoded.RecordOffset, tt.hint.RecordOffset)
			}
			if !bytes.Equal(decoded.Key, tt.hint.Key) {
				t.Errorf("Key = %q, want %q", decoded.Key, tt.hint.Key)
			}
		})
	}
}

func TestBinaryCodec_HintCRCValidationMismatch(t *testing.T) {
	codec := NewBinaryCodec()
	base := &HintRecord{
		Timestamp:    1700000000,
		ValueSize:    128,
		RecordOffset: 2048,
		Key:          []byte("integrity_key"),
	}
	encoded, err := codec.EncodeHint(base)
	if err != nil {
		t.Fatalf("EncodeHint failed: %v", err)
	}

	scenarios := []struct {
		name      string
		tamperPos int
	}{
		{"corrupt CRC byte", HintCRC32Offset},
		{"corrupt Timestamp byte", HintTimestampOffset},
		{"corrupt KeySize byte", HintKeySizeOffset},
		{"corrupt ValueSize byte", HintValueSizeOffset},
		{"corrupt RecordOffset byte", HintRecordOffOffset},
		{"corrupt Key payload byte", HintHeaderSize},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			corrupted := append([]byte(nil), encoded...)
			corrupted[sc.tamperPos] ^= 0xFF

			_, err := codec.DecodeHint(bytes.NewReader(corrupted))
			if !errors.Is(err, ErrCRCValidationFailed) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected ErrCRCValidationFailed or io.ErrUnexpectedEOF, got %v", err)
			}
		})
	}
}

func TestBinaryCodec_HintErrorsAndTruncation(t *testing.T) {
	codec := NewBinaryCodec()

	if _, err := codec.EncodeHint(nil); !errors.Is(err, ErrInvalidHintRecord) {
		t.Fatalf("expected ErrInvalidHintRecord for nil hint, got %v", err)
	}
	if _, err := codec.EncodeHint(&HintRecord{RecordOffset: -1, Key: []byte("k")}); !errors.Is(err, ErrInvalidHintRecord) {
		t.Fatalf("expected ErrInvalidHintRecord for negative offset, got %v", err)
	}

	// Clean EOF on empty stream
	if _, err := codec.DecodeHint(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on empty stream, got %v", err)
	}

	encoded, err := codec.EncodeHint(&HintRecord{
		Timestamp:    99,
		ValueSize:    10,
		RecordOffset: 42,
		Key:          []byte("truncate_me"),
	})
	if err != nil {
		t.Fatalf("EncodeHint failed: %v", err)
	}

	// Truncated header
	if _, err := codec.DecodeHint(bytes.NewReader(encoded[:10])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF on truncated header, got %v", err)
	}
	// Truncated key
	if _, err := codec.DecodeHint(bytes.NewReader(encoded[:HintHeaderSize+2])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected io.ErrUnexpectedEOF on truncated key, got %v", err)
	}

	// Valid CRC but negative RecordOffset must be rejected
	negBuf := append([]byte(nil), encoded...)
	negVal := int64(-5)
	binary.LittleEndian.PutUint64(negBuf[HintRecordOffOffset:], uint64(negVal))
	binary.LittleEndian.PutUint32(negBuf[HintCRC32Offset:], crc32.ChecksumIEEE(negBuf[HintTimestampOffset:]))
	if _, err := codec.DecodeHint(bytes.NewReader(negBuf)); !errors.Is(err, ErrInvalidHintRecord) {
		t.Fatalf("expected ErrInvalidHintRecord on negative offset, got %v", err)
	}
}

func BenchmarkBinaryCodec_EncodeHint(b *testing.B) {
	codec := NewBinaryCodec()
	h := &HintRecord{
		Timestamp:    1700000000,
		ValueSize:    4096,
		RecordOffset: 102400,
		Key:          []byte("benchmark_hint_key_001"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := codec.EncodeHint(h); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryCodec_DecodeHint(b *testing.B) {
	codec := NewBinaryCodec()
	encoded, err := codec.EncodeHint(&HintRecord{
		Timestamp:    1700000000,
		ValueSize:    4096,
		RecordOffset: 102400,
		Key:          []byte("benchmark_hint_key_001"),
	})
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(encoded)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader.Reset(encoded)
		if _, err := codec.DecodeHint(reader); err != nil {
			b.Fatal(err)
		}
	}
}
