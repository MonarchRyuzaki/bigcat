package record

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"
)

func TestBinaryCodec_RoundTrip(t *testing.T) {
	codec := NewBinaryCodec()

	// 1MB payload for large value testing
	largeVal := make([]byte, 1024*1024)
	if _, err := rand.Read(largeVal); err != nil {
		t.Fatalf("failed to generate random bytes for large value test: %v", err)
	}

	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "standard key-value pair",
			record: &Record{
				Type:      RecordTypeNormal,
				Timestamp: uint64(time.Now().UnixNano()),
				Key:       []byte("username"),
				Value:     []byte("shivam"),
			},
		},
		{
			name: "tombstone record with empty value",
			record: &Record{
				Type:      RecordTypeTombstone,
				Timestamp: uint64(time.Now().UnixNano()),
				Key:       []byte("deleted_key"),
				Value:     nil,
			},
		},
		{
			name: "empty key and empty value",
			record: &Record{
				Type:      RecordTypeNormal,
				Timestamp: 123456789,
				Key:       []byte{},
				Value:     []byte{},
			},
		},
		{
			name: "binary payload with null bytes",
			record: &Record{
				Type:      RecordTypeNormal,
				Timestamp: 987654321,
				Key:       []byte("\x00\x01\x02key"),
				Value:     []byte("\xff\xfe\x00val"),
			},
		},
		{
			name: "unicode and emojis in key and value",
			record: &Record{
				Type:      RecordTypeNormal,
				Timestamp: uint64(time.Now().UnixNano()),
				Key:       []byte("🐾_key_キー_🐱"),
				Value:     []byte("🚀_value_データ_✨"),
			},
		},
		{
			name: "large payload (1MB value)",
			record: &Record{
				Type:      RecordTypeNormal,
				Timestamp: uint64(time.Now().UnixNano()),
				Key:       []byte("large_blob_key"),
				Value:     largeVal,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. Encode
			encoded, err := codec.Encode(tt.record)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			// Expected total length = HeaderSize + len(key) + len(value)
			expectedLen := HeaderSize + len(tt.record.Key) + len(tt.record.Value)
			if len(encoded) != expectedLen {
				t.Fatalf("Encoded buffer length mismatch: expected %d, got %d", expectedLen, len(encoded))
			}

			// 2. Decode
			reader := bytes.NewReader(encoded)
			decoded, err := codec.Decode(reader)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			// 3. Verify fields
			if decoded.Type != tt.record.Type {
				t.Errorf("Type mismatch: expected %v, got %v", tt.record.Type, decoded.Type)
			}
			if decoded.Timestamp != tt.record.Timestamp {
				t.Errorf("Timestamp mismatch: expected %v, got %v", tt.record.Timestamp, decoded.Timestamp)
			}
			if !bytes.Equal(decoded.Key, tt.record.Key) {
				t.Errorf("Key mismatch: expected %q, got %q", tt.record.Key, decoded.Key)
			}
			if !bytes.Equal(decoded.Value, tt.record.Value) {
				if len(decoded.Value) != 0 || len(tt.record.Value) != 0 {
					t.Errorf("Value mismatch: expected length %d, got %d", len(tt.record.Value), len(decoded.Value))
				}
			}
		})
	}
}

func TestBinaryCodec_CRCValidationMismatch(t *testing.T) {
	codec := NewBinaryCodec()

	baseRec := &Record{
		Type:      RecordTypeNormal,
		Timestamp: 1700000000,
		Key:       []byte("tamper_key"),
		Value:     []byte("tamper_val"),
	}

	encoded, err := codec.Encode(baseRec)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	corruptionScenarios := []struct {
		name      string
		tamperPos int
	}{
		{"corrupt record type byte", RecordTypeOffset},
		{"corrupt timestamp byte", TimestampOffset},
		{"corrupt key size byte", KeySizeOffset},
		{"corrupt value size byte", ValueSizeOffset},
		{"corrupt key payload byte", HeaderSize},
		{"corrupt value payload byte", HeaderSize + len(baseRec.Key)},
		{"corrupt CRC field itself", CRC32Offset},
	}

	for _, cs := range corruptionScenarios {
		t.Run(cs.name, func(t *testing.T) {
			corrupted := make([]byte, len(encoded))
			copy(corrupted, encoded)

			// Flip bits in the target byte
			corrupted[cs.tamperPos] ^= 0xFF

			_, err := codec.Decode(bytes.NewReader(corrupted))
			if err == nil {
				t.Fatalf("expected error on corrupted data at offset %d, but Decode succeeded", cs.tamperPos)
			}
			// Should fail with ErrCRCValidationFailed or ErrUnexpectedEOF if size was bloated beyond stream
			if !errors.Is(err, ErrCRCValidationFailed) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("expected ErrCRCValidationFailed or io.ErrUnexpectedEOF, got %v", err)
			}
		})
	}
}

func TestBinaryCodec_TruncatedAndEOF(t *testing.T) {
	codec := NewBinaryCodec()

	t.Run("clean EOF on empty reader", func(t *testing.T) {
		emptyReader := bytes.NewReader([]byte{})
		rec, err := codec.Decode(emptyReader)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("expected io.EOF on empty reader, got %v", err)
		}
		if rec != nil {
			t.Fatalf("expected nil record on EOF, got %v", rec)
		}
	})

	t.Run("truncated within header", func(t *testing.T) {
		rec := &Record{
			Type:      RecordTypeNormal,
			Timestamp: 1000,
			Key:       []byte("test_key"),
			Value:     []byte("test_val"),
		}
		encoded, err := codec.Encode(rec)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		// Read with header cut off halfway
		_, err = codec.Decode(bytes.NewReader(encoded[:HeaderSize-5]))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF for truncated header, got: %v", err)
		}
	})

	t.Run("truncated within payload", func(t *testing.T) {
		rec := &Record{
			Type:      RecordTypeNormal,
			Timestamp: 1000,
			Key:       []byte("test_key"),
			Value:     []byte("test_val"),
		}
		encoded, err := codec.Encode(rec)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		// Read with last byte missing from payload
		_, err = codec.Decode(bytes.NewReader(encoded[:len(encoded)-1]))
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF for truncated payload, got: %v", err)
		}
	})
}

func BenchmarkBinaryCodec_Encode(b *testing.B) {
	codec := NewBinaryCodec()
	rec := &Record{
		Type:      RecordTypeNormal,
		Timestamp: uint64(time.Now().UnixNano()),
		Key:       []byte("benchmark_key_12345"),
		Value:     []byte("benchmark_value_data_payload_67890"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(rec)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryCodec_Decode(b *testing.B) {
	codec := NewBinaryCodec()
	rec := &Record{
		Type:      RecordTypeNormal,
		Timestamp: uint64(time.Now().UnixNano()),
		Key:       []byte("benchmark_key_12345"),
		Value:     []byte("benchmark_value_data_payload_67890"),
	}

	encoded, err := codec.Encode(rec)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(encoded)
		_, err := codec.Decode(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBinaryCodec_DecodeBytes(b *testing.B) {
	codec := NewBinaryCodec()
	rec := &Record{
		Type:      RecordTypeNormal,
		Timestamp: uint64(time.Now().UnixNano()),
		Key:       []byte("benchmark_key_12345"),
		Value:     []byte("benchmark_value_data_payload_67890"),
	}

	encoded, err := codec.Encode(rec)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := codec.DecodeBytes(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestBinaryCodec_DecodeBytes(t *testing.T) {
	codec := NewBinaryCodec()

	rec := &Record{
		Type:      RecordTypeNormal,
		Timestamp: 12345678,
		Key:       []byte("k"),
		Value:     []byte("v"),
	}

	encoded, err := codec.Encode(rec)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	t.Run("successful DecodeBytes", func(t *testing.T) {
		decoded, err := codec.DecodeBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeBytes failed: %v", err)
		}
		if !bytes.Equal(decoded.Key, rec.Key) || !bytes.Equal(decoded.Value, rec.Value) {
			t.Fatalf("mismatch in decoded record")
		}
	})

	t.Run("buffer too short for header", func(t *testing.T) {
		_, err := codec.DecodeBytes(encoded[:10])
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("buffer truncated in payload", func(t *testing.T) {
		_, err := codec.DecodeBytes(encoded[:len(encoded)-1])
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
		}
	})

	t.Run("CRC corruption in DecodeBytes", func(t *testing.T) {
		corrupted := append([]byte(nil), encoded...)
		corrupted[len(corrupted)-1] ^= 0xFF
		_, err := codec.DecodeBytes(corrupted)
		if !errors.Is(err, ErrCRCValidationFailed) {
			t.Fatalf("expected ErrCRCValidationFailed, got %v", err)
		}
	})
}
