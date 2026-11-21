package record

import (
	"bytes"
	"testing"
	"time"
)

func TestNewRecord(t *testing.T) {
	before := uint64(time.Now().UnixNano())
	key := []byte("cat_name")
	val := []byte("whiskers")

	rec := NewRecord(key, val)
	after := uint64(time.Now().UnixNano())

	if rec.Type != RecordTypeNormal {
		t.Fatalf("expected RecordTypeNormal (1), got %v", rec.Type)
	}
	if rec.Timestamp < before || rec.Timestamp > after {
		t.Fatalf("timestamp %d out of expected range [%d, %d]", rec.Timestamp, before, after)
	}
	if !bytes.Equal(rec.Key, key) {
		t.Fatalf("expected key %s, got %s", key, rec.Key)
	}
	if !bytes.Equal(rec.Value, val) {
		t.Fatalf("expected value %s, got %s", val, rec.Value)
	}
}

func TestNewTombstone(t *testing.T) {
	before := uint64(time.Now().UnixNano())
	key := []byte("deleted_cat")

	rec := NewTombstone(key)
	after := uint64(time.Now().UnixNano())

	if rec.Type != RecordTypeTombstone {
		t.Fatalf("expected RecordTypeTombstone (2), got %v", rec.Type)
	}
	if rec.Timestamp < before || rec.Timestamp > after {
		t.Fatalf("timestamp %d out of expected range [%d, %d]", rec.Timestamp, before, after)
	}
	if !bytes.Equal(rec.Key, key) {
		t.Fatalf("expected key %s, got %s", key, rec.Key)
	}
	if len(rec.Value) != 0 {
		t.Fatalf("expected empty value for tombstone, got %v", rec.Value)
	}
}
