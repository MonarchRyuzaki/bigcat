package resp

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestReader_ReadArray(t *testing.T) {
	raw := "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$8\r\nwhiskers\r\n"
	reader := NewReader(bytes.NewReader([]byte(raw)))

	cmd, err := reader.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand failed: %v", err)
	}

	if len(cmd) != 3 {
		t.Fatalf("expected 3 arguments, got %d", len(cmd))
	}
	if string(cmd[0]) != "SET" || string(cmd[1]) != "name" || string(cmd[2]) != "whiskers" {
		t.Fatalf("command mismatch: %v", cmd)
	}
}

func TestReader_ReadInline(t *testing.T) {
	raw := "SET age 42\r\n"
	reader := NewReader(bytes.NewReader([]byte(raw)))

	cmd, err := reader.ReadCommand()
	if err != nil {
		t.Fatalf("ReadCommand failed: %v", err)
	}

	if len(cmd) != 3 {
		t.Fatalf("expected 3 tokens, got %d", len(cmd))
	}
	if string(cmd[0]) != "SET" || string(cmd[1]) != "age" || string(cmd[2]) != "42" {
		t.Fatalf("tokens mismatch: %v", cmd)
	}
}

func TestReader_MalformedProtocol(t *testing.T) {
	// Missing CRLF
	raw := "*1\r\n$3\r\nFOOXX"
	reader := NewReader(bytes.NewReader([]byte(raw)))

	_, err := reader.ReadCommand()
	if !errors.Is(err, ErrInvalidProtocol) {
		t.Fatalf("expected ErrInvalidProtocol, got %v", err)
	}
}

func TestReader_EOF(t *testing.T) {
	reader := NewReader(bytes.NewReader([]byte{}))
	_, err := reader.ReadCommand()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestWriter_Formatting(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	// Simple String
	if err := w.WriteSimpleString("OK"); err != nil {
		t.Fatal(err)
	}
	// Error
	if err := w.WriteError("unknown"); err != nil {
		t.Fatal(err)
	}
	// Integer
	if err := w.WriteInteger(100); err != nil {
		t.Fatal(err)
	}
	// Bulk String
	if err := w.WriteBulkString([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	// Null Bulk String
	if err := w.WriteNull(); err != nil {
		t.Fatal(err)
	}
	// Empty Array
	if err := w.WriteEmptyArray(); err != nil {
		t.Fatal(err)
	}
	// Array of Bulk Strings
	items := [][]byte{[]byte("k1"), []byte("k2")}
	if err := w.WriteArray(items); err != nil {
		t.Fatal(err)
	}

	expected := "+OK\r\n-ERR unknown\r\n:100\r\n$5\r\nhello\r\n$-1\r\n*0\r\n*2\r\n$2\r\nk1\r\n$2\r\nk2\r\n"
	if buf.String() != expected {
		t.Fatalf("formatted output mismatch:\nExpected:\n%q\nGot:\n%q", expected, buf.String())
	}
}


func BenchmarkReader_ReadCommand_Array(b *testing.B) {
	raw := []byte("*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$7\r\nshivam1\r\n")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reader := NewReader(bytes.NewReader(raw))
		_, err := reader.ReadCommand()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriter_WriteBulkString(b *testing.B) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	payload := []byte("benchmark_payload_value_data")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = w.WriteBulkString(payload)
	}
}
