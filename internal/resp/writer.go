package resp

import (
	"fmt"
	"io"
	"strconv"
)

var (
	crlf = []byte("\r\n")
	nullBulkString = []byte("$-1\r\n")
	emptyArray = []byte("*0\r\n")
)

// Writer provides helpers to format and write RESP v2 protocol messages.
type Writer struct {
	w io.Writer
}

// NewWriter creates a new RESP Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// WriteSimpleString writes a RESP simple string: +<string>\r\n (e.g. +OK\r\n).
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

// WriteError writes a RESP error: -ERR <message>\r\n.
func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-ERR %s\r\n", msg)
	return err
}

// WriteInteger writes a RESP 64-bit integer: :<int>\r\n.
func (w *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

// WriteBulkString writes a RESP bulk string: $<len>\r\n<data>\r\n.
func (w *Writer) WriteBulkString(b []byte) error {
	header := "$" + strconv.Itoa(len(b)) + "\r\n"
	if _, err := io.WriteString(w.w, header); err != nil {
		return err
	}
	if _, err := w.w.Write(b); err != nil {
		return err
	}
	_, err := w.w.Write(crlf)
	return err
}

// WriteNull writes a RESP null bulk string: $-1\r\n.
func (w *Writer) WriteNull() error {
	_, err := w.w.Write(nullBulkString)
	return err
}

// WriteEmptyArray writes a RESP empty array: *0\r\n.
func (w *Writer) WriteEmptyArray() error {
	_, err := w.w.Write(emptyArray)
	return err
}

// WriteArray writes an array of bulk strings: *<count>\r\n followed by each bulk string.
func (w *Writer) WriteArray(items [][]byte) error {
	header := "*" + strconv.Itoa(len(items)) + "\r\n"
	if _, err := io.WriteString(w.w, header); err != nil {
		return err
	}
	for _, item := range items {
		if item == nil {
			if err := w.WriteNull(); err != nil {
				return err
			}
		} else {
			if err := w.WriteBulkString(item); err != nil {
				return err
			}
		}
	}
	return nil
}
