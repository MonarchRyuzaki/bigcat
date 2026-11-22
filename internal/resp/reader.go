package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	// ErrInvalidProtocol is returned when a malformed RESP payload is encountered.
	ErrInvalidProtocol = errors.New("invalid RESP protocol")
)

// Reader parses incoming commands formatted as RESP arrays or inline strings.
type Reader struct {
	rd *bufio.Reader
}

// NewReader wraps an io.Reader in a RESP Reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(r),
	}
}

// ReadCommand parses the next incoming command and returns its tokens (e.g. ["SET", "key", "val"]).
// Supports both RESP Array-of-Bulk-Strings (from redis-cli) and inline commands (from telnet/nc).
func (r *Reader) ReadCommand() ([][]byte, error) {
	firstByte, err := r.rd.Peek(1)
	if err != nil {
		return nil, err
	}

	// 1. RESP Array: *<count>\r\n$<len>\r\n<arg>\r\n...
	if firstByte[0] == '*' {
		return r.readArray()
	}

	// 2. Inline Command: e.g. "PING\r\n" or "SET mykey myval\r\n"
	return r.readInline()
}

// readArray parses an array of bulk strings: *<count>\r\n...
func (r *Reader) readArray() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[0] != '*' {
		return nil, ErrInvalidProtocol
	}

	count, err := strconv.Atoi(string(line[1:]))
	if err != nil || count < 0 {
		return nil, ErrInvalidProtocol
	}

	args := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		bulkLine, err := r.readLine()
		if err != nil {
			return nil, err
		}

		if len(bulkLine) < 2 || bulkLine[0] != '$' {
			return nil, ErrInvalidProtocol
		}

		bulkLen, err := strconv.Atoi(string(bulkLine[1:]))
		if err != nil || bulkLen < 0 {
			return nil, ErrInvalidProtocol
		}

		// Read bulk string bytes + trailing \r\n
		payload := make([]byte, bulkLen+2)
		if _, err := io.ReadFull(r.rd, payload); err != nil {
			return nil, err
		}

		// Ensure payload terminates with \r\n
		if payload[bulkLen] != '\r' || payload[bulkLen+1] != '\n' {
			return nil, fmt.Errorf("%w: missing CRLF terminator", ErrInvalidProtocol)
		}

		args = append(args, payload[:bulkLen])
	}

	return args, nil
}

// readInline parses a plain text command line separated by whitespace (telnet / netcat support).
func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	fields := bytes.Fields(line)
	if len(fields) == 0 {
		// Empty line, try reading next command
		return r.ReadCommand()
	}

	return fields, nil
}

// readLine reads bytes until \n, stripping any trailing \r\n or \n.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.rd.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	n := len(line)
	if n >= 2 && line[n-2] == '\r' && line[n-1] == '\n' {
		return line[:n-2], nil
	}
	if n >= 1 && line[n-1] == '\n' {
		return line[:n-1], nil
	}

	return line, nil
}
