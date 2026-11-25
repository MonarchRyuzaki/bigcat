package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"strings"
	"sync"

	"bigcat"
	"bigcat/internal/resp"
)

// Server coordinates incoming TCP connections from redis-cli and dispatches
// commands to the underlying BigCat storage engine.
type Server struct {
	db       *bigcat.DB
	addr     string
	listener net.Listener
	ready    chan struct{}
	mu       sync.Mutex
	isClosed bool
	wg       sync.WaitGroup
}

// NewServer creates a new BigCat RESP server.
func NewServer(db *bigcat.DB, addr string) *Server {
	return &Server{
		db:    db,
		addr:  addr,
		ready: make(chan struct{}),
	}
}

// Ready returns a channel that is closed when the server is actively listening.
func (s *Server) Ready() <-chan struct{} {
	return s.ready
}

// Start begins listening for TCP connections on the configured address.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = l
	close(s.ready)
	s.mu.Unlock()

	log.Printf("🐾 BigCat RESP server running on %s", l.Addr().String())

	for {
		conn, err := l.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.isClosed
			s.mu.Unlock()
			if closed {
				return nil
			}
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Stop gracefully shuts down the server, closes connections, and syncs the DB.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return nil
	}
	s.isClosed = true
	l := s.listener
	s.mu.Unlock()

	var err error
	if l != nil {
		err = l.Close()
	}

	s.wg.Wait()

	if dbErr := s.db.Close(); dbErr != nil && err == nil {
		err = dbErr
	}

	return err
}

// Addr returns the network address the server is listening on.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// handleConnection reads incoming commands from a single client connection,
// executes them against the BigCat engine, and writes RESP responses.
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.wg.Done()
	}()

	reader := resp.NewReader(conn)
	bufWriter := bufio.NewWriter(conn)
	writer := resp.NewWriter(bufWriter)

	for {
		cmd, err := reader.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			_ = writer.WriteError(err.Error())
			_ = bufWriter.Flush()
			return
		}

		if len(cmd) == 0 {
			continue
		}

		commandName := strings.ToUpper(string(cmd[0]))

		switch commandName {
		case "PING":
			if len(cmd) == 1 {
				_ = writer.WriteSimpleString("PONG")
			} else if len(cmd) == 2 {
				_ = writer.WriteBulkString(cmd[1])
			} else {
				_ = writer.WriteError("wrong number of arguments for 'ping' command")
			}

		case "SET":
			if len(cmd) != 3 {
				_ = writer.WriteError("wrong number of arguments for 'set' command")
			} else {
				key := cmd[1]
				val := cmd[2]
				if err := s.db.Put(key, val); err != nil {
					_ = writer.WriteError(err.Error())
				} else {
					_ = writer.WriteSimpleString("OK")
				}
			}

		case "MSET":
			if len(cmd) < 3 || (len(cmd)-1)%2 != 0 {
				_ = writer.WriteError("wrong number of arguments for 'mset' command")
			} else {
				batch := bigcat.NewWriteBatch()
				var batchErr error
				for i := 1; i < len(cmd); i += 2 {
					if err := batch.Put(cmd[i], cmd[i+1]); err != nil {
						batchErr = err
						break
					}
				}

				if batchErr != nil {
					_ = writer.WriteError(batchErr.Error())
				} else if err := s.db.WriteBatch(batch); err != nil {
					_ = writer.WriteError(err.Error())
				} else {
					_ = writer.WriteSimpleString("OK")
				}
			}

		case "GET":
			if len(cmd) != 2 {
				_ = writer.WriteError("wrong number of arguments for 'get' command")
			} else {
				key := cmd[1]
				val, err := s.db.Get(key)
				if err != nil {
					if errors.Is(err, bigcat.ErrKeyNotFound) {
						_ = writer.WriteNull()
					} else {
						_ = writer.WriteError(err.Error())
					}
				} else {
					_ = writer.WriteBulkString(val)
				}
			}

		case "DEL":
			if len(cmd) < 2 {
				_ = writer.WriteError("wrong number of arguments for 'del' command")
			} else {
				var deletedCount int64 = 0
				for _, key := range cmd[1:] {
					if err := s.db.Delete(key); err == nil {
						deletedCount++
					}
				}
				_ = writer.WriteInteger(deletedCount)
			}

		case "KEYS":
			pattern := "*"
			if len(cmd) >= 2 {
				pattern = string(cmd[1])
			}

			allKeys := s.db.Keys()
			matched := make([][]byte, 0, len(allKeys))

			for _, k := range allKeys {
				if pattern == "*" {
					matched = append(matched, []byte(k))
					continue
				}
				// Support basic glob/wildcard matching
				if ok, _ := filepath.Match(pattern, k); ok {
					matched = append(matched, []byte(k))
				}
			}
			_ = writer.WriteArray(matched)

		case "COMMAND":
			// Handshake command sent automatically by modern redis-cli
			_ = writer.WriteEmptyArray()

		case "CONFIG":
			// Handshake command used by redis-benchmark to query server configuration.
			// Returns [param, ""] key-value pairs so redis-benchmark proceeds without warnings.
			if len(cmd) >= 3 && strings.ToUpper(string(cmd[1])) == "GET" {
				_ = writer.WriteArray([][]byte{cmd[2], []byte("")})
			} else {
				_ = writer.WriteEmptyArray()
			}

		case "QUIT":
			_ = writer.WriteSimpleString("OK")
			_ = bufWriter.Flush()
			return

		default:
			_ = writer.WriteError(fmt.Sprintf("unknown command '%s'", strings.ToLower(commandName)))
		}

		if err := bufWriter.Flush(); err != nil {
			return
		}
	}
}
