package server

import (
	"bufio"
	"net"
	"strings"
	"testing"

	"bigcat"
)

func TestServer_CommandsOverTCP(t *testing.T) {
	dir := t.TempDir()
	db, err := bigcat.Open(dir)
	if err != nil {
		t.Fatalf("Open DB failed: %v", err)
	}

	// Listen on random port
	srv := NewServer(db, "127.0.0.1:0")
	go func() {
		_ = srv.Start()
	}()

	// Wait for listener to be active
	<-srv.Ready()
	addr := srv.Addr()
	if addr == nil {
		t.Fatalf("server failed to bind address")
	}

	defer srv.Stop()

	// Connect TCP client
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Helper to send line and read response
	sendAndReceive := func(cmd string) string {
		if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatalf("failed to write cmd %s: %v", cmd, err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed to read response for cmd %s: %v", cmd, err)
		}
		return strings.TrimRight(resp, "\r\n")
	}

	// 1. PING & PING with message
	if res := sendAndReceive("PING"); res != "+PONG" {
		t.Errorf("expected +PONG, got %q", res)
	}
	if res := sendAndReceive("PING mymsg"); res != "$5" {
		t.Errorf("expected bulk length $5 for PING mymsg, got %q", res)
	}
	echoPayload, _ := reader.ReadString('\n')
	if strings.TrimRight(echoPayload, "\r\n") != "mymsg" {
		t.Errorf("expected mymsg, got %q", echoPayload)
	}

	// 2. COMMAND handshake
	if res := sendAndReceive("COMMAND"); res != "*0" {
		t.Errorf("expected *0 for COMMAND, got %q", res)
	}

	// 3. SET
	if res := sendAndReceive("SET hero batman"); res != "+OK" {
		t.Errorf("expected +OK, got %q", res)
	}

	// 4. GET
	if res := sendAndReceive("GET hero"); res != "$6" {
		t.Errorf("expected length $6, got %q", res)
	}
	// Read payload
	payload, _ := reader.ReadString('\n')
	if strings.TrimRight(payload, "\r\n") != "batman" {
		t.Errorf("expected batman, got %q", payload)
	}

	// 5. GET non-existent
	if res := sendAndReceive("GET unknown"); res != "$-1" {
		t.Errorf("expected $-1, got %q", res)
	}

	// 6. KEYS with pattern
	if res := sendAndReceive("KEYS her*"); res != "*1" {
		t.Errorf("expected array length *1, got %q", res)
	}
	keyLen, _ := reader.ReadString('\n')
	keyPayload, _ := reader.ReadString('\n')
	if strings.TrimRight(keyLen, "\r\n") != "$4" || strings.TrimRight(keyPayload, "\r\n") != "hero" {
		t.Errorf("expected key hero, got %s / %s", keyLen, keyPayload)
	}

	// 7. Unknown command handling
	if res := sendAndReceive("FOOBAR"); !strings.HasPrefix(res, "-ERR unknown command") {
		t.Errorf("expected -ERR unknown command, got %q", res)
	}

	// 8. Multi-key MSET & argument validation
	if res := sendAndReceive("MSET"); !strings.HasPrefix(res, "-ERR wrong number of arguments") {
		t.Errorf("expected wrong number of arguments for MSET, got %q", res)
	}
	if res := sendAndReceive("MSET k1 v1 k2"); !strings.HasPrefix(res, "-ERR wrong number of arguments") {
		t.Errorf("expected wrong number of arguments for odd MSET, got %q", res)
	}
	if res := sendAndReceive("MSET m1 mv1 m2 mv2"); res != "+OK" {
		t.Errorf("expected +OK for MSET, got %q", res)
	}
	if res := sendAndReceive("GET m2"); res != "$3" {
		t.Errorf("expected $3 for GET m2, got %q", res)
	}
	m2Payload, _ := reader.ReadString('\n')
	if strings.TrimRight(m2Payload, "\r\n") != "mv2" {
		t.Errorf("expected mv2, got %q", m2Payload)
	}

	// 9. Multi-key DEL
	_ = sendAndReceive("SET k1 v1")
	_ = sendAndReceive("SET k2 v2")
	if res := sendAndReceive("DEL hero k1 k2 m1 m2 missing"); res != ":5" {
		t.Errorf("expected :5 deleted keys, got %q", res)
	}

	// 10. COMPACT command & arity check
	if res := sendAndReceive("COMPACT extra"); !strings.HasPrefix(res, "-ERR wrong number of arguments") {
		t.Errorf("expected wrong number of arguments for COMPACT, got %q", res)
	}
	if res := sendAndReceive("COMPACT"); res != "+OK" {
		t.Errorf("expected +OK for COMPACT, got %q", res)
	}

	// 11. QUIT
	if res := sendAndReceive("QUIT"); res != "+OK" {
		t.Errorf("expected +OK on QUIT, got %q", res)
	}
}

func TestServer_MultiFileRotationOverTCP(t *testing.T) {
	dir := t.TempDir()
	// Set small 256-byte max file size to trigger multiple rotations over TCP
	db, err := bigcat.Open(dir, bigcat.WithMaxFileSize(256))
	if err != nil {
		t.Fatalf("Open DB failed: %v", err)
	}

	srv := NewServer(db, "127.0.0.1:0")
	go func() {
		_ = srv.Start()
	}()
	<-srv.Ready()
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	sendAndReceive := func(cmd string) string {
		if _, err := conn.Write([]byte(cmd + "\r\n")); err != nil {
			t.Fatalf("failed to write cmd %s: %v", cmd, err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("failed to read response for cmd %s: %v", cmd, err)
		}
		return strings.TrimRight(resp, "\r\n")
	}

	// 1. Write 15 keys over TCP (causing multiple rotations)
	for i := 0; i < 15; i++ {
		cmd := "SET key:" + string(rune('a'+i)) + " value_payload_number_" + string(rune('0'+i))
		if res := sendAndReceive(cmd); res != "+OK" {
			t.Fatalf("expected +OK, got %q for cmd %s", res, cmd)
		}
	}

	// 2. Read back all 15 keys over TCP
	for i := 0; i < 15; i++ {
		cmd := "GET key:" + string(rune('a'+i))
		res := sendAndReceive(cmd)
		if !strings.HasPrefix(res, "$") {
			t.Fatalf("expected bulk string prefix $, got %q for %s", res, cmd)
		}
		payload, _ := reader.ReadString('\n')
		expected := "value_payload_number_" + string(rune('0'+i))
		if strings.TrimRight(payload, "\r\n") != expected {
			t.Fatalf("value mismatch: expected %s, got %s", expected, payload)
		}
	}

	// 3. Delete an older key
	if res := sendAndReceive("DEL key:a"); res != ":1" {
		t.Fatalf("expected :1 for DEL key:a, got %q", res)
	}
	if res := sendAndReceive("GET key:a"); res != "$-1" {
		t.Fatalf("expected $-1 for deleted key:a, got %q", res)
	}

	// 4. Trigger compaction over TCP and verify remaining keys still read cleanly
	if res := sendAndReceive("COMPACT"); res != "+OK" {
		t.Fatalf("expected +OK for COMPACT, got %q", res)
	}
	if res := sendAndReceive("GET key:a"); res != "$-1" {
		t.Fatalf("expected $-1 for deleted key:a after COMPACT, got %q", res)
	}
	if res := sendAndReceive("GET key:b"); !strings.HasPrefix(res, "$") {
		t.Fatalf("expected bulk string prefix $ for key:b after COMPACT, got %q", res)
	}
	payloadB, _ := reader.ReadString('\n')
	if strings.TrimRight(payloadB, "\r\n") != "value_payload_number_1" {
		t.Fatalf("value mismatch for key:b after COMPACT: got %q", payloadB)
	}
}


func BenchmarkServer_SetOverTCP(b *testing.B) {
	dir := b.TempDir()
	db, err := bigcat.Open(dir)
	if err != nil {
		b.Fatal(err)
	}

	srv := NewServer(db, "127.0.0.1:0")
	go func() {
		_ = srv.Start()
	}()
	<-srv.Ready()
	defer srv.Stop()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	cmd := []byte("*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nval12\r\n")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(cmd); err != nil {
			b.Fatal(err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil || resp != "+OK\r\n" {
			b.Fatalf("unexpected resp: %q, err: %v", resp, err)
		}
	}
}
