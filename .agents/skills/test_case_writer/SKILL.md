---
name: bigcat-test-writer
description: Test case generator and testing suite guidelines for BigCat. Outlines patterns for unit tests, table-driven tests, CRC corruption verification, crash recovery tests, and benchmarks in Go.
---

# BigCat Test Writer Skill

## Role & Mission
You are the dedicated test engineer for **BigCat**. Your responsibility is to write comprehensive, rigorous Go test cases (`*_test.go`) to verify the engine without writing the implementation code itself.

---

## Core Testing Dimensions for BigCat

### 1. Record & Codec Tests (`internal/record`)
- **Round-Trip Serialization:**
  - Standard key/value pairs.
  - Empty keys and empty values.
  - Large values (e.g., 1MB+ payloads).
  - Keys with binary data, null bytes, unicode.
- **Data Integrity & CRC:**
  - Tampered bytes (flip 1 bit in value, verify CRC mismatch error).
  - Truncated records (cut off halfway through reading, verify `io.ErrUnexpectedEOF`).
- **Tombstones:**
  - Encoding and decoding tombstone / deleted entries.

### 2. Indexer Tests (`internal/index`)
- **CRUD on Index:**
  - Put, Get, Delete, Exists.
  - Overwriting an existing key (verifying latest offset and timestamp).
  - Deleting non-existent key vs existing key.
- **Iteration / Range:**
  - Listing all keys, ensuring accurate counts.

### 3. Storage & Engine Tests (`bigcat_test.go`)
- **Basic Put / Get / Delete:**
  - Put key, Get key -> matches.
  - Delete key, Get key -> returns `ErrKeyNotFound`.
  - Put duplicate key -> Get returns updated value.
- **Crash Recovery & Reopen:**
  - Write N keys, close database.
  - Reopen database from same directory/file -> all N keys must be readable via `KeyDir` rebuild.
  - Delete key, reopen -> deleted key remains gone.
- **Concurrency (Once Phase 4 is reached):**
  - Parallel writes and reads across goroutines (`go test -race`).
- **Benchmarks:**
  - `BenchmarkPut`, `BenchmarkGet`, `BenchmarkGetMiss`.

---

## Guidelines for Authoring Tests
1. Always use standard Go testing framework (`testing`, `require`/`assert` if allowed, or standard `t.Fatalf`).
2. Use `t.TempDir()` for temporary test databases so cleanup is automatic.
3. Keep test cases descriptive, table-driven (`[]struct{ name string, ... }`), and isolated.
