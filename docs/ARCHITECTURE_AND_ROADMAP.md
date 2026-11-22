# BigCat: Architecture, Plan & Specifications

BigCat is a log-structured, append-only key-value storage engine inspired by Bitcask, designed in Go with loose coupling and plug-and-play components.

---

## 1. High-Level Architecture

```
                  ┌──────────────────────────────────────────────┐
                  │                 DB (Engine)                  │
                  │   - Open() / Close()                         │
                  │   - Put(key, val)                            │
                  │   - Get(key) -> val                          │
                  │   - Delete(key)                              │
                  └───────────────┬──────────────┬───────────────┘
                                  │              │
                   ┌──────────────▼───┐   ┌──────▼──────────────┐
                   │     Indexer      │   │     Storage / IO    │
                   │    (In-Memory)   │   │     (Append-Only)   │
                   └──────────────────┘   └──────────────┬──────┘
                                                         │
                                                  ┌──────▼──────┐
                                                  │    Codec    │
                                                  │  (Binary)   │
                                                  └─────────────┘
```

### Core Principles
1. **Append-Only Log:** Every update and delete writes a new record to the end of the file.
2. **In-Memory Hash Index (`KeyDir`):** All active keys are kept in RAM, mapping directly to byte offsets on disk.
3. **Full Record Verification:** By storing the record offset, `Get()` reads the complete record to verify CRC checksums on read before serving values.
4. **Pluggable Architecture:**
   - **Codec:** Separates serialization format from I/O logic.
   - **Indexer:** Allows swapping from a hash table to a SkipList/B-Tree (for range queries) without modifying the engine.
   - **Storage:** Abstracts file I/O for potential mmap, buffered, or mock backends.

---

## 2. On-Disk Record Format (Binary Codec)

Records are serialized sequentially using **Little Endian** byte order.

```
+----------------+------------------+----------------+----------------+----------------+-------------------+---------------------+
|   CRC (4B)     | Record Type (1B) | Timestamp (8B) | Key Size (4B)  | Value Size (4B)|    Key (bytes)    |    Value (bytes)    |
+----------------+------------------+----------------+----------------+----------------+-------------------+---------------------+
|<----------------------------------- Fixed Header (21 Bytes) ------------------------>|<--------- Variable Length Payload ------->|
```

### Field Specifications
| Field | Type | Size | Description |
|---|---|---|---|
| `CRC32` | `uint32` | 4 Bytes | CRC32 (IEEE) checksum calculated over `[RecordType + Timestamp + KeySize + ValueSize + Key + Value]` |
| `RecordType` | `uint8` | 1 Byte | `0x01` = Normal Record, `0x02` = Tombstone (Deleted) |
| `Timestamp` | `uint64` | 8 Bytes | Nanoseconds or seconds since Unix epoch |
| `KeySize` | `uint32` | 4 Bytes | Length of the key in bytes |
| `ValueSize` | `uint32` | 4 Bytes | Length of the value in bytes (0 for tombstones) |
| `Key` | `[]byte` | `KeySize` Bytes | The actual key data |
| `Value` | `[]byte` | `ValueSize` Bytes | The actual value data (empty if tombstone) |

### Tombstone & Deletions
- A deletion is written to disk as a record with `RecordType = 0x02` (Tombstone) and `ValueSize = 0`.
- When reading or rebuilding the index: encountering a tombstone removes the key from the in-memory index.

---

## 3. Dual Read Pathways & Performance Optimization

BigCat implements two distinct read pathways tailored for specific access patterns:

```
Pathway A (Sequential Scan / Startup Rebuild):
Data File ──> bufio.Reader ──> Decode(io.Reader) [Read Header (21B) ──> Read Payload]

Pathway B (Point Lookup / Get):
KeyDir Lookup ──> Exactly 1 ReadAt Syscall (RecordSize bytes) ──> DecodeBytes([]byte) [In-Memory Zero-Copy]
```

### Pathway A: Sequential File Scanning (`Decode(io.Reader)`)
* **When Used:** Rebuilding `KeyDir` on database startup (`Open()`), or during file compaction (`Merge()`).
* **The Constraint:** We are scanning an append-only file from start to finish without prior knowledge of individual record sizes.
* **Mechanism:**
  1. Read the fixed 21-byte header to unpack `KeySize` and `ValueSize`.
  2. Read the remaining variable payload (`KeySize + ValueSize`).
  3. Re-verify the CRC32 checksum before adding to `KeyDir`.
* **Optimization:** Wrap the file in `bufio.NewReaderSize(file, 256*1024)`. The OS fetches 256KB chunks into memory, meaning the two read steps happen in CPU memory rather than issuing separate kernel syscalls.

### Pathway B: Random Point Lookups (`DecodeBytes([]byte)`)
* **When Used:** Querying active keys via `Get(key)`.
* **The Advantage:** The in-memory `KeyDir` already holds both `RecordOffset` and `RecordSize` (and in multi-file phase, `FileID`).
* **Mechanism:**
  1. BigCat issues **exactly one `ReadAt` syscall** to load the entire record into a byte buffer.
  2. `DecodeBytes` directly inspects the slice, validates CRC32 over the contiguous memory in a single `crc32.ChecksumIEEE()` call, and returns zero-copy sub-slices for `Key` and `Value`.
* **Performance Benchmark:**
  * `Decode` (via `io.Reader`): `~110.8 ns/op`, `4 allocs/op`.
  * `DecodeBytes` (contiguous memory): `~43.9 ns/op`, `1 alloc/op` (**2.5x faster**, 0 extra payload copies).

---

## 4. In-Memory Index (`KeyDir`) Layout

The in-memory entry stores location and metadata for quick O(1) lookups:

```go
type Item struct {
    FileID       uint32 // 0 for single-file phase
    RecordOffset int64  // File offset where the record header begins (enables full CRC verification)
    RecordSize   uint32 // Total length of entire record (header + key + val)
    Timestamp    uint64 // Timestamp of the write
}
```

---

## 5. Multi-Phase Roadmap

### Phase 1: Single-File Storage Engine (COMPLETED ✅)
- [x] Define `Record`, `RecordType`, and `Codec` interface in `internal/record`.
- [x] Implement Little-Endian Binary Codec with CRC32 check and clear comments.
- [x] Define `Indexer` interface and in-memory map implementation in `internal/index`.
- [x] Implement single-file data storage in `internal/storage`.
- [x] Wire up main engine `DB`: `Open`, `Put`, `Get`, `Delete`, `Close`.
- [x] Unit & integration tests written via `bigcat-test-writer`.

### Phase 2: Multi-File Rotation & Directory Management
- [ ] Configurable file size threshold (e.g. 64MB).
- [ ] Active file (write-only append) vs Immutable older files (read-only).
- [ ] Directory scanner and startup recovery (rebuilding `KeyDir` from all files in sequence).

### Phase 2.5: High-Concurrency Write Optimizations (Group Commit & Lock Elision)
- [ ] Pre-encode & compute CRC32 checksums outside the write mutex (fine-grained locking).
- [ ] Group commit / write batching: aggregate concurrent client writes into batched disk flushes.
- [ ] Re-benchmark high-concurrency `SET` throughput over TCP with `redis-benchmark -c 50`.

### Phase 3: Compaction / Merging & Hint Files
- [ ] Background/manual `Merge()` to reclaim space from deleted/updated keys.
- [ ] Generation of `.hint` files containing only index headers (`Timestamp | KeySize | ValueSize | RecordOffset | Key`).
- [ ] Fast startup: Load `.hint` files first instead of scanning multi-gigabyte data files.

### Phase 4: RESP Protocol Server & CLI (COMPLETED ✅)
- [x] RESP v2 parser and serializer (`internal/resp`).
- [x] TCP command dispatcher (`PING`, `SET`, `GET`, `DEL`, `KEYS`, `COMMAND`, `CONFIG`, `QUIT`).
- [x] Full compatibility with `redis-cli` and `redis-benchmark`.
- [x] Standalone binary in `./bin/bigcat` with CLI flags and graceful shutdown.

### Phase 5: Distributed BigCat
- [ ] Containerization (Docker / Compose).
- [ ] Consensus protocol (Raft) & leader election.
- [ ] Replicated state machine with heartbeats.
