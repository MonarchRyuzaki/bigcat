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

## 3. In-Memory Index (`KeyDir`) Layout

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

## 4. Multi-Phase Roadmap

### Phase 1: Single-File Storage Engine (Current Goal)
- [ ] Define `Record`, `RecordType`, and `Codec` interface in `internal/record`.
- [ ] Implement Little-Endian Binary Codec with CRC32 check and clear comments.
- [ ] Define `Indexer` interface and in-memory map implementation in `internal/index`.
- [ ] Implement single-file data storage in `internal/storage`.
- [ ] Wire up main engine `DB`: `Open`, `Put`, `Get`, `Delete`, `Close`.
- [ ] Unit & integration tests written via `bigcat-test-writer`.

### Phase 2: Multi-File Rotation & Directory Management
- [ ] Configurable file size threshold (e.g. 64MB).
- [ ] Active file (write-only append) vs Immutable older files (read-only).
- [ ] Directory scanner and startup recovery (rebuilding `KeyDir` from all files in sequence).

### Phase 3: Compaction / Merging & Hint Files
- [ ] Background/manual `Merge()` to reclaim space from deleted/updated keys.
- [ ] Generation of `.hint` files containing only index headers (`Timestamp | KeySize | ValueSize | RecordOffset | Key`).
- [ ] Fast startup: Load `.hint` files first instead of scanning multi-gigabyte data files.

### Phase 4: CLI Interfaces
- [ ] Simple interactive REPL / string-based CLI.
- [ ] Upgrade to RESP (Redis Serialization Protocol) server so `redis-cli` can connect to BigCat!

### Phase 5: Distributed BigCat
- [ ] Containerization (Docker / Compose).
- [ ] Consensus protocol (Raft) & leader election.
- [ ] Replicated state machine with heartbeats.
