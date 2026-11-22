# 🐾 BigCat

A fast, lightweight, and embeddable log-structured key-value storage engine in Go, inspired by Bitcask.

[![Go Version](https://img.shields.io/badge/Go-1.27+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen?style=flat)]()
[![Race Detector](https://img.shields.io/badge/Race%20Detector-Clean-brightgreen?style=flat)]()
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## Overview

BigCat is designed around the principles of **Bitcask** (Justin Sheehy & David Smith, Basho Technologies):
* **Append-Only Storage:** All mutations (writes, updates, and deletes) are strictly appended sequentially to the end of the log.
* **In-Memory Hash Index (`KeyDir`):** All active keys reside in RAM, mapping directly to byte offsets on disk.
* **1-Seek Reads:** Any random read requires **at most 1 disk seek** to fetch the value.
* **Full Data Integrity:** Every record is protected by a CRC32 (IEEE) checksum and verified on every read and recovery.
* **Loose Coupling / Pluggable:** Codecs, Indexers, and Storage engines are decoupled behind clean interfaces for easy extension.

---

## Architecture

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

### On-Disk Record Format (21-Byte Header)
All integers are encoded in **Little Endian**:
```
+----------------+------------------+----------------+----------------+----------------+-------------------+---------------------+
|   CRC (4B)     | Record Type (1B) | Timestamp (8B) | Key Size (4B)  | Value Size (4B)|    Key (bytes)    |    Value (bytes)    |
+----------------+------------------+----------------+----------------+----------------+-------------------+---------------------+
|<----------------------------------- Fixed Header (21 Bytes) ------------------------>|<--------- Variable Length Payload ------->|
```

### Dual Read Pathways
1. **Sequential Scan (`Decode(io.Reader)`):** Used during database startup (`Open`) with a 256KB buffer to reconstruct `KeyDir` in memory.
2. **Point Lookup (`DecodeBytes([]byte)`):** Used during `Get(key)` where `KeyDir` already knows the record size. Issues **exactly 1 `ReadAt` syscall** and performs zero-copy in-memory decoding.

---

## Performance Highlights

Tested on an **AMD Ryzen 5 5600H (Linux x86_64)**:

| Operation | Throughput | Latency | Memory Allocs |
|---|---|---|---|
| **Point Reads (`Get`)** | **~1,285,000 ops/sec** | `778.3 ns/op` | 3 allocs/op (144 B) |
| **Appends (`Put`)** | **~632,000 ops/sec** | `1,582.0 ns/op` | 3 allocs/op (144 B) |
| **Index Lookups (`KeyDir`)** | **~81,900,000 ops/sec** | `12.2 ns/op` | **0 allocs/op (0 B)** |
| **Record Encoding** | **~17,900,000 ops/sec** | `55.8 ns/op` | 1 alloc/op (80 B) |

👉 Full detailed benchmarks and methodology can be found in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

---

## Running the Server & Using `redis-cli`

BigCat includes a built-in TCP server that speaks the **Redis Serialization Protocol (RESP v2)** natively:

```bash
# 1. Build and start BigCat
go build -o bin/bigcat ./cmd/bigcat
./bin/bigcat -port 6380 -dir ./data
```

```text
    /\_____/\
   /  o   o  \     🐾 BigCat Key-Value Storage Engine
  ( ==  ^  == )    ------------------------------------
   )         (     📡 Port:     6380
  (           )    📂 Data Dir: ./data
 ( (  |   |  ) )   ⚡ Client:   redis-cli -p 6380
(__(__)___(__)__)  ------------------------------------
```

Now connect directly with `redis-cli`:
```bash
redis-cli -p 6380
127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET myhero "batman"
OK
127.0.0.1:6380> GET myhero
"batman"
127.0.0.1:6380> KEYS *
1) "myhero"
127.0.0.1:6380> DEL myhero
(integer) 1
```

---

## Go API Quick Start

```go
package main

import (
	"fmt"
	"log"

	"bigcat"
)

func main() {
	// Open BigCat database in specified directory
	db, err := bigcat.Open("./data")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Write key-value pair
	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
	}

	// Read key-value pair (1 disk seek!)
	val, err := db.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: %s\n", string(val)) // Output: Get: world

	// Delete key-value pair (appends tombstone)
	if err := db.Delete([]byte("hello")); err != nil {
		log.Fatal(err)
	}
}
```

---

## Roadmap

- [x] **Phase 1: Single-File Storage Engine**
  - [x] Binary record codec (CRC32, Little Endian, Tombstones)
  - [x] Thread-safe in-memory `KeyDir` indexer (`sync.RWMutex`)
  - [x] Append-only file manager with stateless `pread` reads
  - [x] Crash recovery and sequential startup scan
  - [x] Comprehensive test suite & benchmarks
- [x] **RESP Protocol Server & CLI**
  - [x] RESP v2 parser and serializer (`internal/resp`)
  - [x] TCP command dispatcher (`PING`, `SET`, `GET`, `DEL`, `KEYS`, `COMMAND`, `QUIT`)
  - [x] Full compatibility with `redis-cli` and `redis-benchmark`
- [ ] **Phase 2: Multi-File Rotation & Directory Management**
  - [ ] File rotation upon reaching size threshold (e.g. 64MB)
  - [ ] Multiple immutable older files + 1 active file
  - [ ] Multi-file startup scan
- [ ] **Phase 3: Compaction / Merging & Hint Files**
  - [ ] Background garbage collection of overwritten/deleted records
  - [ ] `.hint` files for near-instant cold startups
- [ ] **Phase 4: Distributed BigCat**
  - [ ] Docker containerization
  - [ ] Raft consensus & replicated state machine
