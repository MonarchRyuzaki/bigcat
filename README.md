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
                  │   - Put(key, val) / Delete(key)              │
                  │   - Get(key) -> val                          │
                  └───────┬──────────────┬───────────────┬───────┘
                          │              │               │
           ┌──────────────▼───┐   ┌──────▼────────┐   ┌──▼───────────────┐
           │     Indexer      │   │ GroupCommitter│   │   FileManager    │
           │   (In-Memory)    │   │ (Batch Flush) │   │ (Active + Older) │
           └──────────────────┘   └──────┬────────┘   └──────────┬───────┘
                                         │                       │
                                         └───────────┬───────────┘
                                                     │
                                              ┌──────▼──────┐
                                              │    Codec    │
                                              │  (Binary)   │
                                              └─────────────┘
```

### Multi-File Directory Layout
BigCat automatically manages file lifecycles and rotates files once they reach `MaxFileSize` (default 64MB):
```text
data_dir/
├── 00000.data      # Immutable data file (read-only, concurrent pread)
├── 00001.data      # Immutable data file (read-only, concurrent pread)
└── 00002.data      # Active data file (read-write appends)
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
1. **Sequential Scan (`Decode(io.Reader)`):** Used during database startup (`Open`) with a 256KB buffer across all `.data` files in ascending order to reconstruct `KeyDir` in memory.
2. **Point Lookup (`DecodeBytes([]byte)`):** Used during `Get(key)` where `KeyDir` provides `(FileID, RecordOffset, RecordSize)`. Dispatches to the exact file and executes **strictly 1 `pread(2)` syscall** directly into contiguous memory.

---

## Performance Highlights

Tested on an **AMD Ryzen 5 5600H (Linux x86_64)**:

| Operation | Throughput | Latency | Concurrency / Notes |
|---|---|---|---|
| **Durable Writes (`-sync=true`)** | **~13,245 ops/sec** | Sub-millisecond | **~53x speedup (+5,200%)** via adaptive Group Commit |
| **Point Reads (`Get`)** | **~1,285,000 ops/sec** | `778.3 ns/op` | 3 allocs/op (144 B); 1-seek pread lookups |
| **Appends (`Put` - Page Cache)** | **~574,000 ops/sec** | `1,742.0 ns/op` | Multi-core lock elision (zero mutex queueing) |
| **Live TCP SET (`redis-benchmark`)** | **~76,277 req/sec** | `0.65 ms` | 50 concurrent TCP client connections |
| **Live TCP GET (`redis-benchmark`)** | **~76,161 req/sec** | `0.65 ms` | 50 concurrent TCP client connections |
| **Live TCP Pipelined GET (`-P 16`)** | **~550,505 req/sec** | `0.03 ms` | **>0.55 Million req/sec** over loopback TCP |
| **In-Memory Index (`KeyDir`)** | **~81,900,000 ops/sec** | `12.2 ns/op` | **0 allocs/op (0 B)** |

👉 Full detailed benchmarks and methodology can be found in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

---

## 🚀 Getting Started Guide

BigCat can run both as an independent **network daemon** (compatible with any Redis client or `redis-cli`) and as an **embedded Go library**.

### 1. Prerequisites
* **Go 1.21+** (for building from source)
* Optional: `redis-tools` (to use `redis-cli` and `redis-benchmark`):
  * **Ubuntu/Debian:** `sudo apt install redis-tools`
  * **macOS:** `brew install redis`

---

### 2. Building & Running BigCat Server

Compile the standalone binary into `./bin`:
```bash
go build -o bin/bigcat ./cmd/bigcat
```

Start BigCat with default settings:
```bash
./bin/bigcat
```

You'll be greeted with BigCat's startup banner:
```text
    /\_____/\
   /  o   o  \     🐾 BigCat Key-Value Storage Engine
  ( ==  ^  == )    ------------------------------------
   )         (     📡 Port:        6380
  (           )    📂 Data Dir:    ./data
 ( (  |   |  ) )   📏 Max File:    64 MB
(__(__)___(__)__)  ⚡ Client:      redis-cli -p 6380
                   ------------------------------------
```

---

### 3. Server Configuration & CLI Flags

BigCat provides configurable flags to fine-tune durability, rotation frequency, and storage directories:

| Flag | Type | Default | Description |
|---|---|---|---|
| `-port` | `int` | `6380` | TCP port BigCat listens on. |
| `-dir` | `string` | `./data` | Filesystem directory path where all `.data` files are stored. |
| `-max-file-size` | `int64` | `67108864` (64MB) | Max size of an active file in bytes before rotating to a new immutable file. Set to smaller values (e.g. `1048576` for 1MB or `262144` for 256KB) to test rotation! |
| `-sync` | `bool` | `true` | When `true`, calls `fsync(2)` on disk after every write (maximum durability). Set to `false` for maximum write throughput via OS page cache. |

#### Example: High-Throughput Setup with 16MB File Rotation
```bash
./bin/bigcat -port 6380 -dir ./my_data -max-file-size 16777216 -sync=false
```

---

### 4. Interacting with `redis-cli`

In another terminal, connect to BigCat using standard `redis-cli`:

```bash
redis-cli -p 6380
```

#### Supported Commands:
```text
127.0.0.1:6380> PING
PONG

127.0.0.1:6380> PING "Hello BigCat!"
"Hello BigCat!"

127.0.0.1:6380> SET user:100 "Alice"
OK

127.0.0.1:6380> GET user:100
"Alice"

127.0.0.1:6380> SET user:101 "Bob"
OK

127.0.0.1:6380> KEYS *
1) "user:100"
2) "user:101"

127.0.0.1:6380> KEYS user:10*
1) "user:100"
2) "user:101"

127.0.0.1:6380> DEL user:100
(integer) 1

127.0.0.1:6380> GET user:100
(nil)

127.0.0.1:6380> QUIT
OK
```

---

### 5. Benchmarking on Your Own Machine

You can benchmark BigCat on your own hardware using `redis-benchmark`:

```bash
# 1. Benchmark concurrent point reads (GET) across 50 clients:
redis-benchmark -p 6380 -t get -n 50000 -q

# 2. Benchmark pipelined point reads (16 commands per batch):
redis-benchmark -p 6380 -t get -n 100000 -P 16 -q

# 3. Benchmark concurrent appends (SET):
redis-benchmark -p 6380 -t set -n 50000 -q
```

---

### 6. Inspecting the Multi-File Directory

As you insert data, inspect your data directory (`./data` or your custom `-dir`):
```bash
ls -lh ./data
```
You will see BigCat's Bitcask log segments:
```text
00000.data  # Older sealed segment (strictly read-only)
00001.data  # Older sealed segment (strictly read-only)
00002.data  # Current active segment (receives all new writes)
```
* **Crash-Safe:** If you restart BigCat, it scans all files from `00000.data` to the active file, recovers all keys in memory, and resumes without data loss.
* **Tombstones:** Deleted keys append a compact tombstone record, evicting the key from memory and keeping deleted keys unavailable upon reboot.

---

### 7. Embedding BigCat in Go (Library Quick Start)

BigCat can also be directly embedded in Go applications:

```go
package main

import (
	"fmt"
	"log"

	"bigcat"
)

func main() {
	// Configure options: directory, max file size, and fsync behavior
	opts := []bigcat.Option{
		bigcat.WithMaxFileSize(64 * 1024 * 1024), // Rotate at 64MB
		bigcat.WithSyncOnWrite(false),            // Rely on OS page cache for speed
	}

	// 1. Open database instance
	db, err := bigcat.Open("./data", opts...)
	if err != nil {
		log.Fatalf("failed to open BigCat: %v", err)
	}
	defer db.Close()

	// 2. Write key-value pair (appends to active file)
	if err := db.Put([]byte("hero"), []byte("batman")); err != nil {
		log.Fatal(err)
	}

	// 3. Read key-value pair (strictly 1 disk seek via in-memory KeyDir!)
	val, err := db.Get([]byte("hero"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: %s\n", string(val)) // Output: Get: batman

	// 4. Overwrite key-value pair
	if err := db.Put([]byte("hero"), []byte("catwoman")); err != nil {
		log.Fatal(err)
	}

	// 5. Delete key-value pair (appends tombstone)
	if err := db.Delete([]byte("hero")); err != nil {
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
- [x] **Phase 2: Multi-File Rotation & Directory Management**
  - [x] File rotation upon reaching size threshold (e.g. 64MB)
  - [x] Multiple immutable older files (`map[uint32]*DataFile`) + 1 active file
  - [x] Multi-file ordered startup recovery with tombstone replay
- [x] **Phase 2.5: High-Concurrency Write Optimizations**
  - [x] Lock elision: encoding and CRC32 outside database write mutex
  - [x] Group commit: coalesced sequential disk writes and hardware `fsync`
- [ ] **Batch Write API & Redis MSET**
  - [ ] Atomic `WriteBatch` API with in-memory transaction buffer
  - [ ] Multi-key Redis `MSET` command over RESP
- [ ] **Phase 3: Compaction / Merging & Hint Files**
  - [ ] Background garbage collection of overwritten/deleted records
  - [ ] `.hint` files for near-instant cold startups
- [ ] **Phase 4: Distributed BigCat**
  - [ ] Docker containerization
  - [ ] Raft consensus & replicated state machine
