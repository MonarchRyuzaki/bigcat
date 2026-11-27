# BigCat: Compaction & Merging Design (`Merge`)

This document describes the architecture, decision rationale, crash-recovery state machine, and concurrency model for BigCat's log compaction process (`Merge()`).

> **Scope Note:** Companion `.hint` file generation for fast startup recovery is intentionally excluded from this document and will layer cleanly onto the staging pipeline described below in a subsequent step.

---

## 1. Problem & Goals

BigCat uses an append-only log across rotating `.data` files (`00000.data`, `00001.data`, ..., `00005.data`). Every update appends a new record and every delete appends a tombstone, leaving superseded values and tombstones behind in older sealed files.

Compaction must satisfy four invariants:
1. **Reclaim disk space & reduce file count:** Merge sparse sealed files (e.g., `00001–00004`) into fewer dense `.data` files packed up to `MaxFileSize`.
2. **Non-blocking normal traffic:** `Put`, `Delete`, `WriteBatch`, and `Get` must remain unblocked during the heavy disk copy phase; the active file continues accepting writes and rotating normally.
3. **Crash-safe & idempotent:** A crash or power loss at *any* point during compaction or recovery must never lose live data, resurrect deleted keys, or leave corrupt records behind.
4. **Preserve startup recovery invariants:** [`OpenFileManager`](../internal/storage/file_manager.go#L75-L142) and [`loadIndexFromDisk`](../db.go#L74-L94) must continue to recover the exact database state without special-case startup replay rules.

---

## 2. Design Decision: Why Whole-Range Prefix Compaction?

We evaluated two compaction strategies for a directory with sealed files `00001–00004` and active file `00005`:

| Criteria | Option A: One-by-One (`001 -> new 001`, `002 -> new 002`) | Option B: Whole-Range Prefix (`001–004 -> 001–00k`) ✅ |
|---|---|---|
| **Disk Packing** | Poor — 4 files at 20% live density remain 4 tiny sparse files | Optimal — packs surviving records into 1–2 dense files |
| **Tombstone Removal** | Unsafe in isolation (see below) | Safe — zero older files remain behind the compacted range |
| **Crash & Read Safety** | Still cannot overwrite in-place; needs staging & swap anyway | Same staging mechanism yields full packing & cleanup |

### Why One-by-One Compaction Breaks Down

1. **Fragmentation persists:** Rewriting each file individually never consolidates small partially-dead files into full `MaxFileSize` segments, leaving unnecessary open file descriptors and fragmentation.
2. **The Tombstone Resurrection Trap:**
   During startup, [`loadIndexFromDisk`](../db.go#L74-L94) replays all `.data` files in ascending `FileID` order, using `RecordTypeTombstone` entries to remove previously seen keys from `KeyDir`.
   * Suppose key `"k"` was written (`Put`) in `00001.data` and deleted (`Tombstone`) in `00003.data`.
   * If `00003.data` were compacted independently and dropped the tombstone while `00001.data` still held the original `Put("k")`, `"k"` would **resurrect on the next `Open()`**.
   * By compacting a **contiguous prefix starting from the oldest sealed file** (`00001–00004`) together, there are **no older files left behind `00004`**. Every tombstone inside `00001–00004` can be safely discarded. *(If `"k"` was deleted in active file `00005`, `KeyDir` has no `"k"`, so any older `Put("k")` in `00001–00004` is skipped while `00005`'s tombstone stays until `00005` is later sealed and compacted.)*

---

## 3. Core Invariant: Compacted `FileID`s Must Stay `< activeFile.FileID()`

BigCat's storage layer enforces two strict ordering rules:
1. [`OpenFileManager`](../internal/storage/file_manager.go#L117-L139) sorts all `*.data` files ascending and opens the **highest** `FileID` as `activeFile` (read-write).
2. [`loadIndexFromDisk`](../db.go#L74-L94) replays files in **ascending `FileID` order**, where later records overwrite earlier ones.

### Why New Higher `FileID`s (`00006.data`) Would Corrupt Data
If `00005.data` is active and compacted output were written to `00006.data`:
* **Stale-over-fresh corruption:** If `"x"` was copied from `00002.data` into `00006.data` while a concurrent `Put("x", "new")` or `Delete("x")` landed in `00005.data`, then on restart `00005` replays *before* `0006` — silently overwriting newer writes (or resurrecting a deleted key) with stale data.
* **Wrong active file:** On restart, `OpenFileManager` would pick `00006.data` as the new `activeFile` instead of `00005.data`.

### Chosen Rule
Compacted output files reuse `FileID`s **strictly smaller than the active file's ID** at compaction start (e.g., `00001..00004` compact into `00001..00002`, while active remains `00005`):
* Chronological order (`compacted < active`) holds permanently, so `00005+` always wins on startup replay.
* [`FileManager`](../internal/storage/file_manager.go#L117-L129) already supports non-contiguous IDs (e.g. `[1, 2, 5]`), and [`rotateLocked`](../internal/storage/file_manager.go#L201) advances from the active file (`currID + 1`).

---

## 4. Compaction Lifecycle (4 Phases)

Compaction is serialized via a dedicated `mergeMu` (preventing concurrent `Merge()` runs) and executed in four phases using a hidden staging directory `data_dir/.tmp/`:

```text
data_dir/
├── .tmp/                    # Ignored by OpenFileManager (subdirectories are skipped)
│   ├── 00001.data           # Newly compacted output file(s)
│   ├── 00002.data
│   ├── manifest             # Lists old_ids=[1,2,3,4] and new_ids=[1,2]
│   ├── COMMITTED            # Durable commit marker (point of no return)
│   └── OLD_DELETED          # Marker written once old_ids are unlinked
├── 00001.data               # Original sealed files (untouched until Phase 4)
├── ...
├── 00004.data
└── 00005.data               # Active file (receives live Put/Delete throughout)
```

> **Note:** [`OpenFileManager`](../internal/storage/file_manager.go#L87-L89) ignores subdirectories (`if item.IsDir() { continue }`), so `.tmp/` never interferes with normal file discovery.

### Phase 1: Snapshot Target Set (Instant)
* Read `storage.FileIDs()` and collect all sealed file IDs strictly less than `activeFile.FileID()` (e.g., `old_ids = [1, 2, 3, 4]`).
* If `len(old_ids) == 0`, return `nil` (nothing to compact).
* `00005.data` (and any new files `00006+` created by rotation during compaction) are excluded from this run.

### Phase 2: Offline Copy & Staging (Heavy I/O, Non-Blocking)
* Create `data_dir/.tmp/`.
* Scan `old_ids` (`00001` -> `00004`) sequentially using buffered I/O:
  * For each record decoded at `(srcFileID, srcOffset)`:
    * Skip tombstones (`RecordTypeTombstone`).
    * Query `db.index.Get(string(rec.Key))`.
    * Keep the record **only if** `exists && item.FileID == srcFileID && item.RecordOffset == srcOffset`.
    * Re-encode/copy the raw record into the current `.tmp/` output file (rolling to the next ID when `size + recordLen > MaxFileSize`, capped strictly `< activeStartID`).
    * Append `(key, srcFileID, srcOffset, dstFileID, dstOffset, recordSize, timestamp)` to an in-memory `relocations` list.
* `fsync()` every created `.tmp/*.data` file.
* Write `.tmp/manifest` (recording `old_ids` and `new_ids`), `fsync()` it, and `fsync(.tmp/)`.

#### Why We Do NOT Resume Phase 2 Mid-Copy After a Crash
We explicitly chose **not** to track `(srcFileID, srcOffset)` checkpoints to resume Phase 2 after a crash:
1. **Lost relocation map:** The in-memory `relocations` list needed to update `KeyDir` in Phase 4 is lost on crash, forcing a re-scan of `.tmp/` anyway.
2. **Torn trailing writes:** Writing to `.tmp/00001.data` and updating a checkpoint file are not atomic together; a crash mid-write leaves partial trailing bytes that would corrupt CRC checks if appended to on resume.
3. **Stale data baked in:** A record copied into `.tmp/` right before a `Put`/`Delete` to `00005.data` and an immediate crash would leave dead data inside `.tmp/` without its source-offset metadata.

Because `00001–00004` are completely unmodified during Phase 2, discarding `.tmp/` before commit is simple, instant, and zero-risk.

### Phase 3: Durable Commit Point
* Atomically create `.tmp/COMMITTED` (write `.tmp/COMMITTED.tmp` -> `fsync` -> `os.Rename` to `.tmp/COMMITTED`) and `fsync(.tmp/)`.
* **This is the durability boundary:**
  * Crash *before* `COMMITTED` exists -> roll back by deleting `.tmp/`.
  * Crash *after* `COMMITTED` exists -> roll forward on next `Open()`.

### Phase 4: Atomic Memory & Disk Swap (Brief Lock)
Because `new_ids` reuse IDs from `old_ids` (`1, 2` overlap `[1, 2, 3, 4]`), **updating `KeyDir`, updating `FileManager`, deleting old files, and moving new files into place must appear atomic to concurrent `Get()` and `GroupCommitter.flushBatch()` calls.**

Acquire `db.mu.Lock()` (no bulk copying or data `fsync`s remain—only fast pointer swaps and directory metadata updates):

1. **Conditionally update `KeyDir`:**
   For each entry in `relocations`, update `KeyDir` to `(dstFileID, dstOffset)` **only if** `KeyDir` still points to `(srcFileID, srcOffset)`. Any key overwritten or deleted in `00005+` during Phase 2 has a different `FileID`/`RecordOffset` (or is absent) and is left untouched.
2. **Close and unlink `old_ids`:**
   Remove `old_ids` from `FileManager.olderFiles`, close their `*DataFile` handles, and `os.Remove` `00001–00004.data` from `data_dir/`.
3. **Record `OLD_DELETED` marker:**
   Create and `fsync` `.tmp/OLD_DELETED` (and `fsync(.tmp/)`).
   * *Why `OLD_DELETED` is required:* If a crash occurs *after* moving `.tmp/00001.data -> data_dir/00001.data` and *before* moving `.tmp/00002.data`, recovery must **not** re-run step 2 (`delete old_ids [1,2,3,4]`), or it would delete the newly moved `00001.data`. `OLD_DELETED` guarantees recovery jumps straight to moving remaining `new_ids`.
4. **Move & register `new_ids`:**
   For each ID in `new_ids`, `os.Rename` `.tmp/<id>.data` -> `data_dir/<id>.data`, open it read-only via `OpenDataFileWithMode(..., true)`, and insert it into `FileManager.olderFiles`.
5. **Finalize:**
   `fsync(data_dir)` so directory entries are durable, remove `.tmp/`, and release `db.mu.Unlock()`.

At this point, `Merge()` is complete and returns `nil`.

---

## 5. Crash Recovery Matrix (`Open()`)

Before calling [`OpenFileManager`](../internal/storage/file_manager.go#L75), [`Open`](../db.go#L30) checks `data_dir/.tmp/`:

| Disk State on Startup | When Crash Happened | Recovery Action (Idempotent) |
|---|---|---|
| No `.tmp/` directory | Normal shutdown / no compaction | Proceed straight to `OpenFileManager` |
| `.tmp/` exists, no `COMMITTED` | During Phase 1 or Phase 2 | `os.RemoveAll(".tmp")` (originals `001–004` intact) |
| `COMMITTED` exists, no `OLD_DELETED` | Right after commit or mid-unlink of `001–004` | Read `manifest`, delete any remaining `old_ids` in `data_dir/`, create & `fsync` `OLD_DELETED`, then execute the row below |
| `OLD_DELETED` exists | Mid-move of `.tmp/` -> `data_dir/` or before `.tmp/` cleanup | Read `manifest`, move any remaining `new_ids` from `.tmp/` into `data_dir/`, `fsync(data_dir)`, `os.RemoveAll(".tmp")` |

Every recovery transition is idempotent across repeated crashes during recovery.

---

## 6. Concurrency Adjustments Required in Existing Code

Implementing compaction safely requires two targeted updates to the existing concurrency paths:

1. **Fix TOCTOU Race in [`DB.Get`](../db.go#L171-L207):**
   Currently, [`DB.Get`](../db.go#L176-L193) acquires and immediately releases `db.mu.RLock()` *before* calling `db.index.Get()` and `db.storage.ReadAt()`. Once Phase 4 can swap `olderFiles` and reuse IDs, a `Get` paused between `index.Get` and `ReadAt` could hit `ErrFileNotFound` or read the wrong offset in a newly swapped file.
   * **Fix:** Hold `db.mu.RLock()` across `index.Get` + `storage.ReadAt` (all concurrent `Get`s still run in parallel under `RLock`, while [`GroupCommitter.flushBatch`](../committer.go#L168-L205) and Phase 4 acquire `db.mu.Lock()`).
2. **Conditional Index Update ([`Indexer`](../internal/index/index.go#L23-L41)):**
   Add an atomic `CompareAndSwap(key string, expected, updated Item) bool` method (or perform check-and-update under `db.mu.Lock()` while `flushBatch` is paused) so stale Phase 2 relocations can never clobber concurrent writes from `00005+`.

---

## 7. Edge Cases

* **Zero surviving keys:** If every key in `00001–00004` was overwritten in `00005` or deleted, `new_ids` in `.tmp/manifest` is empty (`[]`). Phase 4 deletes `00001–00004`, leaves `00005` as the sole file, and removes `.tmp/`.
* **`MaxFileSize` shrunk across restarts:** If `00001–00004` were originally created with `MaxFileSize = 64MB` and the DB is later reopened with `1MB`, splitting surviving data strictly at `1MB` could require more than 4 output files and collide with `00005`. Compaction must cap output `FileID` rotation strictly below `activeStartID` (once the last available ID `activeStartID - 1` is reached, keep appending to it instead of rotating past `activeStartID`).
* **Single file only (`len(olderFiles) == 0`):** If only the active file exists, `Merge()` returns `nil` as a no-op (or callers may explicitly `Rotate()` beforehand if they wish to compact the current active file).
