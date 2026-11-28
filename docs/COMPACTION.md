# BigCat: Compaction, Hint Files & Background Triggers (`Merge`)

This document describes the architecture, design rationale, crash-recovery state machine, companion `.hint` files, and background compaction triggers for BigCat's `Merge()` engine.

---

## 1. Problem & Goals

BigCat uses an append-only log across rotating `.data` files (`00000.data`, `00001.data`, ..., `00005.data`). Every update appends a new record and every delete appends a tombstone, leaving superseded values and tombstones behind in older sealed files.

Compaction must satisfy four invariants:
1. **Reclaim disk space & reduce file count:** Merge sparse sealed files (e.g., `00001–00004`) into fewer dense `.data` files packed up to `MaxFileSize`.
2. **Non-blocking normal traffic:** `Put`, `Delete`, `WriteBatch`, and `Get` must remain unblocked during the heavy disk copy phase; the active file continues accepting writes and rotating normally.
3. **Crash-safe & idempotent:** A crash or power loss at *any* point during compaction or recovery must never lose live data, resurrect deleted keys, or leave corrupt records behind.
4. **Preserve startup recovery invariants:** [`OpenFileManager`](../internal/storage/file_manager.go#L75-L142) and [`loadIndexFromDisk`](../db.go#L82-L102) must continue to recover the exact database state without special-case startup replay rules.

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
   During startup, [`loadIndexFromDisk`](../db.go#L82-L102) replays all `.data` files in ascending `FileID` order, using `RecordTypeTombstone` entries to remove previously seen keys from `KeyDir`.
   * Suppose key `"k"` was written (`Put`) in `00001.data` and deleted (`Tombstone`) in `00003.data`.
   * If `00003.data` were compacted independently and dropped the tombstone while `00001.data` still held the original `Put("k")`, `"k"` would **resurrect on the next `Open()`**.
   * By compacting a **contiguous prefix starting from the oldest sealed file** (`00001–00004`) together, there are **no older files left behind `00004`**. Every tombstone inside `00001–00004` can be safely discarded. *(If `"k"` was deleted in active file `00005`, `KeyDir` has no `"k"`, so any older `Put("k")` in `00001–00004` is skipped while `00005`'s tombstone stays until `00005` is later sealed and compacted.)*

### Why We Check `db.index` Directly (Instead of Building a Separate `mergeMap`)

During Phase 2, compaction filters records by querying `db.index.Get(key)` rather than building a second in-memory hash map over `00001–00004`:
1. **No write contention:** `MapIndex.Get()` only takes `RLock()` (~12 ns, 0 allocations), so it never blocks concurrent `DB.Get()` readers.
2. **One sequential pass:** As records stream from a 256KB buffered reader, we already have `(Key, Value)` in memory—no second pass or random `ReadAt` seeks required.
3. **Half the RAM:** Avoids duplicating `KeyDir` in memory.
4. **Sees active-file updates/deletes:** Keys overwritten or deleted in `00005` are skipped immediately instead of being copied unnecessarily.

---

## 3. Core Invariant: Compacted `FileID`s Must Stay `< activeFile.FileID()`

BigCat's storage layer enforces two strict ordering rules:
1. [`OpenFileManager`](../internal/storage/file_manager.go#L117-L139) sorts all `*.data` files ascending and opens the **highest** `FileID` as `activeFile` (read-write).
2. [`loadIndexFromDisk`](../db.go#L82-L102) replays files in **ascending `FileID` order**, where later records overwrite earlier ones.

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

Compaction is serialized via `db.mergeMu` (preventing concurrent `Merge()` runs) and executed in four phases using a hidden staging directory `data_dir/.tmp/`:

```text
data_dir/
├── .tmp/                    # Ignored by OpenFileManager (subdirectories are skipped)
│   ├── 00001.data           # Newly compacted output file(s)
│   ├── 00002.data
│   ├── manifest.json        # Lists old_ids=[1,2,3,4] and new_ids=[1,2]
│   ├── COMMITTED            # Durable commit marker (point of no return)
│   └── OLD_DELETED          # Marker written once old_ids are unlinked
├── 00001.data               # Original sealed files (untouched until Phase 4)
├── ...
├── 00004.data
└── 00005.data               # Active file (receives live Put/Delete throughout)
```

> **Note:** [`OpenFileManager`](../internal/storage/file_manager.go#L87-L89) ignores subdirectories (`if item.IsDir() { continue }`), so `.tmp/` never interferes with normal file discovery.

### Phase 1: Snapshot Target Set (Instant)
* [`SealedSnapshot`](../internal/storage/file_manager.go#L242-L256) returns all sealed file IDs sorted ascending (`oldIDs`) and the current active file ID (`activeStartID`).
* If `len(oldIDs) == 0`, `Merge()` returns `nil` (nothing to compact).
* `00005.data` (and any new files `00006+` created by rotation during compaction) are excluded from this run.

### Phase 2: Offline Copy & Staging (Heavy I/O, Non-Blocking)
* Wipe any stale `.tmp/` and recreate `data_dir/.tmp/`.
* Scan `oldIDs` sequentially using a 256KB `bufio.Reader`:
  * For each record decoded at `(srcFileID, srcOffset)`:
    * Skip tombstones (`RecordTypeTombstone`).
    * Query `db.index.Get(string(rec.Key))`.
    * Keep the record **only if** `exists && item.FileID == srcFileID && item.RecordOffset == srcOffset`.
    * Re-encode via `db.codec.Encode(rec)` (preserving `rec.Timestamp`) and append into `.tmp/` via `mergeWriter` (which reuses [`storage.DataFile`](../internal/storage/file.go#L10-L15) and rotates through `oldIDs` slots at `MaxFileSize`, capped `< activeStartID`).
    * Append `(key, oldFileID, oldOffset, newItem)` to an in-memory `relocations` list.
* `finish()` `fsync`s and closes the final staged `DataFile`.

#### Why We Do NOT Resume Phase 2 Mid-Copy After a Crash
We explicitly chose **not** to track `(srcFileID, srcOffset)` checkpoints to resume Phase 2 after a crash:
1. **Lost relocation map:** The in-memory `relocations` list needed to update `KeyDir` in Phase 4 is lost on crash, forcing a re-scan of `.tmp/` anyway.
2. **Torn trailing writes:** Writing to `.tmp/00001.data` and updating a checkpoint file are not atomic together; a crash mid-write leaves partial trailing bytes that would corrupt CRC checks if appended to on resume.
3. **Stale data baked in:** A record copied into `.tmp/` right before a `Put`/`Delete` to `00005.data` and an immediate crash would leave dead data inside `.tmp/` without its source-offset metadata.

Because `00001–00004` are completely unmodified during Phase 2, discarding `.tmp/` before commit is simple, instant, and zero-risk.

### Phase 3: Durable Commit Point
* [`commitMerge`](../merge.go#L296-L308) atomically writes `.tmp/manifest.json` (`old_ids`, `new_ids`) + `syncDir`, then atomically creates `.tmp/COMMITTED` + `syncDir`.
* **This is the durability boundary:**
  * Crash *before* `COMMITTED` exists -> roll back by deleting `.tmp/`.
  * Crash *after* `COMMITTED` exists -> roll forward on next `Open()`.

### Phase 4: Atomic Memory & Disk Swap (Brief Lock)
Because `new_ids` reuse IDs from `old_ids` (`1, 2` overlap `[1, 2, 3, 4]`), **deleting old files, moving new files into place, registering them in `FileManager`, and updating `KeyDir` must appear atomic to concurrent `Get()` and `GroupCommitter.flushBatch()` calls.**

[`applyCompactionSwap`](../merge.go#L313-L358) acquires `db.mu.Lock()` (no bulk copying or data `fsync`s remain—only fast pointer swaps and directory metadata updates):

1. **Close and unlink `old_ids`:**
   [`CloseAndDeleteOlderFiles`](../internal/storage/file_manager.go#L261-L279) removes `oldIDs` from `FileManager.olderFiles`, closes their `*DataFile` handles, and removes `00001–00004.data` from `data_dir/`.
2. **Record `OLD_DELETED` marker:**
   Atomically create `.tmp/OLD_DELETED` and `syncDir(.tmp)`.
   * *Why `OLD_DELETED` is required:* If a crash occurs *after* moving `.tmp/00001.data -> data_dir/00001.data` and *before* moving `.tmp/00002.data`, recovery must **not** re-run step 1 (`delete old_ids [1,2,3,4]`), or it would delete the newly moved `00001.data`. `OLD_DELETED` guarantees recovery jumps straight to moving remaining `new_ids`.
3. **Move & register `new_ids`:**
   For each ID in `newIDs`, `os.Rename` `.tmp/<id>.data` -> `data_dir/<id>.data`, then call [`AdoptOlderFiles`](../internal/storage/file_manager.go#L284-L311) to open them read-only and register them in `FileManager.olderFiles`.
4. **Conditionally update `KeyDir`:**
   For each entry in `relocations`, call [`CompareAndSwap`](../internal/index/map.go#L29-L46) (`expected = {oldFileID, oldOffset}`, `updated = newItem`). Any key overwritten or deleted in `00005+` during Phase 2 has a different `FileID`/`RecordOffset` (or is absent) and is left untouched. Updating `KeyDir` after `AdoptOlderFiles` succeeds also guarantees `KeyDir` never points at un-adopted files if an OS error interrupts steps 1–3.
5. **Finalize:**
   `syncDir(data_dir)`, remove `.tmp/`, and release `db.mu.Unlock()`.

At this point, `Merge()` is complete and returns `nil`.

---

## 5. End-to-End Walkthrough Example

Suppose `MaxFileSize = 100 bytes` and we start with **3 files**:

* **`00000.data`** *(sealed)*
  * offset `0`: `Put("a", "old_a")` (30B)
  * offset `30`: `Put("b", "val_b")` (30B)
  * offset `60`: `Put("c", "val_c")` (30B)
* **`00001.data`** *(sealed)*
  * offset `0`: `Put("a", "mid_a")` (30B) — *overwrote `"a"`*
  * offset `30`: `Tombstone("b")` (22B) — *deleted `"b"`*
  * offset `52`: `Put("d", "val_d")` (30B)
* **`00002.data`** *(active — receiving live writes)*
  * offset `0`: `Put("a", "new_a")` (30B) — *overwrote `"a"` again*

In RAM, **`KeyDir` (`db.index`)** currently maps:
* `"a"` -> `(FileID: 2, Offset: 0)`
* `"c"` -> `(FileID: 0, Offset: 60)`
* `"d"` -> `(FileID: 1, Offset: 52)`
* *(`"b"` is absent because it was deleted.)*

Now `db.Merge()` runs:

1. **Phase 1 (Snapshot):** `SealedSnapshot()` freezes `oldIDs = [0, 1]` and `activeStartID = 2`. `00002.data` keeps accepting writes normally.
2. **Phase 2 (Scan & Stage into `.tmp/`):**
   * Scanning `00000.data`:
     * `"a"` at `(0, 0)`: `KeyDir["a"]` is `(2, 0)` != `(0, 0)` -> **Skip** (stale).
     * `"b"` at `(0, 30)`: `KeyDir["b"]` missing -> **Skip** (deleted).
     * `"c"` at `(0, 60)`: `KeyDir["c"]` is `(0, 60)` -> **Match!** Written to `.tmp/00000.data` at offset `0` (30B). Relocation recorded: `"c": (0, 60) -> (0, 0)`.
   * Scanning `00001.data`:
     * `"a"` at `(1, 0)`: `KeyDir["a"]` is `(2, 0)` != `(1, 0)` -> **Skip** (stale).
     * `Tombstone("b")` at `(1, 30)`: **Skip** (safe because everything from `0` up to `1` is compacted—no older `Put("b")` exists behind `1`).
     * `"d"` at `(1, 52)`: `KeyDir["d"]` is `(1, 52)` -> **Match!** `30 + 30 = 60 <= 100`, so it fits into `.tmp/00000.data` at offset `30`. Relocation recorded: `"d": (1, 52) -> (0, 30)`.
   * **Concurrent write during Phase 2:** Meanwhile, a client calls `db.Put("c", "brand_new_c")`. `GroupCommitter` appends it to active `00002.data` at offset `30` and updates `KeyDir["c"] = (2, 30)`.
   * `writer.finish()` syncs and closes `.tmp/00000.data`, returning `newIDs = [0]`. Two files totaling 172B shrank into one 60B file.
3. **Phase 3 (Commit):** Writes `.tmp/manifest.json` (`old_ids: [0, 1]`, `new_ids: [0]`) and `.tmp/COMMITTED`.
4. **Phase 4 (Swap under `db.mu.Lock()`):**
   * Closes & unlinks `00000.data` and `00001.data`.
   * Writes `.tmp/OLD_DELETED`.
   * Renames `.tmp/00000.data -> data_dir/00000.data` and registers it via `AdoptOlderFiles([0])`.
   * Updates `KeyDir`:
     * `CompareAndSwap("c", expected=(0, 60), updated=(0, 0))` sees `(2, 30)` != `(0, 60)` -> **Skips!** (`"c"` keeps `"brand_new_c"` in `00002.data`.)
     * `CompareAndSwap("d", expected=(1, 52), updated=(0, 30))` sees `(1, 52)` == `(1, 52)` -> **Swaps!** (`"d"` now points to `(0, 30)` in new `00000.data`.)
   * Syncs `data_dir` and removes `.tmp/`.

**Result:** Disk holds `00000.data` (60B, sealed) and `00002.data` (60B, active); `00001.data` is reclaimed; `KeyDir` maps `"a" -> (2, 0)`, `"c" -> (2, 30)`, `"d" -> (0, 30)`. On reboot, `Open()` scans `00000.data` then `00002.data`, producing the exact same state.

---

## 6. Crash Recovery Matrix (`Open()`)

Before calling [`OpenFileManager`](../internal/storage/file_manager.go#L75), [`Open`](../db.go#L32-L49) runs [`recoverMergeState`](../merge.go#L362-L420) to inspect `data_dir/.tmp/`:

| Disk State on Startup | When Crash Happened | Recovery Action (Idempotent) |
|---|---|---|
| No `.tmp/` directory | Normal shutdown / no compaction | Proceed straight to `OpenFileManager` |
| `.tmp/` exists, no `COMMITTED` | During Phase 1 or Phase 2 | `os.RemoveAll(".tmp")` (originals `001–004` intact) |
| `COMMITTED` exists, no `OLD_DELETED` | Right after commit or mid-unlink of `001–004` | Read `manifest.json`, delete any remaining `old_ids` in `data_dir/`, create & `fsync` `OLD_DELETED`, then execute the row below |
| `OLD_DELETED` exists | Mid-move of `.tmp/` -> `data_dir/` or before `.tmp/` cleanup | Read `manifest.json`, move any remaining `new_ids` from `.tmp/` into `data_dir/`, `fsync(data_dir)`, `os.RemoveAll(".tmp")` |

Every recovery transition is idempotent across repeated crashes during recovery.

---

## 7. Concurrency Guarantees

1. **Reader Safety in [`DB.Get`](../db.go#L179-L210):**
   `DB.Get` holds `db.mu.RLock()` across both `db.index.Get` and `db.storage.ReadAt`. All concurrent `Get()` callers run in parallel under `RLock`, while [`GroupCommitter.flushBatch`](../committer.go#L168-L205) and Phase 4 acquire `db.mu.Lock()`—preventing a TOCTOU race where a reader sees a `FileID` that gets deleted or swapped before `ReadAt` runs.
2. **Conditional Index Updates ([`MapIndex.CompareAndSwap`](../internal/index/map.go#L29-L46)):**
   Checks `cur.FileID == expected.FileID && cur.RecordOffset == expected.RecordOffset` under `m.mu.Lock()` before swapping, ensuring Phase 2 relocations never overwrite concurrent writes or deletes from `00005+`.

---

## 8. Companion `.hint` Files & Fast Cold Startup

During Phase 2 (`stageMergedFiles`), `mergeWriter` writes a companion `<file_id>.hint` file alongside every compacted `<file_id>.data` file:
* **Binary Format ([`internal/record/hint.go`](../internal/record/hint.go)):** 28-byte Little-Endian header + variable key (`CRC32 (4B) | Timestamp (8B) | KeySize (4B) | ValueSize (4B) | RecordOffset (8B) | Key (bytes)`), verified with IEEE CRC32.
* **Atomic Lifecycle:** Staged `.hint` files are flushed, `fsync`ed, renamed, and crash-recovered alongside `.data` files in Phase 4 and `recoverMergeState`. `CloseAndDeleteOlderFiles` unlinks both `.data` and `.hint` for old files so no orphaned `.hint` files remain.
* **Startup Recovery ([`loadIndexFromDisk`](../db.go)):** Checks `<file_id>.hint` first (`scanHintFile`, skipping value payloads on disk) and falls back to scanning raw `.data` when absent (e.g., active or unmerged files).

---

## 9. Automatic Background Compaction Triggers & `COMPACT` Command

Compaction can run manually (`db.Merge()` or RESP `COMPACT`) or automatically via a background worker started in `Open()`:

* **Options ([`options.go`](../options.go)) & CLI ([`cmd/bigcat/main.go`](../cmd/bigcat/main.go)):**
  * `WithAutoCompactInterval` (`-compact-interval`, CLI default `5m`): Periodic ticker checks.
  * `WithCompactMinFiles` (`-compact-min-files`, CLI default `4`): Minimum sealed `.data` files required before compacting.
  * `WithCompactDeadSpaceRatio` (`-compact-dead-ratio`, CLI default `0.4`): Minimum dead-space fraction (`0.0–1.0]`).
* **Rotation Wakeups & Overlap Guard:** Whenever `GroupCommitter.flushBatch()` observes a `FileID` change (active file rotated), it sends a non-blocking signal on `compactNotifyCh`. The worker calls `tryBackgroundMerge()` using `mergeMu.TryLock()` so overlapping triggers skip cleanly rather than queueing.
* **Graceful Shutdown (`DB.Close()`):** Stops `GroupCommitter` first (preventing new rotation signals), closes `compactStopCh` and waits for `compactWg.Wait()` so any in-flight `Merge()` finishes before closing `FileManager` and `KeyDir`.

### Design Decision: O(1) Global Counter Heuristic for Dead Space (`totalBytes` vs `liveBytes`)

To evaluate `CompactDeadSpaceRatio` without scanning `KeyDir` ($O(N_{\text{keys}})$) or maintaining per-file byte maps:
* **`FileManager.totalBytes`** tracks total on-disk bytes across all `.data` files (`activeFile` + `olderFiles`) via `Stat()` on `Open()`, `Write()`, `CloseAndDeleteOlderFiles()`, and `AdoptOlderFiles()`.
* **`MapIndex.liveBytes`** tracks total `RecordSize` of all active keys via `Put()`, `Delete()`, and `CompareAndSwap()`.
* `shouldCompact()` computes `deadBytes = totalBytes - liveBytes` and `deadRatio = deadBytes / totalBytes` in **$O(1)$ time with zero allocations**.

> **Intentional Approximation Tradeoff:** Because `totalBytes` and `liveBytes` include the **active file** as well as sealed files, heavy overwrites inside the current active file raise the global `deadRatio` even though `Merge()` only compacts sealed files. Pairing `CompactDeadSpaceRatio` with `CompactMinFiles` (e.g., `4`) bounds this effect—once multiple sealed files exist, sealed segments dominate `totalBytes`, making the global ratio a reliable, zero-overhead trigger heuristic.

---

## 10. Edge Cases

* **Zero surviving keys:** If every key in `00001–00004` was overwritten in `00005` or deleted, `new_ids` in `.tmp/manifest.json` is empty (`[]`). Phase 4 deletes `00001–00004` (and any old `.hint` files), leaves `00005` as the sole file, and removes `.tmp/`.
* **`MaxFileSize` shrunk across restarts:** If `00001–00004` were originally created with `MaxFileSize = 64MB` and the DB is later reopened with `1MB`, splitting surviving data strictly at `1MB` could require more than 4 output files and collide with `00005`. [`mergeWriter`](../merge.go) caps rotation to the slots in `oldIDs` (`< activeStartID`), continuing to append to the final slot instead of allocating an ID `>= activeStartID`.
* **Single file only (`len(olderFiles) == 0`):** If only the active file exists, `Merge()` returns `nil` as a no-op (or callers may explicitly `Rotate()` beforehand if they wish to compact the current active file).
