# BigCat: Durability & Crash Recovery Architecture

This document explains BigCat's durability defaults, atomic write/batch guarantees, and MIT-style `SIGKILL` & failpoint crash-recovery verification.

---

## 1. Durability Defaults (`SyncOnWrite: true`)

* **Decision:** Both `DefaultOptions` ([`options.go`](../options.go)) and the server CLI flag `-sync` ([`cmd/bigcat/main.go`](../cmd/bigcat/main.go)) default to `SyncOnWrite: true`.
* **Tradeoff:** Physical `fsync(2)` costs ~3–4 ms per sequential commit vs microsecond OS page-cache appends (`WithSyncOnWrite(false)`). Defaulting to `true` prevents silent data loss of acknowledged writes on crash/power loss, while `GroupCommitter` amortizes `fsync` across concurrent writers.

---

## 2. Write & `WriteBatch` Atomicity (`GroupCommitter`)

[`GroupCommitter.flushBatch`](../committer.go) guarantees all-or-nothing durability for both single mutations (`Put`, `Delete`) and multi-key batches (`WriteBatch`, Redis `MSET`):

1. **Pre-encoding outside locks:** Record serialization and IEEE CRC32 computation happen concurrently before entering `GroupCommitter`.
2. **Contiguous single write:** All entries in a batch (and all coalesced tasks in a group flush) are packed into a contiguous buffer and written to the active `DataFile` in a single `storage.Write` call (never split across file rotation boundaries).
3. **Sync before Ack:** When `SyncOnWrite` is enabled, `storage.Sync()` (`fsync`) completes *before* `KeyDir` is updated and waiting callers are unblocked with `OK`.

---

## 3. Startup & Compaction Recovery

On `Open(dirPath)`:
1. **`recoverMergeState(dirPath)`** runs before `OpenFileManager` discovers `*.data` files—rolling back pre-`COMMITTED` `.tmp/` directories or idempotently completing post-`COMMITTED` / post-`OLD_DELETED` swaps (see [COMPACTION.md](COMPACTION.md) for the full state machine).
2. **`loadIndexFromDisk()`** replays all file IDs in ascending order, reading companion `.hint` files (`28B` header + `Key` with CRC32) when present and falling back to raw `.data` scans otherwise.
3. **Graceful `Close()` drain:** Stops `GroupCommitter` first, then waits for any in-flight background compaction (`stopCompactor()`) before closing `FileManager` and `KeyDir`.

---

## 4. Known Boundary: Torn Tail Writes

* [`scanDataFileReader`](../db.go) verifies CRC32 on every record during startup.
* If raw hardware power loss leaves a partial/truncated trailing record at EOF (`io.ErrUnexpectedEOF` or trailing CRC mismatch), `Open()` **fails loudly** with an error (verified by `TestDB_CorruptedRecordDetectionOnStartup`) rather than silently truncating tail bytes.

---

## 5. Crash Chaos & Failpoint Verification ([`crash_test.go`](../crash_test.go))

BigCat verifies crash resilience using real subprocess terminations rather than in-process `Close()` simulation:

1. **`TestCrash_RandomSIGKILLChaos`:** Spawns child `bigcat` TCP workers (`WithMaxFileSize(256)`, `WithSyncOnWrite(true)`), hammers them concurrently with `Put`, `Delete`, atomic 2-key `WriteBatch`, `Get`, and `Merge`, fires `SIGKILL` at random intervals across 12 restart rounds, and asserts on reopen that:
   * Every acknowledged key matches its committed state.
   * Every in-flight `WriteBatch` is atomic (either all keys applied or none).
   * Zero `.tmp/` artifacts or resurrected tombstones remain.
2. **`TestCrash_DeterministicMergeFailpoints`:** Abruptly exits (`os.Exit(99)`) at 9 Phase 2/3/4 compaction points plus 3 **double-crash** scenarios (crashing inside `Merge()`, then crashing again inside `recoverMergeState()`).
3. **`TestCrash_SIGKILLDuringLargeMergeBurst`:** Fires `SIGKILL` mid-`Merge()` over 250 keys across rotated files while mutating active-file keys across 6 rounds.
