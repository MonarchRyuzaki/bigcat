---
name: bigcat-pre-ship-harness
description: Mandatory pre-commit and pre-ship verification harness for BigCat. Enforces code review, explicit red-flag reporting, normal + race + MIT-style SIGKILL/failpoint crash tests, and benchmark recording before committing or shipping.
---

# BigCat Pre-Ship Verification Harness

## Purpose
No feature or fix in BigCat may be committed or shipped (`git commit`, `git push`, `gh pr create`) until every gate in this harness passes. A storage engine must be **correct, crash-resilient, race-free, and benchmarked**.

---

## Mandatory Gates (Execute In Order Before Commit/Ship)

### Gate 1: Rigorous Code Review
Review all modified and added files against the `bigcat-code-reviewer` checklist:
- [ ] **Correctness & Invariants:** Bitcask log ordering (`FileID` monotonicity, tombstone replay, `KeyDir` consistency), CRC32 integrity, Little-Endian encoding.
- [ ] **Concurrency & Locking:** No TOCTOU gaps, strict lock ordering (`db.mu -> {FileManager.mu, MapIndex.mu}`), no `ReadAt` on unpinned/deleted files.
- [ ] **Durability & Crash Safety:** `SyncOnWrite: true` default honored, atomic `write-temp -> fsync -> rename` for metadata/manifests, idempotent startup recovery.
- [ ] **Resource Management:** No leaked file descriptors on error paths, clean `Close()` draining.

### Gate 2: Red-Flag Check (Stop & Surface Risks)
If **any** bug, race condition, durability gap, missing error check, silent data-loss scenario, or significant performance regression is detected:
- **DO NOT** bury, ignore, or work around it silently.
- **RAISE THE FLAG EXPLICITLY** to the user with exact file/line references and failure scenario before committing or shipping.

### Gate 3: Comprehensive Testing (Normal + Crash Chaos + Race)
1. **Unit & Integration Tests:**
   - Ensure table-driven unit tests (`bigcat-test-writer` patterns) cover normal paths, edge cases (empty/nil inputs, boundaries, corruption, error rollbacks), and multi-file rotation/reopen lifecycles.
2. **MIT-Style Crash & Fault-Injection Tests:**
   - Whenever persistence, recovery, batching, compaction, or file-lifecycle logic changes, verify both:
     - **Randomized `SIGKILL` subprocess chaos** (concurrent `Put`/`Delete`/`WriteBatch`/`Get`/`Merge` killed abruptly across multiple restart rounds, verifying acked vs. in-flight invariants and batch atomicity).
     - **Deterministic failpoints** (hard process exits at every critical metadata/swap/recovery boundary, including chained crashes inside recovery).
3. **Full Race-Enabled Suite:**
   - Run `go vet ./... && go test -race -count=1 ./...` and confirm **100% pass** with zero race warnings and zero skipped critical assertions.

### Gate 4: Performance Benchmark & Note-Down
1. **Run Benchmarks:**
   - Run `go test -bench=. -benchmem -run=^$ ./...` (or subsystem benchmarks for touched packages; for write-path changes, benchmark both `SyncOnWrite=true` durable and `SyncOnWrite=false` page-cache behavior).
2. **Record Results:**
   - Update `docs/BENCHMARKS.md` (when engine/storage/network performance characteristics change) and/or record exact observed numbers (`ns/op`, `B/op`, `allocs/op`, throughput) in the PR summary.
   - Never estimate or extrapolate unprinted numbers—only record directly observed benchmark output.

### Gate 5: Documentation & Decision Log
1. **Document Design Decisions & Tradeoffs:**
   - Record **why** key architectural choices, heuristics, approximations, and constraints exist (e.g. in `docs/COMPACTION.md`, `docs/ARCHITECTURE_AND_ROADMAP.md`, or inline doc comments) so future contributors understand the rationale and edge cases.
   - When introducing a new subsystem or major feature, create a dedicated markdown document under `docs/` (e.g., `docs/<FEATURE>.md`) rather than cramming unrelated subsystems into one file.
2. **Sync User-Facing Docs & Roadmaps:**
   - Update `README.md` (CLI flags, commands, examples, roadmap checkboxes) and `docs/` whenever public behavior, defaults, or roadmap milestones change.

### Gate 6: Ship (Only After Gates 1–5 Are Green)
1. Inspect `git status` and `git diff` to verify only intended changes and updated docs are included.
2. Commit on a descriptive branch (linked to the GitHub issue), push to `origin`, and open a Pull Request via `gh pr create` referencing the issue and summarizing test + benchmark + decision verification.
