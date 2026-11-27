package bigcat

import (
	"errors"
	"os"

	"bigcat/internal/index"
)

const (
	// mergeTempDirName is the hidden staging directory inside DirPath where compacted
	// files are written. FileManager.OpenFileManager ignores subdirectories.
	mergeTempDirName = ".tmp"

	// mergeManifestFile stores old_ids and new_ids so crash recovery knows which files to
	// delete and which compacted files to move into DirPath.
	mergeManifestFile = "manifest.json"

	// mergeCommittedMarker marks the durability point of no return after all compacted files
	// and the manifest are fsynced.
	mergeCommittedMarker = "COMMITTED"

	// mergeOldDeletedMarker marks that all old_ids have been unlinked from DirPath.
	// Required so crash recovery stays idempotent when new_ids reuse IDs from old_ids.
	mergeOldDeletedMarker = "OLD_DELETED"
)

// mergeManifest captures the exact input and output FileIDs for a compaction run.
type mergeManifest struct {
	OldIDs []uint32 `json:"old_ids"`
	NewIDs []uint32 `json:"new_ids"`
}

// relocation tracks a single surviving record's old location and its new location in the
// compacted output files so Phase 4 can conditionally update KeyDir.
type relocation struct {
	key       string
	oldFileID uint32
	oldOffset int64
	newItem   index.Item
}

// Merge compacts all sealed data files (excluding the active file) to reclaim disk space
// from overwritten values and tombstones without blocking ongoing Put/Delete/Get operations
// during the heavy I/O copy phase.
//
// See docs/COMPACTION.md for the full 4-phase lifecycle and crash-recovery guarantees.
func (db *DB) Merge() error {
	// TODO(compaction): Implement the 4-phase compaction orchestrator:
	//
	// 0. Guard & serialize:
	//    - Check db.isClosed under db.mu.RLock(); return ErrDatabaseClosed if closed.
	//    - Acquire db.mergeMu.Lock() and defer db.mergeMu.Unlock() so only one Merge runs at a time.
	//    - Re-check db.isClosed after acquiring mergeMu.
	//
	// 1. Phase 1 — Snapshot Target Set:
	//    - Call oldIDs, activeStartID := db.storage.SealedSnapshot()
	//    - If len(oldIDs) == 0, return nil (nothing to compact).
	//
	// 2. Phase 2 — Offline Copy & Staging (outside db.mu!):
	//    - Build tmpDir := filepath.Join(db.options.DirPath, mergeTempDirName).
	//    - Call relocations, newIDs, err := db.stageMergedFiles(tmpDir, oldIDs, activeStartID).
	//    - If err != nil: clean up with os.RemoveAll(tmpDir) and return err.
	//      (Originals are untouched, so failure before Phase 3 rolls back cleanly.)
	//
	// 3. Phase 3 — Durable Commit Point:
	//    - Call err = commitMerge(tmpDir, oldIDs, newIDs).
	//    - If err != nil: clean up with os.RemoveAll(tmpDir) and return err.
	//      (Once COMMITTED is fsynced, we MUST NOT delete tmpDir on failure; it will roll forward.)
	//
	// 4. Phase 4 — Atomic Memory & Disk Swap:
	//    - Return db.applyCompactionSwap(tmpDir, oldIDs, newIDs, relocations).
	return errors.New("merge: not implemented")
}

// stageMergedFiles streams every sealed file in oldIDs sequentially, filters out obsolete
// records and tombstones by checking db.index.Get, writes surviving records into tmpDir,
// and fsyncs all generated files.
func (db *DB) stageMergedFiles(tmpDir string, oldIDs []uint32, activeStartID uint32) ([]relocation, []uint32, error) {
	// TODO(compaction):
	// 1. Remove any stale tmpDir (os.RemoveAll(tmpDir)), then os.MkdirAll(tmpDir, 0755).
	// 2. Initialize writer := newMergeWriter(tmpDir, oldIDs, db.options.MaxFileSize, activeStartID).
	// 3. Initialize var relocations []relocation.
	// 4. For each srcFileID in oldIDs (already sorted ascending):
	//    a. Open filepath.Join(db.options.DirPath, storage.DataFileName(srcFileID)) for sequential read.
	//    b. Wrap in bufio.NewReaderSize(file, 256*1024) and track currentOffset := int64(0).
	//    c. Loop calling rec, err := db.codec.Decode(reader) until io.EOF (return any other error).
	//    d. Compute recordSize := uint32(record.HeaderSize + len(rec.Key) + len(rec.Value)).
	//    e. Only if rec.Type == record.RecordTypeNormal:
	//       - Lookup item, exists := db.index.Get(string(rec.Key)).
	//       - Keep ONLY if exists && item.FileID == srcFileID && item.RecordOffset == currentOffset.
	//       - Re-encode with db.codec.Encode(rec) (preserves rec.Timestamp) or copy raw bytes.
	//       - Call dstFileID, dstOffset, err := writer.writeRecord(encoded).
	//       - Append relocation{
	//             key:       string(rec.Key),
	//             oldFileID: srcFileID,
	//             oldOffset: currentOffset,
	//             newItem: index.Item{
	//                 FileID:       dstFileID,
	//                 RecordOffset: dstOffset,
	//                 RecordSize:   uint32(len(encoded)),
	//                 Timestamp:    rec.Timestamp,
	//             },
	//         }.
	//    f. Advance currentOffset += int64(recordSize) and close the source file when done.
	// 5. Call newIDs, err := writer.finish() (syncs & closes all staged .data files).
	//    Note: len(newIDs) can be 0 if all keys in oldIDs were deleted or overwritten in active files!
	// 6. Return relocations, newIDs, nil.
	return nil, nil, nil
}

// mergeWriter writes surviving compacted records into sequential .data files inside tmpDir.
// It rotates files at maxFileSize while capping output FileIDs strictly below activeStartID.
type mergeWriter struct {
	tmpDir        string
	idSlots       []uint32 // Reuses oldIDs slots (guarantees every dstID < activeStartID)
	slotIdx       int
	maxFileSize   int64
	activeStartID uint32

	currFile   *os.File
	currID     uint32
	currOffset int64
	createdIDs []uint32
}

func newMergeWriter(tmpDir string, idSlots []uint32, maxFileSize int64, activeStartID uint32) *mergeWriter {
	return &mergeWriter{
		tmpDir:        tmpDir,
		idSlots:       idSlots,
		maxFileSize:   maxFileSize,
		activeStartID: activeStartID,
	}
}

// writeRecord appends an encoded record to the current staged .data file, rotating to the
// next slot in idSlots when currOffset > 0 and currOffset + len(encoded) > maxFileSize.
func (w *mergeWriter) writeRecord(encoded []byte) (fileID uint32, offset int64, err error) {
	// TODO(compaction):
	// 1. If w.currFile == nil: open the first slot w.idSlots[0] in w.tmpDir
	//    (os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644), record it in w.createdIDs.
	// 2. Else if w.currOffset > 0 && w.currOffset+int64(len(encoded)) > w.maxFileSize && w.slotIdx+1 < len(w.idSlots):
	//    - Sync and close w.currFile.
	//    - Advance w.slotIdx++, open next w.idSlots[w.slotIdx] (guaranteed < w.activeStartID),
	//      reset w.currOffset = 0, and append new ID to w.createdIDs.
	//    (Note: if we are on the final slot w.slotIdx == len(w.idSlots)-1, e.g. because MaxFileSize
	//    was reduced across restarts, keep appending to it so we never allocate an ID >= activeStartID.)
	// 3. Write encoded bytes to w.currFile, record startOff := w.currOffset, increment w.currOffset += int64(n).
	// 4. Return w.currID, startOff, nil.
	return 0, 0, nil
}

// finish fsyncs and closes the final open staged file and returns all created FileIDs.
// On error, it should close any open handle before returning.
func (w *mergeWriter) finish() ([]uint32, error) {
	// TODO(compaction):
	// 1. If w.currFile != nil: call w.currFile.Sync(), then w.currFile.Close(), set w.currFile = nil.
	// 2. Return w.createdIDs, nil.
	return nil, nil
}

// commitMerge writes manifest.json and atomically creates the COMMITTED marker in tmpDir.
// After this returns nil, any crash must roll forward rather than discarding tmpDir.
func commitMerge(tmpDir string, oldIDs, newIDs []uint32) error {
	// TODO(compaction):
	// 1. Write mergeManifest{OldIDs: oldIDs, NewIDs: newIDs} to filepath.Join(tmpDir, mergeManifestFile)
	//    atomically (write temp file -> Sync -> Close -> os.Rename) and fsync tmpDir.
	// 2. Atomically create filepath.Join(tmpDir, mergeCommittedMarker)
	//    (write "COMMITTED.tmp" -> Sync -> Close -> os.Rename) and fsync tmpDir.
	return nil
}

// applyCompactionSwap performs Phase 4 under db.mu.Lock(): updates KeyDir pointers via
// CompareAndSwap, deletes old sealed files, marks OLD_DELETED, moves compacted files into
// DirPath, adopts them into FileManager, and removes tmpDir.
func (db *DB) applyCompactionSwap(tmpDir string, oldIDs, newIDs []uint32, relocations []relocation) error {
	// TODO(compaction):
	// 1. Acquire db.mu.Lock() and defer db.mu.Unlock().
	// 2. If db.isClosed, return ErrDatabaseClosed.
	//
	// 3. Update KeyDir conditionally:
	//    For each r in relocations:
	//      expected := index.Item{FileID: r.oldFileID, RecordOffset: r.oldOffset}
	//      db.index.CompareAndSwap(r.key, expected, r.newItem)
	//
	// 4. Close and unlink old uncompacted files:
	//    Call db.storage.CloseAndDeleteOlderFiles(oldIDs).
	//
	// 5. Write & fsync OLD_DELETED marker in tmpDir (so crash recovery knows oldIDs are gone
	//    and won't delete newly moved files that happen to share the same FileIDs):
	//    Create mergeOldDeletedMarker atomically + syncDir(tmpDir).
	//
	// 6. Move compacted files from tmpDir into db.options.DirPath:
	//    For each id in newIDs:
	//      os.Rename(filepath.Join(tmpDir, storage.DataFileName(id)),
	//                filepath.Join(db.options.DirPath, storage.DataFileName(id)))
	//
	// 7. Register new compacted files into FileManager.olderFiles:
	//    Call db.storage.AdoptOlderFiles(newIDs).
	//
	// 8. Persist directory entries and clean up staging directory:
	//    Call syncDir(db.options.DirPath), then os.RemoveAll(tmpDir).
	return nil
}

// recoverMergeState runs during Open() before OpenFileManager scans *.data files.
// It inspects dirPath/.tmp and completes or discards any interrupted compaction idempotently.
func recoverMergeState(dirPath string) error {
	// TODO(compaction): Implement the crash recovery matrix from docs/COMPACTION.md:
	//
	// tmpDir := filepath.Join(dirPath, mergeTempDirName)
	// 1. If tmpDir does not exist (os.IsNotExist): return nil.
	//
	// 2. Check for COMMITTED and OLD_DELETED markers inside tmpDir:
	//    - Case A: Neither COMMITTED nor OLD_DELETED exists:
	//      Crash happened before commit point (Phase 1/2/3). Originals are intact.
	//      Run os.RemoveAll(tmpDir) and return.
	//
	//    - Case B: COMMITTED exists, OLD_DELETED does NOT exist:
	//      Read mergeManifest from filepath.Join(tmpDir, mergeManifestFile).
	//      For each id in manifest.OldIDs:
	//        Remove filepath.Join(dirPath, storage.DataFileName(id)) (ignore os.ErrNotExist).
	//      Create mergeOldDeletedMarker atomically + syncDir(tmpDir).
	//      (Fall through to Case C.)
	//
	//    - Case C: OLD_DELETED exists:
	//      Read mergeManifest (if not already read in Case B).
	//      For each id in manifest.NewIDs:
	//        src := filepath.Join(tmpDir, storage.DataFileName(id))
	//        dst := filepath.Join(dirPath, storage.DataFileName(id))
	//        If src exists: os.Rename(src, dst). (If already moved before a prior crash, skip.)
	//      Call syncDir(dirPath), then os.RemoveAll(tmpDir).
	return nil
}

// TODO(compaction): Add helper functions as you implement merge.go:
// - writeManifest(path string, m mergeManifest) error
// - readManifest(path string) (mergeManifest, error)
// - createMarkerAtomic(dir, name string) error
// - syncDir(dir string) error (open dir with os.Open(dir), call Sync(), Close() — ignore EINVAL on filesystems that don't support dir fsync if needed)
