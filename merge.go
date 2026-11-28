package bigcat

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"bigcat/internal/index"
	"bigcat/internal/record"
	"bigcat/internal/storage"
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

// mergeFailpoint is a test-only hook (nil in production) invoked at critical crash
// boundaries during compaction and recovery.
var mergeFailpoint func(point string)

func hitFailpoint(point string) {
	if mergeFailpoint != nil {
		mergeFailpoint(point)
	}
}

// Merge compacts all sealed data files (excluding the active file) to reclaim disk space
// from overwritten values and tombstones without blocking ongoing Put/Delete/Get operations
// during the heavy I/O copy phase.
//
// See docs/COMPACTION.md for the full 4-phase lifecycle and crash-recovery guarantees.
func (db *DB) Merge() error {
	db.mu.RLock()
	closed := db.isClosed
	db.mu.RUnlock()
	if closed {
		return ErrDatabaseClosed
	}

	// Serialize concurrent Merge() calls so only one .tmp staging directory exists at a time.
	db.mergeMu.Lock()
	defer db.mergeMu.Unlock()

	return db.mergeLocked()
}

func (db *DB) tryBackgroundMerge() {
	if !db.mergeMu.TryLock() {
		return
	}
	defer db.mergeMu.Unlock()

	_ = db.mergeLocked()
}

func (db *DB) mergeLocked() error {
	db.mu.RLock()
	closed := db.isClosed
	db.mu.RUnlock()
	if closed {
		return ErrDatabaseClosed
	}

	// Phase 1: Snapshot sealed files at start; active file and future rotations remain untouched.
	oldIDs, activeStartID := db.storage.SealedSnapshot()
	if len(oldIDs) == 0 {
		return nil
	}

	tmpDir := filepath.Join(db.options.DirPath, mergeTempDirName)

	// Phase 2: Offline sequential copy & filtering into .tmp/ without holding db.mu.
	relocations, newIDs, err := db.stageMergedFiles(tmpDir, oldIDs, activeStartID)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}

	// Abort before committing if the database was closed while Phase 2 was running.
	db.mu.RLock()
	closed = db.isClosed
	db.mu.RUnlock()
	if closed {
		_ = os.RemoveAll(tmpDir)
		return ErrDatabaseClosed
	}

	// Phase 3: Durable commit point (manifest + COMMITTED marker).
	if err := commitMerge(tmpDir, oldIDs, newIDs); err != nil {
		if !fileExists(filepath.Join(tmpDir, mergeCommittedMarker)) {
			_ = os.RemoveAll(tmpDir)
		}
		return err
	}

	// Phase 4: Brief atomic swap of files and KeyDir pointers under db.mu.Lock().
	return db.applyCompactionSwap(tmpDir, oldIDs, newIDs, relocations)
}

func (db *DB) startCompactor() {
	if db.options.AutoCompactInterval <= 0 &&
		db.options.CompactMinFiles <= 0 &&
		db.options.CompactDeadSpaceRatio <= 0 {
		return
	}

	db.compactNotifyCh = make(chan struct{}, 1)
	db.compactStopCh = make(chan struct{})
	db.compactWg.Add(1)
	go db.compactionLoop()
}

func (db *DB) stopCompactor() {
	if db.compactStopCh == nil {
		return
	}
	db.compactStopOnce.Do(func() {
		close(db.compactStopCh)
	})
	db.compactWg.Wait()
}

func (db *DB) notifyCompaction() {
	if db.compactNotifyCh == nil {
		return
	}
	select {
	case db.compactNotifyCh <- struct{}{}:
	default:
	}
}

func (db *DB) compactionLoop() {
	defer db.compactWg.Done()

	var tickerC <-chan time.Time
	if db.options.AutoCompactInterval > 0 {
		ticker := time.NewTicker(db.options.AutoCompactInterval)
		defer ticker.Stop()
		tickerC = ticker.C
	}

	for {
		select {
		case <-db.compactStopCh:
			return
		case <-tickerC:
		case <-db.compactNotifyCh:
		}

		// Re-check stop signal before launching a merge if both fired simultaneously.
		select {
		case <-db.compactStopCh:
			return
		default:
		}

		if db.shouldCompact() {
			db.tryBackgroundMerge()
		}
	}
}

func (db *DB) shouldCompact() bool {
	sealedCount, totalBytes := db.storage.DiskStats()
	if sealedCount == 0 {
		return false
	}
	if db.options.CompactMinFiles > 0 && sealedCount < db.options.CompactMinFiles {
		return false
	}
	if db.options.CompactDeadSpaceRatio > 0 {
		if totalBytes <= 0 {
			return false
		}
		liveBytes := db.index.LiveBytes()
		deadBytes := totalBytes - liveBytes
		if deadBytes <= 0 {
			return false
		}
		deadRatio := float64(deadBytes) / float64(totalBytes)
		return deadRatio >= db.options.CompactDeadSpaceRatio
	}
	return true
}

// stageMergedFiles streams every sealed file in oldIDs sequentially, filters out obsolete
// records and tombstones by checking db.index.Get, writes surviving records into tmpDir,
// and fsyncs all generated files.
func (db *DB) stageMergedFiles(tmpDir string, oldIDs []uint32, activeStartID uint32) ([]relocation, []uint32, error) {
	if err := os.RemoveAll(tmpDir); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, nil, err
	}

	writer := newMergeWriter(tmpDir, oldIDs, db.options.MaxFileSize, activeStartID, db.hintCodec)
	var relocations []relocation
	reader := bufio.NewReaderSize(nil, 256*1024)

	for _, srcFileID := range oldIDs {
		srcPath := filepath.Join(db.options.DirPath, storage.DataFileName(srcFileID))
		f, err := os.Open(srcPath)
		if err != nil {
			writer.abort()
			return nil, nil, err
		}

		reader.Reset(f)
		var currentOffset int64

		for {
			rec, err := db.codec.Decode(reader)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = f.Close()
				writer.abort()
				return nil, nil, err
			}

			recordSize := uint32(record.HeaderSize + len(rec.Key) + len(rec.Value))

			if rec.Type == record.RecordTypeNormal {
				keyStr := string(rec.Key)
				item, exists := db.index.Get(keyStr)
				if exists && item.FileID == srcFileID && item.RecordOffset == currentOffset {
					encoded, err := db.codec.Encode(rec)
					if err != nil {
						_ = f.Close()
						writer.abort()
						return nil, nil, err
					}

					dstFileID, dstOffset, err := writer.writeRecord(rec, encoded)
					if err != nil {
						_ = f.Close()
						writer.abort()
						return nil, nil, err
					}

					relocations = append(relocations, relocation{
						key:       keyStr,
						oldFileID: srcFileID,
						oldOffset: currentOffset,
						newItem: index.Item{
							FileID:       dstFileID,
							RecordOffset: dstOffset,
							RecordSize:   uint32(len(encoded)),
							Timestamp:    rec.Timestamp,
						},
					})
					if len(relocations) == 1 {
						hitFailpoint("stage_mid_copy")
					}
				}
			}

			currentOffset += int64(recordSize)
		}

		if err := f.Close(); err != nil {
			writer.abort()
			return nil, nil, err
		}
	}

	newIDs, err := writer.finish()
	if err != nil {
		return nil, nil, err
	}
	return relocations, newIDs, nil
}

// mergeWriter writes surviving compacted records into sequential storage.DataFile segments
// alongside companion .hint files inside tmpDir, rotating at maxFileSize while capping
// FileIDs strictly below activeStartID.
type mergeWriter struct {
	tmpDir        string
	idSlots       []uint32 // Reuses oldIDs slots (guarantees every dstID < activeStartID)
	slotIdx       int
	maxFileSize   int64
	activeStartID uint32
	hintCodec     record.HintCodec
	currFile      *storage.DataFile
	currHintFile  *os.File
	currHintBuf   *bufio.Writer
	createdIDs    []uint32
}

func newMergeWriter(tmpDir string, idSlots []uint32, maxFileSize int64, activeStartID uint32, hintCodec record.HintCodec) *mergeWriter {
	return &mergeWriter{
		tmpDir:        tmpDir,
		idSlots:       idSlots,
		maxFileSize:   maxFileSize,
		activeStartID: activeStartID,
		hintCodec:     hintCodec,
	}
}

func (w *mergeWriter) openSlot(idx int) error {
	if idx < 0 || idx >= len(w.idSlots) {
		return errors.New("merge: no output file ID slots available")
	}

	id := w.idSlots[idx]
	if id >= w.activeStartID {
		return fmt.Errorf("merge: compacted file ID %d must be < active file ID %d", id, w.activeStartID)
	}

	dataPath := filepath.Join(w.tmpDir, storage.DataFileName(id))
	df, err := storage.OpenDataFileWithMode(dataPath, id, false)
	if err != nil {
		return err
	}

	hintPath := filepath.Join(w.tmpDir, storage.HintFileName(id))
	hf, err := os.OpenFile(hintPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		_ = df.Close()
		return err
	}

	if w.currHintBuf == nil {
		w.currHintBuf = bufio.NewWriterSize(hf, 64*1024)
	} else {
		w.currHintBuf.Reset(hf)
	}

	w.currFile = df
	w.currHintFile = hf
	w.slotIdx = idx
	w.createdIDs = append(w.createdIDs, id)
	return nil
}

func (w *mergeWriter) closeCurrentSlot(syncFiles bool) error {
	var firstErr error
	if w.currHintBuf != nil && w.currHintFile != nil {
		if syncFiles {
			if err := w.currHintBuf.Flush(); err != nil && firstErr == nil {
				firstErr = err
			}
			if err := w.currHintFile.Sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := w.currHintFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.currHintFile = nil
	}

	if w.currFile != nil {
		if syncFiles {
			if err := w.currFile.Sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := w.currFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.currFile = nil
	}
	return firstErr
}

// writeRecord appends an encoded record to the current staged DataFile and writes its
// corresponding HintRecord to the companion .hint file, rotating when maxFileSize is exceeded.
func (w *mergeWriter) writeRecord(rec *record.Record, encoded []byte) (fileID uint32, offset int64, err error) {
	if w.currFile == nil {
		if err := w.openSlot(0); err != nil {
			return 0, 0, err
		}
	} else if size := w.currFile.Size(); size > 0 && size+int64(len(encoded)) > w.maxFileSize && w.slotIdx+1 < len(w.idSlots) {
		if err := w.closeCurrentSlot(true); err != nil {
			return 0, 0, err
		}
		if err := w.openSlot(w.slotIdx + 1); err != nil {
			return 0, 0, err
		}
	}

	offset, err = w.currFile.Write(encoded)
	if err != nil {
		return 0, 0, err
	}

	hintBytes, err := w.hintCodec.EncodeHint(&record.HintRecord{
		Timestamp:    rec.Timestamp,
		ValueSize:    uint32(len(rec.Value)),
		RecordOffset: offset,
		Key:          rec.Key,
	})
	if err != nil {
		return 0, 0, err
	}
	if _, err := w.currHintBuf.Write(hintBytes); err != nil {
		return 0, 0, err
	}

	return w.currFile.FileID(), offset, nil
}

// finish fsyncs and closes the final open staged data and hint files and returns all created FileIDs.
func (w *mergeWriter) finish() ([]uint32, error) {
	if err := w.closeCurrentSlot(true); err != nil {
		return nil, err
	}
	return w.createdIDs, nil
}

// abort closes any open staged files without syncing on error cleanup paths.
func (w *mergeWriter) abort() {
	_ = w.closeCurrentSlot(false)
}

// commitMerge writes manifest.json and atomically creates the COMMITTED marker in tmpDir.
// After this returns nil, any crash must roll forward rather than discarding tmpDir.
func commitMerge(tmpDir string, oldIDs, newIDs []uint32) error {
	manifestPath := filepath.Join(tmpDir, mergeManifestFile)
	if err := writeManifest(manifestPath, mergeManifest{OldIDs: oldIDs, NewIDs: newIDs}); err != nil {
		return err
	}
	if err := syncDir(tmpDir); err != nil {
		return err
	}
	hitFailpoint("commit_after_manifest")

	if err := createMarkerAtomic(tmpDir, mergeCommittedMarker); err != nil {
		return err
	}
	if err := syncDir(tmpDir); err != nil {
		return err
	}
	hitFailpoint("commit_after_committed")
	return nil
}

// applyCompactionSwap performs Phase 4 under db.mu.Lock(): deletes old sealed files,
// marks OLD_DELETED, moves compacted files into DirPath, adopts them into FileManager,
// conditionally updates KeyDir pointers via CompareAndSwap, and removes tmpDir.
func (db *DB) applyCompactionSwap(tmpDir string, oldIDs, newIDs []uint32, relocations []relocation) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.isClosed {
		return ErrDatabaseClosed
	}

	hitFailpoint("swap_before_delete_old")

	// 1. Close and unlink old uncompacted files.
	if err := db.storage.CloseAndDeleteOlderFiles(oldIDs); err != nil {
		return err
	}
	hitFailpoint("swap_after_delete_old")

	// 2. Mark OLD_DELETED so crash recovery never deletes newly moved files on ID overlap.
	if err := createMarkerAtomic(tmpDir, mergeOldDeletedMarker); err != nil {
		return err
	}
	if err := syncDir(tmpDir); err != nil {
		return err
	}
	hitFailpoint("swap_after_old_deleted")

	// 3. Move compacted .data and companion .hint files from .tmp/ into DirPath.
	for i, id := range newIDs {
		for _, name := range []string{storage.DataFileName(id), storage.HintFileName(id)} {
			if err := os.Rename(filepath.Join(tmpDir, name), filepath.Join(db.options.DirPath, name)); err != nil {
				return err
			}
		}
		if i == 0 && len(newIDs) > 1 {
			hitFailpoint("swap_mid_rename")
		}
	}

	// 4. Register new compacted files into FileManager.olderFiles.
	if err := db.storage.AdoptOlderFiles(newIDs); err != nil {
		return err
	}
	hitFailpoint("swap_after_adopt")

	// 5. Conditionally update KeyDir pointers now that new files are open and registered.
	for _, r := range relocations {
		expected := index.Item{FileID: r.oldFileID, RecordOffset: r.oldOffset}
		db.index.CompareAndSwap(r.key, expected, r.newItem)
	}
	hitFailpoint("swap_after_cas")

	// 6. Persist directory renames and remove staging directory.
	if err := syncDir(db.options.DirPath); err != nil {
		return err
	}
	return os.RemoveAll(tmpDir)
}

// recoverMergeState runs during Open() before OpenFileManager scans *.data files.
// It inspects dirPath/.tmp and completes or discards any interrupted compaction idempotently.
func recoverMergeState(dirPath string) error {
	tmpDir := filepath.Join(dirPath, mergeTempDirName)
	info, err := os.Stat(tmpDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return os.RemoveAll(tmpDir)
	}

	committed := fileExists(filepath.Join(tmpDir, mergeCommittedMarker))
	oldDeleted := fileExists(filepath.Join(tmpDir, mergeOldDeletedMarker))

	// Case A: Interrupted before commit point; originals are intact so discard .tmp/.
	if !committed && !oldDeleted {
		return os.RemoveAll(tmpDir)
	}

	manifest, err := readManifest(filepath.Join(tmpDir, mergeManifestFile))
	if err != nil {
		return err
	}

	// Case B: Committed, but old files may still remain; finish unlinking and mark OLD_DELETED.
	if !oldDeleted {
		for _, id := range manifest.OldIDs {
			for _, name := range []string{storage.DataFileName(id), storage.HintFileName(id)} {
				p := filepath.Join(dirPath, name)
				if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
		}
		hitFailpoint("recover_after_delete_old")

		if err := createMarkerAtomic(tmpDir, mergeOldDeletedMarker); err != nil {
			return err
		}
		if err := syncDir(tmpDir); err != nil {
			return err
		}
		hitFailpoint("recover_after_old_deleted")
	}

	// Case C: OLD_DELETED exists; move any remaining compacted .data and .hint files into dirPath.
	for i, id := range manifest.NewIDs {
		for _, name := range []string{storage.DataFileName(id), storage.HintFileName(id)} {
			src := filepath.Join(tmpDir, name)
			dst := filepath.Join(dirPath, name)
			if fileExists(src) {
				if err := os.Rename(src, dst); err != nil {
					return err
				}
			}
		}
		if i+1 < len(manifest.NewIDs) {
			hitFailpoint("recover_mid_rename")
		}
	}

	if err := syncDir(dirPath); err != nil {
		return err
	}
	return os.RemoveAll(tmpDir)
}

func writeManifest(path string, m mergeManifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

func readManifest(path string) (mergeManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return mergeManifest{}, err
	}
	var m mergeManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return mergeManifest{}, err
	}
	return m, nil
}

func createMarkerAtomic(dir, name string) error {
	return writeFileAtomic(filepath.Join(dir, name), nil)
}

func writeFileAtomic(path string, data []byte) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	if err := d.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
