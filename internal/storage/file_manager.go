package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

var (
	// ErrFileNotFound is returned when requesting a file ID that does not exist.
	ErrFileNotFound = errors.New("storage: data file not found")

	// ErrNoActiveFile is returned when an operation requires an active file but none is open.
	ErrNoActiveFile = errors.New("storage: no active data file")
)

const (
	// DataFileSuffix is the extension used for all BigCat data files.
	DataFileSuffix = ".data"

	// DataFileFormat defines the zero-padded 5-digit file naming scheme (e.g. 00000.data).
	DataFileFormat = "%05d" + DataFileSuffix

	// HintFileSuffix is the extension used for companion BigCat hint files.
	HintFileSuffix = ".hint"

	// HintFileFormat defines the zero-padded 5-digit hint file naming scheme (e.g. 00000.hint).
	HintFileFormat = "%05d" + HintFileSuffix
)

// DataFileName formats a numeric file ID into its standardized filename (e.g., 00001.data).
func DataFileName(id uint32) string {
	return fmt.Sprintf(DataFileFormat, id)
}

// HintFileName formats a numeric file ID into its companion hint filename (e.g., 00001.hint).
func HintFileName(id uint32) string {
	return fmt.Sprintf(HintFileFormat, id)
}

// ParseFileID extracts the numeric file ID from a filename like "00001.data".
// It returns an error if the extension is not ".data" or the base name is not numeric.
func ParseFileID(name string) (uint32, error) {
	if !strings.HasSuffix(name, DataFileSuffix) {
		return 0, fmt.Errorf("invalid data file extension: %s", name)
	}

	base := strings.TrimSuffix(name, DataFileSuffix)
	id64, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid data file name %q: %w", name, err)
	}

	return uint32(id64), nil
}

// FileManager manages multiple append-only and immutable read-only DataFiles in a directory.
//
// Invariants:
//  1. Exactly ONE active file exists at any time for appending new writes.
//  2. All older files are strictly read-only and immutable.
//  3. When activeFile.Size() + recordSize > maxFileSize, activeFile is sealed into olderFiles
//     and a new activeFile with ID = currentID + 1 is opened.
type FileManager struct {
	mu          sync.RWMutex
	dirPath     string
	maxFileSize int64

	// activeFile is the only file receiving append writes.
	activeFile *DataFile

	// olderFiles stores sealed, immutable data files indexed by fileID.
	// Reads against these files do not contend with active file appends.
	olderFiles map[uint32]*DataFile
}

// OpenFileManager initializes or opens an existing multi-file storage directory.
// If the directory contains no data files, it initializes 00000.data as the active file.
// If data files exist, the highest file ID is opened as the active read-write append file,
// while all lower file IDs are opened in read-only mode in olderFiles.
func OpenFileManager(dirPath string, maxFileSize int64) (*FileManager, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	dirItems, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var fileIDs []uint32
	for _, item := range dirItems {
		if item.IsDir() {
			continue
		}
		if strings.HasSuffix(item.Name(), DataFileSuffix) {
			id, err := ParseFileID(item.Name())
			if err != nil {
				continue
			}
			fileIDs = append(fileIDs, id)
		}
	}

	fm := &FileManager{
		dirPath:     dirPath,
		maxFileSize: maxFileSize,
		olderFiles:  make(map[uint32]*DataFile, len(fileIDs)),
	}

	// Case 1: Fresh directory with no data files -> create 00000.data
	if len(fileIDs) == 0 {
		activePath := filepath.Join(dirPath, DataFileName(0))
		activeFile, err := OpenDataFileWithMode(activePath, 0, false)
		if err != nil {
			return nil, err
		}
		fm.activeFile = activeFile
		return fm, nil
	}

	// Case 2: Existing data files -> sort by numeric ID ascending
	slices.Sort(fileIDs)

	// Open all older files in read-only mode
	for i := 0; i < len(fileIDs)-1; i++ {
		id := fileIDs[i]
		path := filepath.Join(dirPath, DataFileName(id))
		df, err := OpenDataFileWithMode(path, id, true)
		if err != nil {
			_ = fm.Close()
			return nil, err
		}
		fm.olderFiles[id] = df
	}

	// Open highest ID in read-write append mode (readOnly = false)
	activeID := fileIDs[len(fileIDs)-1]
	activePath := filepath.Join(dirPath, DataFileName(activeID))
	activeFile, err := OpenDataFileWithMode(activePath, activeID, false)
	if err != nil {
		_ = fm.Close()
		return nil, err
	}
	fm.activeFile = activeFile

	return fm, nil
}

// Write appends data to the current active file.
// If appending data would cause the active file to exceed maxFileSize, it automatically
// rotates to a new active file before performing the write.
//
// Returns:
// - fileID: the ID of the file the data was written to
// - offset: the byte offset where the record begins
// - err: any error encountered
func (fm *FileManager) Write(data []byte) (fileID uint32, offset int64, err error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.activeFile == nil {
		return 0, 0, ErrNoActiveFile
	}

	if fm.activeFile.Size()+int64(len(data)) > fm.maxFileSize {
		if err := fm.rotateLocked(); err != nil {
			return 0, 0, err
		}
	}

	offset, err = fm.activeFile.Write(data)
	if err != nil {
		return 0, 0, err
	}

	return fm.activeFile.FileID(), offset, nil
}

// Rotate manually triggers rotation of the active file.
// It syncs the current active file, seals it into the olderFiles map as immutable,
// and opens a new active file with ID = currentID + 1.
func (fm *FileManager) Rotate() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	return fm.rotateLocked()
}

// rotateLocked rotates the active file while fm.mu is already held.
// It syncs the active file and transitions its handle into olderFiles without closing it,
// ensuring in-flight concurrent pread(2) readers are not interrupted.
func (fm *FileManager) rotateLocked() error {
	if fm.activeFile == nil {
		return ErrNoActiveFile
	}

	// 1. Flush all pending writes of the current active file to disk
	if err := fm.activeFile.Sync(); err != nil {
		return err
	}

	// 2. Transfer active file directly to olderFiles (zero syscalls, no reader disruption)
	currID := fm.activeFile.FileID()
	fm.olderFiles[currID] = fm.activeFile

	// 3. Open next active file in read-write append mode
	nextID := currID + 1
	nextPath := filepath.Join(fm.dirPath, DataFileName(nextID))
	newActive, err := OpenDataFileWithMode(nextPath, nextID, false)
	if err != nil {
		return err
	}
	fm.activeFile = newActive

	return nil
}

// ReadAt reads length bytes starting at offset from the data file specified by fileID.
// It selects the target file under RLock, releases the manager lock, and executes
// the pread(2) syscall concurrently without blocking active file appends.
func (fm *FileManager) ReadAt(fileID uint32, offset int64, length uint32) ([]byte, error) {
	fm.mu.RLock()
	var target *DataFile
	if fm.activeFile != nil && fileID == fm.activeFile.FileID() {
		target = fm.activeFile
	} else {
		target = fm.olderFiles[fileID]
	}
	fm.mu.RUnlock()

	if target == nil {
		return nil, ErrFileNotFound
	}

	return target.ReadAt(offset, length)
}

// ActiveFile returns the current active file.
func (fm *FileManager) ActiveFile() *DataFile {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.activeFile
}

// SealedSnapshot atomically returns all sealed (olderFiles) IDs sorted in ascending order,
// along with the current activeFile ID at the moment of the snapshot.
// Compaction uses this in Phase 1 to freeze the set of files to merge and enforce dstID < activeID.
func (fm *FileManager) SealedSnapshot() (sealedIDs []uint32, activeID uint32) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if fm.activeFile != nil {
		activeID = fm.activeFile.FileID()
	}

	sealedIDs = make([]uint32, 0, len(fm.olderFiles))
	for k := range fm.olderFiles {
		sealedIDs = append(sealedIDs, k)
	}
	slices.Sort(sealedIDs)
	return sealedIDs, activeID
}

// CloseAndDeleteOlderFiles detaches and closes the DataFile handles for fileIDs from olderFiles,
// and unlinks their corresponding .data and companion .hint files from fm.dirPath.
// Called during compaction Phase 4 under DB lock before moving compacted files out of .tmp/.
func (fm *FileManager) CloseAndDeleteOlderFiles(fileIDs []uint32) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	for _, id := range fileIDs {
		if df, ok := fm.olderFiles[id]; ok {
			delete(fm.olderFiles, id)
			if err := df.Close(); err != nil {
				return err
			}
		}

		for _, name := range []string{DataFileName(id), HintFileName(id)} {
			err := os.Remove(filepath.Join(fm.dirPath, name))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	return nil
}

// AdoptOlderFiles opens already-moved compacted .data files in fm.dirPath in read-only mode
// and registers them into fm.olderFiles.
// Called during compaction Phase 4 right after renaming compacted files from .tmp/ into fm.dirPath.
func (fm *FileManager) AdoptOlderFiles(fileIDs []uint32) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	opened := make(map[uint32]*DataFile, len(fileIDs))
	for _, id := range fileIDs {
		if _, dup := opened[id]; dup {
			continue
		}
		path := filepath.Join(fm.dirPath, DataFileName(id))
		df, err := OpenDataFileWithMode(path, id, true)
		if err != nil {
			for _, f := range opened {
				_ = f.Close()
			}
			return err
		}
		opened[id] = df
	}

	for id, df := range opened {
		if existing, ok := fm.olderFiles[id]; ok {
			_ = existing.Close()
		}
		fm.olderFiles[id] = df
	}
	return nil
}

// FileIDs returns all data file IDs currently managed (both older and active),
// sorted in ascending order.
func (fm *FileManager) FileIDs() []uint32 {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	fileIDs := make([]uint32, 0, len(fm.olderFiles)+1)
	for id := range fm.olderFiles {
		fileIDs = append(fileIDs, id)
	}
	if fm.activeFile != nil {
		fileIDs = append(fileIDs, fm.activeFile.FileID())
	}

	slices.Sort(fileIDs)
	return fileIDs
}

// GetFile returns a DataFile handle for the given fileID (either active or older).
// Useful for startup recovery and compaction scans.
func (fm *FileManager) GetFile(fileID uint32) (*DataFile, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if fm.activeFile != nil && fm.activeFile.FileID() == fileID {
		return fm.activeFile, nil
	}
	if file, ok := fm.olderFiles[fileID]; ok {
		return file, nil
	}
	return nil, ErrFileNotFound
}

// Sync commits the contents of the active file to stable disk storage.
func (fm *FileManager) Sync() error {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	if fm.activeFile == nil {
		return nil
	}
	return fm.activeFile.Sync()
}

// Close flushes and closes all open data files (the active file and all older files).
func (fm *FileManager) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	var firstErr error

	// Close active file
	if fm.activeFile != nil {
		if err := fm.activeFile.Sync(); err != nil {
			firstErr = err
		}
		if err := fm.activeFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		fm.activeFile = nil
	}

	// Close all older files
	for id, file := range fm.olderFiles {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(fm.olderFiles, id)
	}

	return firstErr
}
