package bigcat

import (
	"errors"
	"sync"

	"bigcat/internal/index"
)

var (
	// ErrCommitterClosed is returned when submitting to a closed committer.
	ErrCommitterClosed = errors.New("committer: closed")
)

const (
	// DefaultMaxBatchSize is the maximum number of operations to coalesce into a single disk write.
	DefaultMaxBatchSize = 128

	// DefaultMaxBatchBytes is the maximum total payload size (4MB) to coalesce into a single disk write.
	DefaultMaxBatchBytes = 4 * 1024 * 1024

	// DefaultBatchQueueCapacity is the buffer capacity for the incoming task channel.
	DefaultBatchQueueCapacity = 1024
)

// writeEntry represents a single encoded Put or Delete mutation.
type writeEntry struct {
	key       []byte
	encoded   []byte
	isDelete  bool
	timestamp uint64
}

// writeTask represents a single write (Put, Delete, or atomic WriteBatch) queued for group commitment.
// All entries inside a single writeTask are guaranteed to be committed together atomically.
type writeTask struct {
	entries []writeEntry
	err     error
	done    chan struct{}
}

func (t *writeTask) totalBytes() int {
	total := 0
	for _, e := range t.entries {
		total += len(e.encoded)
	}
	return total
}

// GroupCommitter coalesces concurrent write requests from multiple goroutines into
// a single sequential disk write (and a single fsync, if SyncOnWrite=true).
//
// Concurrency Model:
//   - Multiple client goroutines call Submit() simultaneously.
//   - Callers wait on task.done.
//   - A single worker loop gathers all currently pending tasks, writes their combined
//     encoded bytes in ONE single storage.Write call, calls storage.Sync ONCE,
//     updates the in-memory KeyDir for all keys, and wakes up all waiting callers.
type GroupCommitter struct {
	db            *DB
	mu            sync.RWMutex
	queue         chan *writeTask
	isClosed      bool
	closeOnce     sync.Once
	wg            sync.WaitGroup
	maxBatchSize  int
	maxBatchBytes int
	hasLastFileID bool
	lastFileID    uint32
}

// newGroupCommitter creates and starts a new GroupCommitter worker.
func newGroupCommitter(db *DB, maxBatchSize, maxBatchBytes int) *GroupCommitter {
	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	if maxBatchBytes <= 0 {
		maxBatchBytes = DefaultMaxBatchBytes
	}

	gc := &GroupCommitter{
		db:            db,
		queue:         make(chan *writeTask, DefaultBatchQueueCapacity),
		maxBatchSize:  maxBatchSize,
		maxBatchBytes: maxBatchBytes,
	}

	gc.wg.Add(1)
	go gc.workerLoop()

	return gc
}

// Submit enqueues a write task and blocks until the task has been written to disk
// (and fsynced, if SyncOnWrite is configured).
func (gc *GroupCommitter) Submit(task *writeTask) error {
	gc.mu.RLock()
	if gc.isClosed {
		gc.mu.RUnlock()
		return ErrCommitterClosed
	}

	task.done = make(chan struct{}, 1)
	gc.queue <- task
	gc.mu.RUnlock()

	<-task.done
	return task.err
}

// workerLoop is the dedicated goroutine that collects and flushes batches of writes.
func (gc *GroupCommitter) workerLoop() {
	defer gc.wg.Done()

	for task := range gc.queue {
		batch := make([]*writeTask, 0, gc.maxBatchSize)
		batch = append(batch, task)
		batchOps := len(task.entries)
		batchBytes := task.totalBytes()

		// Greedily drain any other tasks currently waiting in queue up to maxBatchSize or maxBatchBytes
	drain:
		for batchOps < gc.maxBatchSize && batchBytes < gc.maxBatchBytes {
			select {
			case nextTask, ok := <-gc.queue:
				if !ok {
					break drain
				}
				batch = append(batch, nextTask)
				batchOps += len(nextTask.entries)
				batchBytes += nextTask.totalBytes()
			default:
				break drain
			}
		}

		gc.flushBatch(batch)
	}
}

// flushBatch writes all tasks in the batch to disk in ONE disk write,
// updates the in-memory index, and notifies all waiting goroutines.
func (gc *GroupCommitter) flushBatch(batch []*writeTask) {
	if len(batch) == 0 {
		return
	}

	// 1. Calculate total payload size across all tasks outside the DB lock
	totalBytes := 0
	for _, task := range batch {
		totalBytes += task.totalBytes()
	}

	if totalBytes == 0 {
		for _, task := range batch {
			close(task.done)
		}
		return
	}

	// 2. Assemble contiguous write buffer outside the DB lock
	buffer := make([]byte, 0, totalBytes)
	for _, task := range batch {
		for _, entry := range task.entries {
			buffer = append(buffer, entry.encoded...)
		}
	}

	// 3. Acquire database lock to serialize disk append and index updates
	gc.db.mu.Lock()
	if gc.db.isClosed {
		gc.db.mu.Unlock()
		for _, task := range batch {
			task.err = ErrDatabaseClosed
			close(task.done)
		}
		return
	}

	// 4. Single sequential write to active data file (blind append)
	fileID, startOffset, err := gc.db.storage.Write(buffer)

	// 5. If SyncOnWrite is configured, issue ONE physical sync for the entire batch
	if err == nil && gc.db.options.SyncOnWrite {
		err = gc.db.storage.Sync()
	}

	// 6. Update index for each entry while holding db.mu
	if err == nil {
		currOffset := startOffset
		for _, task := range batch {
			for _, entry := range task.entries {
				if entry.isDelete {
					gc.db.index.Delete(string(entry.key))
				} else {
					gc.db.index.Put(string(entry.key), index.Item{
						FileID:       fileID,
						RecordOffset: currOffset,
						RecordSize:   uint32(len(entry.encoded)),
						Timestamp:    entry.timestamp,
					})
				}
				currOffset += int64(len(entry.encoded))
			}
		}
	}
	gc.db.mu.Unlock()

	if err == nil {
		if gc.hasLastFileID && fileID != gc.lastFileID {
			gc.db.notifyCompaction()
		}
		gc.lastFileID = fileID
		gc.hasLastFileID = true
	}

	// 7. Unblock callers outside the database lock so they can proceed immediately
	for _, task := range batch {
		task.err = err
		close(task.done)
	}
}

// Close gracefully stops the committer and waits for pending writes to finish.
func (gc *GroupCommitter) Close() error {
	gc.closeOnce.Do(func() {
		gc.mu.Lock()
		gc.isClosed = true
		close(gc.queue)
		gc.mu.Unlock()
	})
	gc.wg.Wait()
	return nil
}
