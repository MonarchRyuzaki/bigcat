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
	// DefaultMaxBatchSize is the maximum number of writes to coalesce into a single disk write.
	DefaultMaxBatchSize = 128

	// DefaultMaxBatchBytes is the maximum total payload size (4MB) to coalesce into a single disk write.
	DefaultMaxBatchBytes = 4 * 1024 * 1024

	// DefaultBatchQueueCapacity is the buffer capacity for the incoming task channel.
	DefaultBatchQueueCapacity = 1024
)

// writeTask represents a single write (Put or Delete) queued for group commitment.
type writeTask struct {
	key       []byte
	encoded   []byte
	isDelete  bool
	timestamp uint64
	err       error
	done      chan struct{}
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
		batchBytes := len(task.encoded)

		// Greedily drain any other tasks currently waiting in queue up to maxBatchSize or maxBatchBytes
	drain:
		for len(batch) < gc.maxBatchSize && batchBytes < gc.maxBatchBytes {
			select {
			case nextTask, ok := <-gc.queue:
				if !ok {
					break drain
				}
				batch = append(batch, nextTask)
				batchBytes += len(nextTask.encoded)
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

	// 1. Calculate total length of all encoded payloads in the batch
	totalBytes := 0
	for _, item := range batch {
		totalBytes += len(item.encoded)
	}

	// 2. Pre-allocate combined buffer and coalesce all encoded records
	buffer := make([]byte, 0, totalBytes)
	for _, item := range batch {
		buffer = append(buffer, item.encoded...)
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

	// 4. Single sequential write to active data file
	fileID, startOffset, err := gc.db.storage.Write(buffer)

	// 5. If SyncOnWrite is configured, issue ONE physical sync for the entire batch
	if err == nil && gc.db.options.SyncOnWrite {
		err = gc.db.storage.Sync()
	}

	// 6. Update index for each task while holding db.mu
	if err == nil {
		currOffset := startOffset
		for _, task := range batch {
			if task.isDelete {
				gc.db.index.Delete(string(task.key))
			} else {
				gc.db.index.Put(string(task.key), index.Item{
					FileID:       fileID,
					RecordOffset: currOffset,
					RecordSize:   uint32(len(task.encoded)),
					Timestamp:    task.timestamp,
				})
			}
			currOffset += int64(len(task.encoded))
		}
	}
	gc.db.mu.Unlock()

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
