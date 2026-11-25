package bigcat

import "bytes"

// batchOp represents a single staged mutation inside a WriteBatch.
type batchOp struct {
	key      []byte
	value    []byte
	isDelete bool
}

// WriteBatch stages multiple Put and Delete operations in memory so they can be
// committed together atomically via db.WriteBatch(batch).
//
// Repeated operations on the same key inside the same batch are deduplicated
// (last-write-wins) before hitting disk.
type WriteBatch struct {
	ops   []batchOp
	index map[string]int
}

// NewWriteBatch creates an empty WriteBatch ready to stage mutations.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		index: make(map[string]int),
		ops:   make([]batchOp, 0, 10),
	}
}

// Put stages a key-value write in the batch.
// Keys and values are copied defensively so caller buffer reuse does not mutate staged entries.
func (wb *WriteBatch) Put(key, value []byte) error {
	return wb.stage(key, value, false)
}

// Delete stages a key deletion in the batch.
func (wb *WriteBatch) Delete(key []byte) error {
	return wb.stage(key, nil, true)
}

func (wb *WriteBatch) stage(key, value []byte, isDelete bool) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	var valCopy []byte
	if !isDelete {
		valCopy = bytes.Clone(value)
	}

	if idx, exists := wb.index[string(key)]; exists {
		wb.ops[idx].isDelete = isDelete
		wb.ops[idx].value = valCopy
		return nil
	}

	keyCopy := bytes.Clone(key)
	wb.index[string(keyCopy)] = len(wb.ops)
	wb.ops = append(wb.ops, batchOp{
		key:      keyCopy,
		value:    valCopy,
		isDelete: isDelete,
	})
	return nil
}

// Size returns the number of unique pending operations currently in the batch.
func (wb *WriteBatch) Size() int {
	return len(wb.ops)
}

// Clear resets the batch so it can be reused without reallocating the backing map.
func (wb *WriteBatch) Clear() {
	wb.ops = wb.ops[:0]
	clear(wb.index)
}
