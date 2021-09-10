// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Pool of Transactions ----------------------------

// txnPool is a pool of transactions which are retained for the lifetime of the process.
type txnPool struct {
	txns  sync.Pool
	pages sync.Pool
}

func newTxnPool() *txnPool {
	return &txnPool{
		txns: sync.Pool{
			New: func() interface{} {
				return &Txn{
					index:   make(bitmap.Bitmap, 0, 4),
					dirty:   make(bitmap.Bitmap, 0, 4),
					updates: make([]*commit.Buffer, 0, 256),
					columns: make([]columnCache, 0, 16),
					reader:  commit.NewReader(),
				}
			},
		},
		pages: sync.Pool{
			New: func() interface{} {
				return commit.NewBuffer(chunkSize)
			},
		},
	}
}

// acquire acquires a new transaction from the pool
func (p *txnPool) acquire(owner *Collection) *Txn {
	txn := p.txns.Get().(*Txn)
	txn.owner = owner
	txn.writer = owner.writer
	txn.index.Grow(uint32(owner.size))
	owner.fill.Clone(&txn.index)
	return txn
}

// release the transaction to the pool or the GC
func (p *txnPool) release(txn *Txn) {
	p.txns.Put(txn)
}

// acquirePage acquires a new page for a particular column and initializes it
func (p *txnPool) acquirePage(columnName string) *commit.Buffer {
	page := p.pages.Get().(*commit.Buffer)
	page.Reset(columnName)
	return page
}

// releasePage releases the buffer back
func (p *txnPool) releasePage(buffer *commit.Buffer) {
	buffer.Reset("")
	p.pages.Put(buffer)
}

// --------------------------- Transaction ----------------------------

// Txn represents a transaction which supports filtering and projection.
type Txn struct {
	owner   *Collection      // The target collection
	index   bitmap.Bitmap    // The filtering index
	dirty   bitmap.Bitmap    // The dirty chunks
	updates []*commit.Buffer // The update buffers
	columns []columnCache    // The column mapping
	writer  commit.Writer    // The optional commit writer
	reader  *commit.Reader   // The commit reader to re-use
}

// Reset resets the transaction state so it can be used again.
func (txn *Txn) reset() {
	for i := range txn.updates {
		txn.owner.txns.releasePage(txn.updates[i])
	}

	txn.dirty.Clear()
	txn.reader.Rewind()
	txn.columns = txn.columns[:0]
	txn.updates = txn.updates[:0]
}

// columnCache caches a column by its name. This speeds things up since it's a very
// common operation.
type columnCache struct {
	name string  // The column name
	col  *column // The columns and its computed
}

// columnAt loads and caches the column for the transaction
func (txn *Txn) columnAt(columnName string) (*column, bool) {
	for _, v := range txn.columns {
		if v.name == columnName {
			return v.col, true
		}
	}

	// Load the column from the owner
	column, ok := txn.owner.cols.Load(columnName)
	if !ok {
		return nil, false
	}

	// Cache the loaded column for this transaction
	txn.columns = append(txn.columns, columnCache{
		name: columnName,
		col:  column,
	})
	return column, true
}

// With applies a logical AND operation to the current query and the specified index.
func (txn *Txn) With(columns ...string) *Txn {
	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.rangeReadPair(*idx.Column.Index(), func(dst, src bitmap.Bitmap) {
				dst.And(src)
			})
		} else {
			txn.index.Clear()
		}
	}
	return txn
}

// Without applies a logical AND NOT operation to the current query and the specified index.
func (txn *Txn) Without(columns ...string) *Txn {
	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.rangeReadPair(*idx.Column.Index(), func(dst, src bitmap.Bitmap) {
				dst.AndNot(src)
			})
		}
	}
	return txn
}

// Union computes a union between the current query and the specified index.
func (txn *Txn) Union(columns ...string) *Txn {
	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.rangeReadPair(*idx.Column.Index(), func(dst, src bitmap.Bitmap) {
				dst.Or(src)
			})
		}
	}
	return txn
}

// WithValue applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (txn *Txn) WithValue(column string, predicate func(v interface{}) bool) *Txn {
	c, ok := txn.columnAt(column)
	if !ok {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		index.Filter(func(x uint32) (match bool) {
			if v, ok := c.Value(offset + x); ok {
				match = predicate(v)
			}
			return
		})
	})
	return txn
}

// WithFloat filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to float64.
func (txn *Txn) WithFloat(column string, predicate func(v float64) bool) *Txn {
	c, ok := txn.columnAt(column)
	if !ok || !c.IsNumeric() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		c.Column.(Numeric).FilterFloat64(offset, index, predicate)
	})
	return txn
}

// WithInt filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to int64.
func (txn *Txn) WithInt(column string, predicate func(v int64) bool) *Txn {
	c, ok := txn.columnAt(column)
	if !ok || !c.IsNumeric() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		c.Column.(Numeric).FilterInt64(offset, index, predicate)
	})
	return txn
}

// WithUint filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to uint64.
func (txn *Txn) WithUint(column string, predicate func(v uint64) bool) *Txn {
	c, ok := txn.columnAt(column)
	if !ok || !c.IsNumeric() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		c.Column.(Numeric).FilterUint64(offset, index, predicate)
	})
	return txn
}

// WithString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (txn *Txn) WithString(column string, predicate func(v string) bool) *Txn {
	c, ok := txn.columnAt(column)
	if !ok || !c.IsTextual() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		c.Column.(Textual).FilterString(offset, index, predicate)
	})
	return txn
}

// Count returns the number of objects matching the query
func (txn *Txn) Count() int {
	return int(txn.index.Count())
}

// UpdateAt creates a cursor to a specific element that can be read or updated.
func (txn *Txn) UpdateAt(index uint32, columnName string, fn func(v Cursor) error) error {
	cursor, err := txn.cursorFor(columnName)
	if err != nil {
		return err
	}

	cursor.idx = index
	return fn(cursor)
}

// ReadAt returns a selector for a specified index together with a boolean value that indicates
// whether an element is present at the specified index or not.
func (txn *Txn) ReadAt(index uint32) (Selector, bool) {
	if !txn.index.Contains(index) {
		return Selector{}, false
	}

	return Selector{
		idx: index,
		txn: txn,
	}, true
}

// DeleteAt attempts to delete an item at the specified index for this transaction. If the item
// exists, it marks at as deleted and returns true, otherwise it returns false.
func (txn *Txn) DeleteAt(index uint32) bool {
	if !txn.index.Contains(index) {
		return false
	}

	txn.deleteAt(index)
	return true
}

// deleteAt marks an index as deleted
func (txn *Txn) deleteAt(idx uint32) {
	txn.bufferFor(rowColumn).PutOperation(commit.Delete, idx)
}

// Insert inserts an object at a new index and returns the index for this object. This is
// done transactionally and the object will only be visible after the transaction is committed.
func (txn *Txn) Insert(object Object) uint32 {
	return txn.insert(object, 0)
}

// InsertWithTTL inserts an object at a new index and returns the index for this object. In
// addition, it also sets the time-to-live of an object to the specified time. This is done
// transactionally and the object will only be visible after the transaction is committed.
func (txn *Txn) InsertWithTTL(object Object, ttl time.Duration) uint32 {
	return txn.insert(object, time.Now().Add(ttl).UnixNano())
}

// Insert inserts an object at a new index and returns the index for this object. This is
// done transactionally and the object will only be visible after the transaction is committed.
func (txn *Txn) insert(object Object, expireAt int64) uint32 {
	slot := Cursor{
		Selector: Selector{
			idx: txn.owner.next(),
			txn: txn,
		},
	}

	// Set the insert bit and generate the updates
	txn.bufferFor(rowColumn).PutOperation(commit.Insert, slot.idx)
	for k, v := range object {
		if _, ok := txn.columnAt(k); ok {
			slot.SetAt(k, v)
		}
	}

	// Add expiration if specified
	if expireAt != 0 {
		slot.SetAt(expireColumn, expireAt)
	}
	return slot.idx
}

// Select iterates over the result set and allows to read any column. While this
// is flexible, it is not the most efficient way, consider Range() as an alternative
// iteration method over a specific column which also supports modification.
func (txn *Txn) Select(fn func(v Selector)) {
	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		index.Range(func(x uint32) {
			fn(Selector{
				idx: offset + x,
				txn: txn,
			})
		})
	})
}

// DeleteIf iterates over the result set and calls the provided funciton on each element. If
// the function returns true, the element at the index will be marked for deletion. The
// actual delete will take place once the transaction is committed.
func (txn *Txn) DeleteIf(fn func(v Selector) bool) {
	txn.index.Range(func(x uint32) {
		if fn(Selector{idx: x, txn: txn}) {
			txn.deleteAt(x)
		}
	})
}

// DeleteAll marks all of the items currently selected by this transaction for deletion. The
// actual delete will take place once the transaction is committed.
func (txn *Txn) DeleteAll() {
	txn.index.Range(func(x uint32) {
		txn.deleteAt(x)
	})
}

// Range selects and iterates over a results for a specific column. The cursor provided
// also allows to select other columns, but at a slight performance cost.
func (txn *Txn) Range(column string, fn func(v Cursor)) error {
	cur, err := txn.cursorFor(column)
	if err != nil {
		return err
	}

	txn.rangeRead(func(offset uint32, index bitmap.Bitmap) {
		index.Range(func(x uint32) {
			cur.idx = offset + x
			fn(cur)
		})
	})
	return nil
}

// Rollback empties the pending update and delete queues and does not apply any of
// the pending updates/deletes. This operation can be called several times for
// a transaction in order to perform partial rollbacks.
func (txn *Txn) rollback() {
	txn.reset()
}

// Commit commits the transaction by applying all pending updates and deletes to
// the collection. This operation is can be called several times for a transaction
// in order to perform partial commits. If there's no pending updates/deletes, this
// operation will result in a no-op.
func (txn *Txn) commit() {
	defer txn.reset()

	// Mark the dirty chunks from the updates
	for _, u := range txn.updates {
		u.RangeChunks(func(chunk uint32) {
			txn.dirty.Set(chunk)
		})
	}

	// Get the upper bound chunk
	lastChunk, _ := txn.dirty.Max()

	// Grow the size of the fill list
	markers, changedRows := txn.findMarkers()
	max := txn.reader.MaxOffset(markers, lastChunk)
	if max > 0 {
		txn.commitCapacity(max)
	}

	// Commit chunk by chunk to reduce lock contentions
	txn.rangeWrite(func(chunk uint32, fill bitmap.Bitmap) {
		if changedRows {
			txn.commitMarkers(chunk, fill, markers)
		}

		updated := txn.commitUpdates(chunk, fill)

		// Write the commited chunk to the writer (if any)
		if (changedRows || updated) && txn.writer != nil {
			txn.writer.Write(commit.Commit{
				Chunk:   chunk,
				Updates: txn.updates,
			})
		}
	})
}

// commitUpdates applies the pending updates to the collection.
func (txn *Txn) commitUpdates(chunk uint32, fill bitmap.Bitmap) (updated bool) {
	for _, u := range txn.updates {
		if u.IsEmpty() || u.Column == rowColumn {
			continue // No updates for this column
		}

		// Get the column to update
		columns, exists := txn.owner.cols.LoadWithIndex(u.Column)
		if !exists || len(columns) == 0 {
			continue
		}

		// Do a linear search to find the offset for the current chunk
		updated = true
		txn.reader.Range(u, chunk, func(r *commit.Reader) {

			// Range through all of the pending updates and apply them to the column
			// and its associated computed columns.
			for _, v := range columns {
				v.Apply(r)
			}
		})
	}
	return updated
}

// commitMarkers commits inserts and deletes to the collection.
func (txn *Txn) commitMarkers(chunk uint32, fill bitmap.Bitmap, buffer *commit.Buffer) {
	txn.reader.Range(buffer, chunk, func(r *commit.Reader) {
		for r.Next() {
			txn.owner.lock.Lock()
			switch r.Type {
			case commit.Insert:
				txn.owner.fill.Set(r.Index())
			case commit.Delete:
				txn.owner.fill.Remove(r.Index())
			}
			txn.owner.lock.Unlock()
		}
	})

	// We also need to apply the delete operations on the column so it
	// can remove unnecessary data.
	txn.reader.Range(buffer, chunk, func(r *commit.Reader) {
		txn.owner.cols.Range(func(column *column) {
			column.Apply(r)
		})
	})

	txn.owner.lock.Lock()
	atomic.StoreUint64(&txn.owner.count, uint64(txn.owner.fill.Count()))
	txn.owner.lock.Unlock()
}

// findMarkers finds a set of insert/deletes
func (txn *Txn) findMarkers() (*commit.Buffer, bool) {
	for _, u := range txn.updates {
		if !u.IsEmpty() && u.Column == rowColumn {
			return u, true
		}
	}
	return nil, false
}

// commitCapacity grows all columns until they reach the max index
func (txn *Txn) commitCapacity(max uint32) {
	txn.owner.lock.Lock()
	defer txn.owner.lock.Unlock()

	// Grow the fill list and all of the owner's columns
	txn.owner.fill.Grow(max)
	txn.owner.cols.Range(func(column *column) {
		column.Grow(max)
	})
}
