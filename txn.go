// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

var (
	errNoKey = errors.New("column: collection does not have a key column")
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
	txn.logger = owner.logger
	txn.setup = false
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
	cursor  uint32           // The current cursor
	setup   bool             // Whether the transaction was set up or not
	owner   *Collection      // The target collection
	index   bitmap.Bitmap	 // The filtering index
	dirty   bitmap.Bitmap	 // The dirty chunks
	updates []*commit.Buffer // The update buffers
	columns []columnCache    // The column mapping
	logger  commit.Logger    // The optional commit logger
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

// bufferFor loads or creates a buffer for a given column.
func (txn *Txn) bufferFor(columnName string) *commit.Buffer {
	for _, c := range txn.updates {
		if c.Column == columnName {
			return c
		}
	}

	// Create a new buffer
	buffer := txn.owner.txns.acquirePage(columnName)
	txn.updates = append(txn.updates, buffer)
	return buffer
}

// columnCache caches a column by its name. This speeds things up since it's a very
// common operation.
type columnCache struct {
	name string  // The column name
	col  *column // The loaded column
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
	txn.initialize()
	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.rangeReadPair(idx, func(dst, src bitmap.Bitmap) {
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
	txn.initialize()
	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.rangeReadPair(idx, func(dst, src bitmap.Bitmap) {
				dst.AndNot(src)
			})
		}
	}
	return txn
}

// Union computes a union between the current query and the specified index.
func (txn *Txn) Union(columns ...string) *Txn {
	first := !txn.setup
	txn.initialize()
	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.rangeReadPair(idx, func(dst, src bitmap.Bitmap) {
				if first {
					dst.And(src)
				} else {
					dst.Or(src)
				}
			})
		}
		first = false
	}
	return txn
}

// WithValue applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (txn *Txn) WithValue(column string, predicate func(v interface{}) bool) *Txn {
	txn.initialize()
	c, ok := txn.columnAt(column)
	if !ok {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		offset := chunk.Min()
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
	txn.initialize()
	c, ok := txn.columnAt(column)
	if !ok || !c.IsNumeric() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		c.Column.(Numeric).FilterFloat64(chunk, index, predicate)
	})
	return txn
}

// WithInt filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to int64.
func (txn *Txn) WithInt(column string, predicate func(v int64) bool) *Txn {
	txn.initialize()
	c, ok := txn.columnAt(column)
	if !ok || !c.IsNumeric() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		c.Column.(Numeric).FilterInt64(chunk, index, predicate)
	})
	return txn
}

// WithUint filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to uint64.
func (txn *Txn) WithUint(column string, predicate func(v uint64) bool) *Txn {
	txn.initialize()
	c, ok := txn.columnAt(column)
	if !ok || !c.IsNumeric() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		c.Column.(Numeric).FilterUint64(chunk, index, predicate)
	})
	return txn
}

// WithString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (txn *Txn) WithString(column string, predicate func(v string) bool) *Txn {
	txn.initialize()
	c, ok := txn.columnAt(column)
	if !ok || !c.IsTextual() {
		txn.index.Clear()
		return txn
	}

	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		c.Column.(Textual).FilterString(chunk, index, predicate)
	})
	return txn
}

// Count returns the number of objects matching the query
func (txn *Txn) Count() int {
	txn.initialize()
	return int(txn.index.Count())
}

// QueryKey jumps at a particular key in the collection, sets the cursor to the
// provided position and executes given callback fn.
func (txn *Txn) QueryKey(key string, fn func(Row) error) error {
	if txn.owner.pk == nil {
		return errNoKey
	}

	if idx, ok := txn.owner.pk.OffsetOf(key); ok {
		return txn.QueryAt(idx, fn)
	}

	// If not found, insert at a new index
	idx, err := txn.insert(fn, 0)
	txn.bufferFor(txn.owner.pk.name).PutString(commit.Put, idx, key)
	return err
}

// DeleteAt attempts to delete an item at the specified index for this transaction. If the item
// exists, it marks at as deleted and returns true, otherwise it returns false.
func (txn *Txn) DeleteAt(index uint32) bool {
	txn.initialize()
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

// InsertObject adds an object to a collection and returns the allocated index.
func (txn *Txn) InsertObject(object Object) (uint32, error) {
	return txn.insertObject(object, 0)
}

// InsertObjectWithTTL adds an object to a collection, sets the expiration time
// based on the specified time-to-live and returns the allocated index.
func (txn *Txn) InsertObjectWithTTL(object Object, ttl time.Duration) (uint32, error) {
	return txn.insertObject(object, time.Now().Add(ttl).UnixNano())
}

// Insert executes a mutable cursor transactionally at a new offset.
func (txn *Txn) Insert(fn func(Row) error) (uint32, error) {
	return txn.insert(fn, 0)
}

// InsertWithTTL executes a mutable cursor transactionally at a new offset and sets the expiration time
// based on the specified time-to-live and returns the allocated index.
func (txn *Txn) InsertWithTTL(ttl time.Duration, fn func(Row) error) (uint32, error) {
	return txn.insert(fn, time.Now().Add(ttl).UnixNano())
}

// insertObject inserts all of the keys of a map, if previously registered as columns.
func (txn *Txn) insertObject(object Object, expireAt int64) (uint32, error) {
	return txn.insert(func(Row) error {
		for k, v := range object {
			if _, ok := txn.columnAt(k); ok {
				txn.bufferFor(k).PutAny(commit.Put, txn.cursor, v)
			}
		}
		return nil
	}, expireAt)
}

// insert creates an insertion cursor for a given column and expiration time.
func (txn *Txn) insert(fn func(Row) error, expireAt int64) (uint32, error) {

	// At a new index, add the insertion marker
	idx := txn.owner.next()
	txn.bufferFor(rowColumn).PutOperation(commit.Insert, idx)

	// If no expiration was specified, simply insert
	if expireAt == 0 {
		return idx, txn.QueryAt(idx, fn)
	}

	// If expiration was specified, set it
	return idx, txn.QueryAt(idx, func(r Row) error {
		r.SetInt64(expireColumn, expireAt)
		return fn(r)
	})
}

// DeleteAll marks all of the items currently selected by this transaction for deletion. The
// actual delete will take place once the transaction is committed.
func (txn *Txn) DeleteAll() {
	txn.initialize()
	txn.index.Range(func(x uint32) {
		txn.deleteAt(x)
	})
}

// Range selects and iterates over result set. In each iteration step, the internal
// transaction cursor is updated and can be used by various column accessors.
func (txn *Txn) Range(fn func(idx uint32)) error {
	txn.initialize()
	txn.rangeRead(func(chunk commit.Chunk, index bitmap.Bitmap) {
		offset := chunk.Min()
		index.Range(func(x uint32) {
			txn.cursor = offset + x
			fn(offset + x)
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
		u.RangeChunks(func(chunk commit.Chunk) {
			txn.dirty.Set(uint32(chunk))
		})
	}

	// Grow the size of the fill list
	markers, changedRows := txn.findMarkers()
	if last, ok := txn.dirty.Max(); ok {
		txn.commitCapacity(commit.Chunk(last))
	}

	// Commit chunk by chunk to reduce lock contentions
	txn.rangeWrite(func(commitID uint64, chunk commit.Chunk, fill bitmap.Bitmap) {
		if changedRows {
			txn.commitMarkers(chunk, fill, markers)
		}

		// Attemp to update, if nothing was changed we're done
		updated := txn.commitUpdates(chunk)
		if !changedRows && !updated {
			return
		}

		// If there is a pending snapshot, append commit into a temp log
		if dst, ok := txn.owner.isSnapshotting(); ok {
			dst.Append(commit.Commit{
				ID:      commitID,
				Chunk:   chunk,
				Updates: txn.updates,
			})
		}

		if txn.logger != nil {
			txn.logger.Append(commit.Commit{
				ID:      commitID,
				Chunk:   chunk,
				Updates: txn.updates,
			})
		}
	})
}

// commitUpdates applies the pending updates to the collection.
func (txn *Txn) commitUpdates(chunk commit.Chunk) (updated bool) {
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
				v.Apply(chunk, r)
			}
		})
	}
	return updated
}

// commitMarkers commits inserts and deletes to the collection.
func (txn *Txn) commitMarkers(chunk commit.Chunk, fill bitmap.Bitmap, buffer *commit.Buffer) {
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
			column.Apply(chunk, r)
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
func (txn *Txn) commitCapacity(last commit.Chunk) {
	txn.owner.lock.Lock()
	defer txn.owner.lock.Unlock()
	if len(txn.owner.commits) >= int(last+1) {
		return
	}

	// Grow the commits array
	for len(txn.owner.commits) < int(last+1) {
		txn.owner.commits = append(txn.owner.commits, 0)
	}

	// Grow the fill list and all of the owner's columns
	max := last.Max()
	txn.owner.fill.Grow(max)
	txn.owner.cols.Range(func(column *column) {
		column.Grow(max)
	})
}
