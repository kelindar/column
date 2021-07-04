// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"sync/atomic"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Pool of Transactions ----------------------------

// txnPool is a pool of transactions which are retained for the lifetime of the process.
type txnPool struct {
	txns  chan *Txn
	pages chan commit.Updates
}

func newTxnPool() *txnPool {
	return &txnPool{
		txns:  make(chan *Txn, 256),            // Max transactions pooled
		pages: make(chan commit.Updates, 1024), // Max scratch pages pooled
	}
}

func (p *txnPool) acquire(owner *Collection) (txn *Txn) {
	select {
	case txn = <-p.txns:
	default:
		txn = &Txn{
			index:   make(bitmap.Bitmap, 0, 4),
			deletes: make(bitmap.Bitmap, 0, 4),
			inserts: make(bitmap.Bitmap, 0, 4),
			dirty:   make(bitmap.Bitmap, 0, 4),
			updates: make([]commit.Updates, 0, 256),
			columns: make([]columnCache, 0, 16),
		}
	}

	// Initialize
	txn.owner = owner
	txn.columns = txn.columns[:0]
	txn.writer = owner.writer
	owner.fill.Clone(&txn.index)
	return
}

// acquirePage acquires a new page for a particular column and initializes it
func (p *txnPool) acquirePage(columnName string) (page commit.Updates) {
	select {
	case page = <-p.pages:
	default:
		page = commit.Updates{}
	}

	// Initialize
	page.Column = columnName
	page.Current = -1
	page.Update = page.Update[:0]
	page.Offsets = page.Offsets[:0]
	return
}

func (p *txnPool) release(txn *Txn) {
	for i := range txn.updates {
		select {
		case p.pages <- txn.updates[i]:
		default:
		}
	}

	// Release the transaction to the pool or the GC
	txn.updates = txn.updates[:0]
	select {
	case p.txns <- txn:
	default:
	}
}

// --------------------------- Transaction ----------------------------

// Txn represents a transaction which supports filtering and projection.
type Txn struct {
	owner   *Collection      // The target collection
	index   bitmap.Bitmap    // The filtering index
	deletes bitmap.Bitmap    // The delete queue
	inserts bitmap.Bitmap    // The insert queue
	dirty   bitmap.Bitmap    // The dirty chunks
	updates []commit.Updates // The update queue
	columns []columnCache    // The column mapping
	writer  commit.Writer    // The optional commit writer
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
			txn.rlockEachPair(*idx.Column.Index(), func(dst, src bitmap.Bitmap) {
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
			txn.rlockEachPair(*idx.Column.Index(), func(dst, src bitmap.Bitmap) {
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
			txn.rlockEachPair(*idx.Column.Index(), func(dst, src bitmap.Bitmap) {
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

	txn.rlockEach(func(_ uint32, index bitmap.Bitmap) {
		index.Filter(func(x uint32) (match bool) {
			if v, ok := c.Value(x); ok {
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

	/* ESCAPES
	.\txn.go:157:31: index escapes to heap:
	.\txn.go:172:31: index escapes to heap:
	.\txn.go:187:31: index escapes to heap:
	.\txn.go:202:31: index escapes to heap:
	*/
	txn.rlockEach(func(offset uint32, index bitmap.Bitmap) {
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

	txn.rlockEach(func(offset uint32, index bitmap.Bitmap) {
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

	txn.rlockEach(func(offset uint32, index bitmap.Bitmap) {
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

	txn.rlockEach(func(offset uint32, index bitmap.Bitmap) {
		c.Column.(Textual).FilterString(offset, index, predicate)
	})
	return txn
}

// Count returns the number of objects matching the query
func (txn *Txn) Count() int {
	return int(txn.index.Count())
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

	txn.deletes.Set(index)
	txn.dirty.Set(index >> chunkShift)
	return true
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
	txn.inserts.Set(slot.idx)
	txn.dirty.Set(slot.idx >> chunkShift)
	for k, v := range object {
		if _, ok := txn.columnAt(k); ok {
			slot.UpdateAt(k, v)
		}
	}

	// Add expiration if specified
	if expireAt != 0 {
		slot.UpdateAt(expireColumn, expireAt)
	}
	return slot.idx
}

// Select iterates over the result set and allows to read any column. While this
// is flexible, it is not the most efficient way, consider Range() as an alternative
// iteration method over a specific column which also supports modification.
func (txn *Txn) Select(fn func(v Selector)) {
	txn.rlockEach(func(offset uint32, index bitmap.Bitmap) {
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
			txn.deletes.Set(x)
			txn.dirty.Set(x >> chunkShift)
		}
	})
}

// DeleteAll marks all of the items currently selected by this transaction for deletion. The
// actual delete will take place once the transaction is committed.
func (txn *Txn) DeleteAll() {
	txn.deletes.Or(txn.index)

	// TODO: optimize this
	txn.deletes.Range(func(x uint32) {
		txn.dirty.Set(x >> chunkShift)
	})
}

// Range selects and iterates over a results for a specific column. The cursor provided
// also allows to select other columns, but at a slight performance cost.
func (txn *Txn) Range(column string, fn func(v Cursor)) error {
	cur, err := txn.cursorFor(column)
	if err != nil {
		return err
	}

	txn.rlockEach(func(offset uint32, index bitmap.Bitmap) {
		index.Range(func(x uint32) {
			cur.idx = offset + x
			fn(cur)
		})
	})
	return nil
}

// Reset resets the transaction state so it can be used again.
func (txn *Txn) reset() {
	for i := range txn.updates {
		txn.updates[i].Current = -1
		txn.updates[i].Update = txn.updates[i].Update[:0]
		txn.updates[i].Offsets = txn.updates[i].Offsets[:0]
	}

	txn.dirty.Clear()
	txn.deletes.Clear()
	txn.inserts.Clear()
}

// Rollback empties the pending update and delete queues and does not apply any of
// the pending updates/deletes. This operation can be called several times for
// a transaction in order to perform partial rollbacks.
func (txn *Txn) rollback() {
	txn.reset()
}

func (txn *Txn) release() {
	txn.owner.txns.release(txn)
}

// Commit commits the transaction by applying all pending updates and deletes to
// the collection. This operation is can be called several times for a transaction
// in order to perform partial commits. If there's no pending updates/deletes, this
// operation will result in a no-op.
func (txn *Txn) commit() {
	defer txn.reset()

	// Grow the size of the fill list
	max, _ := txn.inserts.Max()
	txn.owner.lock.Lock()
	txn.owner.fill.Grow(max)
	txn.owner.lock.Unlock()

	// Commit chunk by chunk to reduce lock contentions
	var typ commit.Type
	txn.commitEach(func(chunk uint32, fill bitmap.Bitmap) {
		typ |= txn.commitDeletes(chunk, fill)
		typ |= txn.commitInserts(chunk, fill)
		typ |= txn.commitUpdates(chunk, max)

		// TODO: stream commits in chunks to keep consistency ?
		// need to test with MULTIPLE chunks (large collection)

		if typ > 0 && txn.writer != nil {
			txn.writer.Write(commit.Commit{
				Type:    typ,
				Dirty:   txn.dirty,
				Inserts: txn.inserts,
				Deletes: txn.deletes,
				Updates: txn.updates,
			})
		}
	})

	// If there's a writer, write into it before we clean up the transaction
	/*if typ > 0 && txn.writer != nil {
		txn.writer.Write(commit.Commit{
			Type:    typ,
			Dirty:   txn.dirty,
			Inserts: txn.inserts,
			Deletes: txn.deletes,
			Updates: txn.updates,
		})
	}*/
}

// commitUpdates applies the pending updates to the collection.
func (txn *Txn) commitUpdates(chunk, max uint32) (typ commit.Type) {
	for _, u := range txn.updates {
		if len(u.Update) == 0 {
			continue // No updates for this column
		}

		// Get the column to update
		columns, exists := txn.owner.cols.LoadWithIndex(u.Column)
		if !exists || len(columns) == 0 {
			continue
		}

		// Do a linear search to find the offset for the current chunk
		typ |= commit.Store
		for i, offset := range u.Offsets {
			if (u.Update[offset].Index >> chunkShift) != chunk {
				continue // Not the right chunk (TODO: optimize)
			}

			// Find the next offset
			chunkUpdates := u.Update[offset:]
			if len(u.Offsets) > i+1 {
				until := u.Offsets[i+1]
				chunkUpdates = u.Update[offset:until]
			}

			// Range through all of the pending updates and apply them to the column
			// and its associated computed columns.
			for _, v := range columns {
				v.Update(chunkUpdates, max)
			}
		}
	}
	return
}

// commitDeletes removes all of the entries marked as to be deleted
func (txn *Txn) commitDeletes(chunk uint32, fill bitmap.Bitmap) commit.Type {
	at := int(chunk << (chunkShift - 6))
	if len(txn.deletes) <= at {
		return 0 // Nothing to delete
	}

	// Apply a batch delete on all of the columns
	deletes := txn.deletes[at:]
	if len(deletes) > at+len(fill) {
		deletes = txn.deletes[at : at+len(fill)]
	}

	if deletes.Count() == 0 {
		return 0
	}

	txn.owner.cols.Range(func(column *column) {
		column.Delete(at, deletes)
	})

	// Clear the items in the collection and reinitialize the purge list
	//txn.owner.lock.Lock()
	fill.AndNot(deletes)
	atomic.StoreUint64(&txn.owner.count, uint64(txn.owner.fill.Count()))
	//txn.owner.lock.Unlock()
	return commit.Delete
}

// commitInserts inserts all of the entries marked as to be inserted. This just makes them
// visible by setting the fill list atomically in the collection.
func (txn *Txn) commitInserts(chunk uint32, fill bitmap.Bitmap) commit.Type {
	at := int(chunk << (chunkShift - 6))
	if len(txn.inserts) <= at {
		return 0
	}

	inserts := txn.inserts[at:]
	if len(inserts) > at+len(fill) {
		inserts = txn.inserts[at : at+len(fill)]
	}

	if inserts.Count() == 0 {
		return 0
	}

	//txn.owner.lock.Lock()
	fill.Or(inserts)
	atomic.StoreUint64(&txn.owner.count, uint64(txn.owner.fill.Count()))
	//txn.owner.lock.Unlock()
	return commit.Insert
}
