// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Pool of Transactions ----------------------------

// txns represents a pool of transactions
var txns = &sync.Pool{
	New: func() interface{} {
		return &Txn{
			index:   make(bitmap.Bitmap, 0, 4),
			deletes: make(bitmap.Bitmap, 0, 4),
			inserts: make(bitmap.Bitmap, 0, 4),
			updates: make([]updateQueue, 0, 256),
			columns: make([]columnCache, 0, 16),
		}
	},
}

// aquireBitmap acquires a transaction for a transaction
func aquireTxn(owner *Collection) *Txn {
	txn := txns.Get().(*Txn)
	txn.owner = owner
	txn.columns = txn.columns[:0]
	txn.writer = owner.writer
	owner.fill.Clone(&txn.index)
	return txn
}

// releaseTxn releases a transaction back to the pool
func releaseTxn(txn *Txn) {
	txns.Put(txn)
}

// --------------------------- Transaction ----------------------------

// Txn represents a transaction which supports filtering and projection.
type Txn struct {
	owner   *Collection   // The target collection
	index   bitmap.Bitmap // The filtering index
	deletes bitmap.Bitmap // The delete queue
	inserts bitmap.Bitmap // The insert queue
	updates []updateQueue // The update queue
	columns []columnCache // The column mapping
	writer  commit.Writer // The optional commit writer
}

// Update queue represents a queue per column that contains the pending updates.
type updateQueue struct {
	name   string          // The column name
	update []commit.Update // The update queue
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
	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()

	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.index.And(*idx.Column.Index())
		} else {
			txn.index.Clear()
		}
	}
	return txn
}

// Without applies a logical AND NOT operation to the current query and the specified index.
func (txn *Txn) Without(columns ...string) *Txn {
	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()

	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.index.AndNot(*idx.Column.Index())
		}
	}
	return txn
}

// Union computes a union between the current query and the specified index.
func (txn *Txn) Union(columns ...string) *Txn {
	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()

	for _, columnName := range columns {
		if idx, ok := txn.columnAt(columnName); ok {
			txn.index.Or(*idx.Column.Index())
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

	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()
	txn.index.Filter(func(x uint32) (match bool) {
		if v, ok := c.Value(x); ok {
			match = predicate(v)
		}
		return
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

	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()
	c.Column.(Numeric).FilterFloat64(&txn.index, predicate)
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

	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()
	c.Column.(Numeric).FilterInt64(&txn.index, predicate)
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

	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()
	c.Column.(Numeric).FilterUint64(&txn.index, predicate)
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

	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()
	c.Column.(Textual).FilterString(&txn.index, predicate)
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
func (txn *Txn) Select(fn func(v Selector) bool) {
	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()

	txn.index.Range(func(x uint32) bool {
		return fn(Selector{
			idx: x,
			txn: txn,
		})
	})
}

// DeleteIf iterates over the result set and calls the provided funciton on each element. If
// the function returns true, the element at the index will be marked for deletion. The
// actual delete will take place once the transaction is committed.
func (txn *Txn) DeleteIf(fn func(v Selector) bool) {
	txn.index.Range(func(x uint32) bool {
		if fn(Selector{idx: x, txn: txn}) {
			txn.deletes.Set(x)
		}
		return true
	})
}

// DeleteAll marks all of the items currently selected by this transaction for deletion. The
// actual delete will take place once the transaction is committed.
func (txn *Txn) DeleteAll() {
	txn.deletes.Or(txn.index)
}

// Range selects and iterates over a results for a specific column. The cursor provided
// also allows to select other columns, but at a slight performance cost. If the range
// function returns false, it halts the iteration.
func (txn *Txn) Range(column string, fn func(v Cursor) bool) error {
	txn.owner.lock.RLock()
	defer txn.owner.lock.RUnlock()

	cur, err := txn.cursorFor(column)
	if err != nil {
		return err
	}

	txn.index.Range(func(x uint32) bool {
		cur.idx = x
		return fn(cur)
	})
	return nil
}

// cursorFor returns a cursor for a specified column
func (txn *Txn) cursorFor(columnName string) (Cursor, error) {
	c, ok := txn.columnAt(columnName)
	if !ok {
		return Cursor{}, fmt.Errorf("column: specified column '%v' does not exist", columnName)
	}

	// Attempt to find the existing update queue index for this column
	updateQueueIndex := -1
	for i, c := range txn.updates {
		if c.name == columnName {
			updateQueueIndex = i
			break
		}
	}

	// Create a new update queue for the selected column
	if updateQueueIndex == -1 {
		updateQueueIndex = len(txn.updates)
		txn.updates = append(txn.updates, updateQueue{
			name:   columnName,
			update: make([]commit.Update, 0, 64),
		})
	}

	// Create a Cursor
	return Cursor{
		column: c,
		update: int16(updateQueueIndex),
		Selector: Selector{
			txn: txn,
		},
	}, nil
}

// Rollback empties the pending update and delete queues and does not apply any of
// the pending updates/deletes. This operation can be called several times for
// a transaction in order to perform partial rollbacks.
func (txn *Txn) rollback() {
	txn.deletes.Clear()
	txn.inserts.Clear()
	for i := range txn.updates {
		txn.updates[i].update = txn.updates[i].update[:0]
	}
}

// Commit commits the transaction by applying all pending updates and deletes to
// the collection. This operation is can be called several times for a transaction
// in order to perform partial commits. If there's no pending updates/deletes, this
// operation will result in a no-op.
func (txn *Txn) commit() {

	// Currently, we need to acquire a global lock in order to make sure that the entire
	// transaction is completely atomic.
	txn.owner.lock.Lock()
	defer txn.owner.lock.Unlock()

	txn.deletePending()
	txn.updatePending()
	txn.insertPending()
}

// updatePending updates the pending entries that were modified during the query
func (txn *Txn) updatePending() {

	// Now we can iterate over all of the updates and apply them.
	for i, u := range txn.updates {
		if len(u.update) == 0 {
			continue // No updates for this column
		}

		// Get the column that needs to be updated
		columns, exists := txn.owner.cols.LoadWithIndex(u.name)
		if !exists || len(columns) == 0 {
			continue
		}

		// Range through all of the pending updates and apply them to the column
		// and its associated computed columns.
		for _, v := range columns {
			max, _ := txn.inserts.Max()
			v.Update(u.update, max)
		}

		// If there's a writer, write before we unlock the column so that the transactions
		// are seiralized in the writer as well, making everything consistent.
		if txn.writer != nil {
			txn.writer.Write(commit.Commit{
				Type:    commit.TypeStore,
				Column:  u.name,
				Updates: txn.updates[i].update,
			})
		}

		// Reset the queue
		txn.updates[i].update = txn.updates[i].update[:0]
	}
}

// deletePending removes all of the entries marked as to be deleted
func (txn *Txn) deletePending() {
	if len(txn.deletes) == 0 {
		return // Nothing to delete
	}

	// Apply a batch delete on all of the columns
	txn.owner.cols.Range(func(column *column) {
		column.Delete(&txn.deletes)
	})

	// Clear the items in the collection and reinitialize the purge list
	txn.owner.fill.AndNot(txn.deletes)
	atomic.StoreUint64(&txn.owner.count, uint64(txn.owner.fill.Count()))

	// If there's an associated writer, write into it
	if txn.writer != nil {
		txn.writer.Write(commit.Commit{
			Type:    commit.TypeDelete,
			Deletes: txn.deletes,
		})
	}

	// Now that we're done with the deletion and the commit has been written, clear
	txn.deletes.Clear()
}

// insertPending inserts all of the entries marked as to be inserted. This just makes them
// visible by setting the fill list atomically in the collection.
func (txn *Txn) insertPending() {
	if len(txn.inserts) == 0 {
		return
	}

	txn.owner.fill.Or(txn.inserts)
	atomic.StoreUint64(&txn.owner.count, uint64(txn.owner.fill.Count()))

	// If there's a writer, write before we unlock the column so that the transactions
	// are seiralized in the writer as well, making everything consistent.
	if txn.writer != nil {
		txn.writer.Write(commit.Commit{
			Type:    commit.TypeInsert,
			Inserts: txn.inserts,
		})
	}
	txn.inserts.Clear()
}
