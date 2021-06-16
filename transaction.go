// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"

	"github.com/kelindar/bitmap"
)

// UpdateKind represents a type of an update operation.
type UpdateKind uint8

// Various update operations supported.
const (
	UpdatePut UpdateKind = iota // Put stores a value regardless of a previous value
	UpdateAdd                   // Add increments the current stored value by the amount
)

// Update represents an update operation
type Update struct {
	Kind  UpdateKind  // The type of an update operation
	Index uint32      // The index to update/delete
	Value interface{} // The value to update to
}

// --------------------------- Pool of Transactions ----------------------------

// txns represents a pool of transactions
var txns = &sync.Pool{
	New: func() interface{} {
		return &Txn{
			index:   make(bitmap.Bitmap, 0, 64),
			deletes: make(bitmap.Bitmap, 0, 64),
			updates: make([]updateQueue, 0, 16),
			columns: make([]columnCache, 0, 16),
		}
	},
}

// aquireBitmap acquires a transaction for a transaction
func aquireTxn(owner *Collection) *Txn {
	txn := txns.Get().(*Txn)
	txn.owner = owner
	txn.columns = txn.columns[:0]
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
	updates []updateQueue // The update queue
	columns []columnCache // The column mapping
}

// Update queue represents a queue per column that contains the pending updates.
type updateQueue struct {
	name   string   // The column name
	update []Update // The update queue
}

// columnCache caches a column by its name. This speeds things up since it's a very
// common operation.
type columnCache struct {
	name string // The column name
	col  Column // The columns and its computed
}

// columnAt loads and caches the column for the transaction
func (txn *Txn) columnAt(columnName string) (Column, bool) {
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
func (txn *Txn) With(column string, extra ...string) *Txn {
	if idx, ok := txn.columnAt(column); ok {
		idx.Intersect(&txn.index)
	} else {
		txn.index.Clear()
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := txn.columnAt(e); ok {
			idx.Intersect(&txn.index)
		} else {
			txn.index.Clear()
		}
	}
	return txn
}

// Without applies a logical AND NOT operation to the current query and the specified index.
func (txn *Txn) Without(column string, extra ...string) *Txn {
	if idx, ok := txn.columnAt(column); ok {
		idx.Difference(&txn.index)
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := txn.columnAt(e); ok {
			idx.Difference(&txn.index)
		}
	}
	return txn
}

// Union computes a union between the current query and the specified index.
func (txn *Txn) Union(column string, extra ...string) *Txn {
	if idx, ok := txn.columnAt(column); ok {
		idx.Union(&txn.index)
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := txn.columnAt(e); ok {
			idx.Union(&txn.index)
		}
	}
	return txn
}

// WithValue applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (txn *Txn) WithValue(column string, predicate func(v interface{}) bool) *Txn {
	if p, ok := txn.columnAt(column); ok {
		txn.index.Filter(func(x uint32) bool {
			if v, ok := p.Value(x); ok {
				return predicate(v)
			}
			return false
		})
	}
	return txn
}

// WithFloat filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to float64.
func (txn *Txn) WithFloat(column string, predicate func(v float64) bool) *Txn {
	if p, ok := txn.columnAt(column); ok {
		if n, ok := p.(numerical); ok {
			txn.index.Filter(func(x uint32) bool {
				if v, ok := n.Float64(x); ok {
					return predicate(v)
				}
				return false
			})
		}
	}
	return txn
}

// WithInt filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to int64.
func (txn *Txn) WithInt(column string, predicate func(v int64) bool) *Txn {
	if p, ok := txn.columnAt(column); ok {
		if n, ok := p.(numerical); ok {
			txn.index.Filter(func(x uint32) bool {
				if v, ok := n.Int64(x); ok {
					return predicate(v)
				}
				return false
			})
		}
	}
	return txn
}

// WithUint filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to uint64.
func (txn *Txn) WithUint(column string, predicate func(v uint64) bool) *Txn {
	if p, ok := txn.columnAt(column); ok {
		if n, ok := p.(numerical); ok {
			txn.index.Filter(func(x uint32) bool {
				if v, ok := n.Uint64(x); ok {
					return predicate(v)
				}
				return false
			})
		}
	}
	return txn
}

// WithString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (txn *Txn) WithString(column string, predicate func(v string) bool) *Txn {
	return txn.WithValue(column, func(v interface{}) bool {
		return predicate(v.(string))
	})
}

// Count returns the number of objects matching the query
func (txn *Txn) Count() int {
	return int(txn.index.Count())
}

// Select iterates over the result set and allows to query or update any column. While
// this is flexible, it is not the most efficient way, consider Range() as an alternative
// iteration method over a specific column.
func (txn *Txn) Select(fn func(v Selector) bool) {
	txn.index.Range(func(x uint32) bool {
		return fn(Selector{
			index: x,
			txn:   txn,
			owner: txn.owner,
		})
	})
}

// Range selects and iterates over a specific column. The cursor provided also allows
// to select other columns, but at a slight performance cost.
func (txn *Txn) Range(column string, fn func(v Cursor) bool) error {
	c, ok := txn.columnAt(column)
	if !ok {
		return fmt.Errorf("select: specified column '%v' does not exist", column)
	}

	// Attempt to find the existing update queue index for this column
	updateQueueIndex := -1
	for i, c := range txn.updates {
		if c.name == column {
			updateQueueIndex = i
			break
		}
	}

	// Create a new update queue for the selected column
	if updateQueueIndex == -1 {
		updateQueueIndex = 0
		txn.updates = append(txn.updates, updateQueue{
			name:   column,
			update: make([]Update, 0, 64),
		})
	}

	// Create a Cursor
	cur := Cursor{
		column: c,
		update: int16(updateQueueIndex),
		Selector: Selector{
			txn:   txn,
			owner: txn.owner,
		},
	}

	txn.index.Range(func(x uint32) bool {
		cur.index = x
		return fn(cur)
	})
	return nil
}

// Commit commits the transaction by applying all pending updates and deletes to
// the collection. This operation is can be called several times for a transaction
// in order to perform partial commits. If there's no pending updates/deletes, this
// operation will result in a no-op.
func (txn *Txn) Commit() {
	txn.updatePending()
	txn.deletePending()
}

// Rollback empties the pending update and delete queues and does not apply any of
// the pending updates/deletes. This operation can be called several times for
// a transaction in order to perform partial rollbacks.
func (txn *Txn) Rollback() {
	txn.deletes.Clear()
	for i := range txn.updates {
		txn.updates[i].update = txn.updates[i].update[:0]
	}
}

// updatePending updates the pending entries that were modified during the query
func (txn *Txn) updatePending() {
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
			v.UpdateMany(u.update)
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
	txn.owner.cols.Range(func(column Column) {
		column.DeleteMany(&txn.deletes)
	})

	// Clear the items in the collection and reinitialize the purge list
	txn.owner.lock.Lock()
	txn.owner.fill.AndNot(txn.deletes)
	txn.owner.lock.Unlock()
	txn.deletes.Clear()
}

// --------------------------- Selector ---------------------------

// Selector represents a iteration Selector that supports both retrieval of column values
// for the specified row and modification (update, delete).
type Selector struct {
	index    uint32      // The current index
	owner    *Collection // The owner collection
	txn      *Txn        // The transaction
	updateAt int32       // The update queue index
	column   Column      // The selected column
}

// ValueAt reads a value for a current row at a given column.
func (cur *Selector) ValueAt(column string) (out interface{}) {
	if c, ok := cur.owner.cols.Load(column); ok {
		out, _ = c.Value(cur.index)
	}
	return
}

// StringAt reads a string value for a current row at a given column.
func (cur *Selector) StringAt(column string) (out string) {
	if c, ok := cur.owner.cols.Load(column); ok {
		if v, ok := c.Value(cur.index); ok {
			out, _ = v.(string)
		}
	}
	return
}

// FloatAt reads a float64 value for a current row at a given column.
func (cur *Selector) FloatAt(column string) (out float64) {
	if c, ok := cur.owner.cols.Load(column); ok {
		if n, ok := c.(numerical); ok {
			out, _ = n.Float64(cur.index)
		}
	}
	return
}

// IntAt reads an int64 value for a current row at a given column.
func (cur *Selector) IntAt(column string) (out int64) {
	if c, ok := cur.owner.cols.Load(column); ok {
		if n, ok := c.(numerical); ok {
			out, _ = n.Int64(cur.index)
		}
	}
	return
}

// UintAt reads a uint64 value for a current row at a given column.
func (cur *Selector) UintAt(column string) (out uint64) {
	if c, ok := cur.owner.cols.Load(column); ok {
		if n, ok := c.(numerical); ok {
			out, _ = n.Uint64(cur.index)
		}
	}
	return
}

// BoolAt reads a boolean value for a current row at a given column.
func (cur *Selector) BoolAt(column string) bool {
	if c, ok := cur.owner.cols.Load(column); ok {
		return c.Contains(cur.index)
	}
	return false
}

// --------------------------- Update ----------------------------

// Delete deletes the current item. The actual operation will be queued and
// executed once the current the transaction completes.
func (cur *Selector) Delete() {
	cur.txn.deletes.Set(cur.index)
}

// UpdateAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Selector) UpdateAt(column string, value interface{}) {
	for i, c := range cur.txn.updates {
		if c.name == column {
			cur.txn.updates[i].update = append(c.update, Update{
				Kind:  UpdatePut,
				Index: cur.index,
				Value: value,
			})
			return
		}
	}

	// Create a new update queue
	cur.txn.updates = append(cur.txn.updates, updateQueue{
		name: column,
		update: []Update{{
			Kind:  UpdatePut,
			Index: cur.index,
			Value: value,
		}},
	})
}

// Add atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Selector) AddAt(column string, amount interface{}) {
	for i, c := range cur.txn.updates {
		if c.name == column {
			cur.txn.updates[i].update = append(c.update, Update{
				Kind:  UpdateAdd,
				Index: cur.index,
				Value: amount,
			})
			return
		}
	}

	// Create a new update queue
	cur.txn.updates = append(cur.txn.updates, updateQueue{
		name: column,
		update: []Update{{
			Kind:  UpdateAdd,
			Index: cur.index,
			Value: amount,
		}},
	})
}

// --------------------------- Cursor ---------------------------

// Cursor represents a iteration Selector that is bound to a specific column.
type Cursor struct {
	Selector
	update int16  // The index of the update queue
	column Column // The selected column
}

// Value reads a value for a current row at a given column.
func (cur *Cursor) Value() (out interface{}) {
	out, _ = cur.column.Value(cur.index)
	return
}

// String reads a string value for a current row at a given column.
func (cur *Cursor) String() (out string) {
	if v, ok := cur.column.Value(cur.index); ok {
		out, _ = v.(string)
	}
	return
}

// Float reads a float64 value for a current row at a given column.
func (cur *Cursor) Float() (out float64) {
	if n, ok := cur.column.(numerical); ok {
		out, _ = n.Float64(cur.index)
	}
	return
}

// Int reads an int64 value for a current row at a given column.
func (cur *Cursor) Int() (out int64) {
	if n, ok := cur.column.(numerical); ok {
		out, _ = n.Int64(cur.index)
	}
	return
}

// Uint reads a uint64 value for a current row at a given column.
func (cur *Cursor) Uint() (out uint64) {
	if n, ok := cur.column.(numerical); ok {
		out, _ = n.Uint64(cur.index)
	}
	return
}

// Bool reads a boolean value for a current row at a given column.
func (cur *Cursor) Bool() bool {
	return cur.column.Contains(cur.index)
}

// Update updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) Update(value interface{}) {
	cur.txn.updates[cur.update].update = append(cur.txn.updates[cur.update].update, Update{
		Kind:  UpdatePut,
		Index: cur.index,
		Value: value,
	})
}

// Add atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) Add(amount interface{}) {
	cur.txn.updates[cur.update].update = append(cur.txn.updates[cur.update].update, Update{
		Kind:  UpdateAdd,
		Index: cur.index,
		Value: amount,
	})
}
