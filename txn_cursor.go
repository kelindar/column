// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"

	"github.com/kelindar/column/commit"
)

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

// cursorFor returns a cursor for a specified column
func (txn *Txn) cursorFor(columnName string) (Cursor, error) {
	c, ok := txn.columnAt(columnName)
	if !ok {
		return Cursor{}, fmt.Errorf("column: specified column '%v' does not exist", columnName)
	}

	// Create a Cursor
	return Cursor{
		column: c,
		update: txn.bufferFor(columnName),
		Selector: Selector{
			txn: txn,
		},
	}, nil
}

// --------------------------- Selector ---------------------------

// Selector represents a iteration Selector that supports both retrieval of column values
// for the specified row and modification (update, delete).
type Selector struct {
	idx uint32      // The current index
	txn *Txn        // The optional transaction, but one of them is required
	col *Collection // The optional collection, but one of them is required
}

// columnAt loads the column based on whether the selector has a transaction or not.
func (cur *Selector) columnAt(column string) (*column, bool) {
	if cur.txn != nil {
		return cur.txn.columnAt(column)
	}

	// Load directly from the collection
	return cur.col.cols.Load(column)
}

// ValueAt reads a value for a current row at a given column.
func (cur *Selector) ValueAt(column string) (out interface{}) {
	if c, ok := cur.columnAt(column); ok {
		out, _ = c.Value(cur.idx)
	}
	return
}

// StringAt reads a string value for a current row at a given column.
func (cur *Selector) StringAt(column string) (out string) {
	if c, ok := cur.columnAt(column); ok {
		out, _ = c.String(cur.idx)
	}
	return
}

// FloatAt reads a float64 value for a current row at a given column.
func (cur *Selector) FloatAt(column string) (out float64) {
	if c, ok := cur.columnAt(column); ok {
		out, _ = c.Float64(cur.idx)
	}
	return
}

// IntAt reads an int64 value for a current row at a given column.
func (cur *Selector) IntAt(columnName string) (out int64) {
	if c, ok := cur.columnAt(columnName); ok {
		out, _ = c.Int64(cur.idx)
	}
	return
}

// UintAt reads a uint64 value for a current row at a given column.
func (cur *Selector) UintAt(column string) (out uint64) {
	if c, ok := cur.columnAt(column); ok {
		out, _ = c.Uint64(cur.idx)
	}
	return
}

// BoolAt reads a boolean value for a current row at a given column.
func (cur *Selector) BoolAt(column string) bool {
	if c, ok := cur.columnAt(column); ok {
		return c.Contains(cur.idx)
	}
	return false
}

// --------------------------- Cursor ---------------------------

// Cursor represents a iteration Selector that is bound to a specific column.
type Cursor struct {
	Selector
	update *commit.Buffer // The index of the update queue
	column *column        // The selected column
}

// Index returns the current index of the cursor.
func (cur *Cursor) Index() uint32 {
	return cur.idx
}

// Value reads a value for a current row at a given column.
func (cur *Cursor) Value() (out interface{}) {
	out, _ = cur.column.Value(cur.idx)
	return
}

// String reads a string value for a current row at a given column.
func (cur *Cursor) String() (out string) {
	out, _ = cur.column.String(cur.idx)
	return
}

// Float reads a float64 value for a current row at a given column.
func (cur *Cursor) Float() (out float64) {
	out, _ = cur.column.Float64(cur.idx)
	return
}

// Int reads an int64 value for a current row at a given column.
func (cur *Cursor) Int() int {
	out, _ := cur.column.Int64(cur.idx)
	return int(out)
}

// Uint reads a uint64 value for a current row at a given column.
func (cur *Cursor) Uint() uint {
	out, _ := cur.column.Uint64(cur.idx)
	return uint(out)
}

// Bool reads a boolean value for a current row at a given column.
func (cur *Cursor) Bool() bool {
	return cur.column.Contains(cur.idx)
}

// --------------------------- Update/Delete ----------------------------

// Delete deletes the current item. The actual operation will be queued and
// executed once the current the transaction completes.
func (cur *Cursor) Delete() {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.txn.deletes.Set(cur.idx)
}

// Update updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) Update(value interface{}) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.update.PutAny(commit.Put, cur.idx, value)
}

// UpdateAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateAt(column string, value interface{}) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.txn.bufferFor(column).PutAny(commit.Put, cur.idx, value)
}

// UpdateString updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateString(value string) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.update.PutString(commit.Put, cur.idx, value)
}

// UpdateStringAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateStringAt(column string, value string) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.txn.bufferFor(column).PutString(commit.Put, cur.idx, value)
}

// UpdateBool updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateBool(value bool) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.update.PutBool(commit.Put, cur.idx, value)
}

// UpdateBoolAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateBoolAt(column string, value bool) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.txn.bufferFor(column).PutBool(commit.Put, cur.idx, value)
}

// UpdateFloat64 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateFloat64(value float64) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.update.PutFloat64(commit.Put, cur.idx, value)
}

// AddFloat64 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddFloat64(amount float64) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.update.PutFloat64(commit.Add, cur.idx, amount)
}

// UpdateFloat64At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateFloat64At(column string, value float64) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.txn.bufferFor(column).PutFloat64(commit.Put, cur.idx, value)
}

// AddFloat64At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddFloat64At(column string, amount float64) {
	cur.txn.dirty.Set(cur.idx >> chunkShift)
	cur.txn.bufferFor(column).PutFloat64(commit.Add, cur.idx, amount)
}
