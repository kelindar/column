// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"

	"github.com/kelindar/column/commit"
)

// cursorFor returns a cursor for a specified column
func (txn *Txn) cursorFor(columnName string) (Cursor, error) {
	c, ok := txn.columnAt(columnName)
	if !ok {
		return Cursor{}, fmt.Errorf("column: specified column '%v' does not exist", columnName)
	}

	// Attempt to find the existing update queue index for this column
	updateQueueIndex := -1
	for i, c := range txn.updates {
		if c.Column == columnName {
			updateQueueIndex = i
			break
		}
	}

	// Create a new update queue for the selected column
	if updateQueueIndex == -1 {
		updateQueueIndex = len(txn.updates)
		txn.updates = append(txn.updates, txns.acquirePage(columnName))
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
	update int16   // The index of the update queue
	column *column // The selected column
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
func (cur *Cursor) Int() (out int64) {
	out, _ = cur.column.Int64(cur.idx)
	return
}

// Uint reads a uint64 value for a current row at a given column.
func (cur *Cursor) Uint() (out uint64) {
	out, _ = cur.column.Uint64(cur.idx)
	return
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
	cur.updateChunk(cur.idx)
	cur.txn.updates[cur.update].Update = append(cur.txn.updates[cur.update].Update, commit.Update{
		Type:  commit.Put,
		Index: cur.idx,
		Value: value,
	})
}

// Add atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) Add(amount interface{}) {
	cur.updateChunk(cur.idx)
	cur.txn.updates[cur.update].Update = append(cur.txn.updates[cur.update].Update, commit.Update{
		Type:  commit.Add,
		Index: cur.idx,
		Value: amount,
	})

}

// UpdateAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateAt(column string, value interface{}) {
	columnIndex := cur.updateChunkAt(column, cur.idx)
	cur.txn.updates[columnIndex].Update = append(cur.txn.updates[columnIndex].Update, commit.Update{
		Type:  commit.Put,
		Index: cur.idx,
		Value: value,
	})
}

// Add atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddAt(column string, amount interface{}) {
	columnIndex := cur.updateChunkAt(column, cur.idx)
	cur.txn.updates[columnIndex].Update = append(cur.txn.updates[columnIndex].Update, commit.Update{
		Type:  commit.Add,
		Index: cur.idx,
		Value: amount,
	})
}

func (cur *Cursor) updateChunk(idx uint32) {
	chunk := idx >> chunkShift
	if cur.txn.dirty.Contains(chunk) {
		return
	}

	cur.txn.dirty.Set(chunk)
	if cur.txn.updates[cur.update].Current != int(chunk) {
		cur.txn.updates[cur.update].Offsets = append(cur.txn.updates[cur.update].Offsets, len(cur.txn.updates[cur.update].Update))
		cur.txn.updates[cur.update].Current = int(chunk)
	}
}

func (cur *Cursor) updateChunkAt(column string, idx uint32) int {
	chunk := idx >> chunkShift
	cur.txn.dirty.Set(chunk)

	for i, c := range cur.txn.updates {
		if c.Column == column {
			if c.Current != int(chunk) {
				cur.txn.updates[i].Offsets = append(cur.txn.updates[i].Offsets, len(cur.txn.updates[i].Update))
				cur.txn.updates[i].Current = int(chunk)
			}
			return i
		}
	}

	// Create a new update queue
	page := txns.acquirePage(column)
	page.Offsets = append(page.Offsets, 0)
	page.Current = int(chunk)
	cur.txn.updates = append(cur.txn.updates, page)
	return len(cur.txn.updates) - 1
}
