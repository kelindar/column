// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import "github.com/kelindar/column/commit"

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
		if v, ok := c.Value(cur.idx); ok {
			out, _ = v.(string)
		}
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
	if v, ok := cur.column.Value(cur.idx); ok {
		out, _ = v.(string)
	}
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
	cur.txn.deletes.Set(cur.idx)
}

// Update updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) Update(value interface{}) {
	cur.txn.updates[cur.update].update = append(cur.txn.updates[cur.update].update, commit.Update{
		Type:  commit.Put,
		Index: cur.idx,
		Value: value,
	})
}

// Add atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) Add(amount interface{}) {
	cur.txn.updates[cur.update].update = append(cur.txn.updates[cur.update].update, commit.Update{
		Type:  commit.Add,
		Index: cur.idx,
		Value: amount,
	})
}

// UpdateAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) UpdateAt(column string, value interface{}) {
	for i, c := range cur.txn.updates {
		if c.name == column {
			cur.txn.updates[i].update = append(c.update, commit.Update{
				Type:  commit.Put,
				Index: cur.idx,
				Value: value,
			})
			return
		}
	}

	// Create a new update queue
	cur.txn.updates = append(cur.txn.updates, updateQueue{
		name: column,
		update: []commit.Update{{
			Type:  commit.Put,
			Index: cur.idx,
			Value: value,
		}},
	})
}

// Add atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddAt(column string, amount interface{}) {
	for i, c := range cur.txn.updates {
		if c.name == column {
			cur.txn.updates[i].update = append(c.update, commit.Update{
				Type:  commit.Add,
				Index: cur.idx,
				Value: amount,
			})
			return
		}
	}

	// Create a new update queue
	cur.txn.updates = append(cur.txn.updates, updateQueue{
		name: column,
		update: []commit.Update{{
			Type:  commit.Add,
			Index: cur.idx,
			Value: amount,
		}},
	})
}
