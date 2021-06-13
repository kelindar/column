// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"

	"github.com/kelindar/bitmap"
)

// Bitmaps represents a pool of bitmaps
var bitmaps = &sync.Pool{
	New: func() interface{} {
		return &bitmap.Bitmap{}
	},
}

// aquireBitmap acquires a bitmap for a transaction
func aquireBitmap(source *bitmap.Bitmap) *bitmap.Bitmap {
	b := bitmaps.Get().(*bitmap.Bitmap)
	source.Clone(b)
	return b
}

// releaseBitmap releases a bitmap back to the pool
func releaseBitmap(b *bitmap.Bitmap) {
	bitmaps.Put(b)
}

// --------------------------- Transaction ----------------------------

// Txn represents a transaction which supports filtering and projection.
type Txn struct {
	owner *Collection    // The target collection
	index *bitmap.Bitmap // The filtering index
}

// With applies a logical AND operation to the current query and the specified index.
func (txn Txn) With(column string, extra ...string) Txn {
	if idx, ok := txn.owner.cols[column]; ok {
		idx.Intersect(txn.index)
	} else {
		txn.index.Clear()
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := txn.owner.cols[e]; ok {
			idx.Intersect(txn.index)
		} else {
			txn.index.Clear()
		}
	}
	return txn
}

// Without applies a logical AND NOT operation to the current query and the specified index.
func (txn Txn) Without(column string, extra ...string) Txn {
	if idx, ok := txn.owner.cols[column]; ok {
		idx.Difference(txn.index)
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := txn.owner.cols[e]; ok {
			idx.Difference(txn.index)
		}
	}
	return txn
}

// Union computes a union between the current query and the specified index.
func (txn Txn) Union(column string, extra ...string) Txn {
	if idx, ok := txn.owner.cols[column]; ok {
		idx.Union(txn.index)
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := txn.owner.cols[e]; ok {
			idx.Union(txn.index)
		}
	}
	return txn
}

// WithValue applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (txn Txn) WithValue(column string, predicate func(v interface{}) bool) Txn {
	if p, ok := txn.owner.cols[column]; ok {
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
func (txn Txn) WithFloat(column string, predicate func(v float64) bool) Txn {
	if p, ok := txn.owner.cols[column]; ok {
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
func (txn Txn) WithInt(column string, predicate func(v int64) bool) Txn {
	if p, ok := txn.owner.cols[column]; ok {
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
func (txn Txn) WithUint(column string, predicate func(v uint64) bool) Txn {
	if p, ok := txn.owner.cols[column]; ok {
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
func (txn Txn) WithString(column string, predicate func(v string) bool) Txn {
	return txn.WithValue(column, func(v interface{}) bool {
		return predicate(v.(string))
	})
}

// Count returns the number of objects matching the query
func (txn Txn) Count() int {
	return int(txn.index.Count())
}

// Range iterates over the result set and allows to query or update any column. While
// this is flexible, it is not the most efficient way, consider Select() as an alternative
// iteration method.
func (txn Txn) Range(fn func(v Cursor) bool) {
	txn.index.Range(func(x uint32) bool {
		return fn(Cursor{
			index: x,
			owner: txn.owner,
		})
	})
}

// Select selects and iterates over a specific column. The selector provided also allows
// to select other columns, but at a slight performance cost.
func (txn Txn) Select(fn func(v Selector) bool, column string) error {
	c, ok := txn.owner.cols[column]
	if !ok {
		return fmt.Errorf("select: specified column '%v' does not exist", column)
	}

	// Create a selector
	cur := Selector{
		column: c,
		Cursor: Cursor{
			owner: txn.owner,
		},
	}

	txn.index.Range(func(x uint32) bool {
		cur.index = x
		return fn(cur)
	})
	return nil
}

// SelectMany selects and iterates over a set of specified columns. The selector provided also allows
// to select other columns, but at a slight performance cost.
func (txn Txn) SelectMany(fn func(v []Selector) bool, columns ...string) error {
	selectors := make([]Selector, len(columns))
	for i, columnName := range columns {
		c, ok := txn.owner.cols[columnName]
		if !ok {
			return fmt.Errorf("select: specified column '%v' does not exist", columnName)
		}

		selectors[i] = Selector{
			column: c,
			Cursor: Cursor{
				owner: txn.owner,
			},
		}
	}

	txn.index.Range(func(x uint32) bool {
		for i := 0; i < len(selectors); i++ {
			selectors[i].index = x
		}
		return fn(selectors)
	})
	return nil
}

// --------------------------- Cursor ---------------------------

// Cursor represents a iteration cursor that supports both retrieval of column values
// for the specified row and modification (update, delete).
type Cursor struct {
	index uint32      // The current index
	owner *Collection // The owner collection
}

// ValueOf reads a value for a current row at a given column.
func (cur *Cursor) ValueOf(column string) (out interface{}) {
	if c, ok := cur.owner.cols[column]; ok {
		out, _ = c.Value(cur.index)
	}
	return
}

// StringOf reads a string value for a current row at a given column.
func (cur *Cursor) StringOf(column string) (out string) {
	if c, ok := cur.owner.cols[column]; ok {
		if v, ok := c.Value(cur.index); ok {
			out, _ = v.(string)
		}
	}
	return
}

// FloatOf reads a float64 value for a current row at a given column.
func (cur *Cursor) FloatOf(column string) (out float64) {
	if c, ok := cur.owner.cols[column]; ok {
		if n, ok := c.(numerical); ok {
			out, _ = n.Float64(cur.index)
		}
	}
	return
}

// IntOf reads an int64 value for a current row at a given column.
func (cur *Cursor) IntOf(column string) (out int64) {
	if c, ok := cur.owner.cols[column]; ok {
		if n, ok := c.(numerical); ok {
			out, _ = n.Int64(cur.index)
		}
	}
	return
}

// UintOf reads a uint64 value for a current row at a given column.
func (cur *Cursor) UintOf(column string) (out uint64) {
	if c, ok := cur.owner.cols[column]; ok {
		if n, ok := c.(numerical); ok {
			out, _ = n.Uint64(cur.index)
		}
	}
	return
}

// BoolOf reads a boolean value for a current row at a given column.
func (cur *Cursor) BoolOf(column string) bool {
	if c, ok := cur.owner.cols[column]; ok {
		return c.Contains(cur.index)
	}
	return false
}

// --------------------------- Update ----------------------------

// Update represents an update operation
type Update struct {
	Index uint32      // The index to update/delete
	Value interface{} // The value to update to
}

// Delete deletes the current item. The actual operation will be queued and
// executed once the current the transaction completes.
func (cur *Cursor) Delete() {
	cur.owner.qlock.Lock()
	cur.owner.deletes.Set(cur.index)
	cur.owner.qlock.Unlock()
}

// Update updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) Update(column string, value interface{}) {
	cur.owner.qlock.Lock()
	cur.owner.updates[column] = append(cur.owner.updates[column], Update{
		Index: cur.index,
		Value: value,
	})
	cur.owner.qlock.Unlock()
}

// --------------------------- Selector ---------------------------

// Selector represents a iteration cursor that is bound to a specific column.
type Selector struct {
	Cursor
	column Column // The selected column
}

// Value reads a value for a current row at a given column.
func (cur *Selector) Value() (out interface{}) {
	out, _ = cur.column.Value(cur.index)
	return
}

// String reads a string value for a current row at a given column.
func (cur *Selector) String() (out string) {
	if v, ok := cur.column.Value(cur.index); ok {
		out, _ = v.(string)
	}
	return
}

// Float reads a float64 value for a current row at a given column.
func (cur *Selector) Float() (out float64) {
	if n, ok := cur.column.(numerical); ok {
		out, _ = n.Float64(cur.index)
	}
	return
}

// Int reads an int64 value for a current row at a given column.
func (cur *Selector) Int() (out int64) {
	if n, ok := cur.column.(numerical); ok {
		out, _ = n.Int64(cur.index)
	}
	return
}

// Uint reads a uint64 value for a current row at a given column.
func (cur *Selector) Uint() (out uint64) {
	if n, ok := cur.column.(numerical); ok {
		out, _ = n.Uint64(cur.index)
	}
	return
}

// Bool reads a boolean value for a current row at a given column.
func (cur *Selector) Bool() bool {
	return cur.column.Contains(cur.index)
}
