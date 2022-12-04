// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"strings"
	// "runtime/debug"
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"

	"github.com/tidwall/btree"
)

// --------------------------- Reader ---------------------------

// Reader represents a reader cursor for a specific row/column combination.
type Reader interface {
	IsUpsert() bool
	IsDelete() bool
	Index() uint32
	String() string
	Bytes() []byte
	Float() float64
	Int() int
	Uint() uint
	Bool() bool
}

// Assert reader implementations. Both our cursor and commit reader need to implement
// this so that we can feed it to the index transparently.
var _ Reader = new(commit.Reader)

// computed represents a computed column
type computed interface {
	Column() string
}

// --------------------------- Index ----------------------------

// columnIndex represents the index implementation
type columnIndex struct {
	fill bitmap.Bitmap     // The fill list for the column
	name string            // The name of the target column
	rule func(Reader) bool // The rule to apply when building the index
}

// newIndex creates a new bitmap index column.
func newIndex(indexName, columnName string, rule func(Reader) bool) *column {
	return columnFor(indexName, &columnIndex{
		fill: make(bitmap.Bitmap, 0, 4),
		name: columnName,
		rule: rule,
	})
}

// Grow grows the size of the column until we have enough to store
func (c *columnIndex) Grow(idx uint32) {
	c.fill.Grow(idx)
}

// Column returns the target name of the column on which this index should apply.
func (c *columnIndex) Column() string {
	return c.name
}

// Apply applies a set of operations to the column.
func (c *columnIndex) Apply(chunk commit.Chunk, r *commit.Reader) {

	// Index can only be updated based on the final stored value, so we can only work
	// with put operations here. The trick is to update the final value after applying
	// on the actual column.
	for r.Next() {
		switch r.Type {
		case commit.Put:
			if c.rule(r) {
				c.fill.Set(uint32(r.Offset))
			} else {
				c.fill.Remove(uint32(r.Offset))
			}
		case commit.Delete:
			c.fill.Remove(uint32(r.Offset))
		}
	}
}

// Value retrieves a value at a specified index.
func (c *columnIndex) Value(idx uint32) (v interface{}, ok bool) {
	if idx < uint32(len(c.fill))<<6 {
		v, ok = c.fill.Contains(idx), true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *columnIndex) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnIndex) Index(chunk commit.Chunk) bitmap.Bitmap {
	return chunk.OfBitmap(c.fill)
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnIndex) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	dst.PutBitmap(commit.PutTrue, chunk, c.fill)
}

// --------------------------- Trigger ----------------------------

// columnTrigger represents the trigger implementation
type columnTrigger struct {
	name string       // The name of the target column
	clbk func(Reader) // The trigger callback
}

// newTrigger creates a new trigger column.
func newTrigger(indexName, columnName string, callback func(r Reader)) *column {
	return columnFor(indexName, &columnTrigger{
		name: columnName,
		clbk: callback,
	})
}

// Column returns the target name of the column on which this index should apply.
func (c *columnTrigger) Column() string {
	return c.name
}

// Grow grows the size of the column until we have enough to store
func (c *columnTrigger) Grow(idx uint32) {
	// Noop
}

// Apply applies a set of operations to the column.
func (c *columnTrigger) Apply(chunk commit.Chunk, r *commit.Reader) {
	for r.Next() {
		if r.Type == commit.Put || r.Type == commit.Delete {
			c.clbk(r)
		}
	}
}

// Value retrieves a value at a specified index.
func (c *columnTrigger) Value(idx uint32) (v any, ok bool) {
	return nil, false
}

// Contains checks whether the column has a value at a specified index.
func (c *columnTrigger) Contains(idx uint32) bool {
	return false
}

// Index returns the fill list for the column
func (c *columnTrigger) Index(chunk commit.Chunk) bitmap.Bitmap {
	return nil
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnTrigger) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	// Noop
}

// ----------------------- Sorted Index --------------------------

// SortIndexItem represents an offset sorted in a generic BTree
type SortIndexItem struct {
    Key    string
    Value  uint32
}

// columnSortIndex implements a constantly sorted column via BTree
type columnSortIndex struct {
	btree   *btree.BTreeG[SortIndexItem] // 1 constantly sorted data structure
	name    string         		     // The name of the target column
}

// newSortIndex creates a new bitmap index column.
func newSortIndex(indexName, columnName string) *column {
	byKeys := func (a, b SortIndexItem) bool {
		return a.Key < b.Key
	}
	return columnFor(indexName, &columnSortIndex{
		btree: btree.NewBTreeG[SortIndexItem](byKeys),
		name:  columnName,
	})
}

// Grow grows the size of the column until we have enough to store
func (c *columnSortIndex) Grow(idx uint32) {
	return
}

// Column returns the target name of the column on which this index should apply.
func (c *columnSortIndex) Column() string {
	return c.name
}


// Apply applies a set of operations to the column.
func (c *columnSortIndex) Apply(chunk commit.Chunk, r *commit.Reader) {

	// Index can only be updated based on the final stored value, so we can only work
	// with put, merge, & delete operations here.

	// TODO - account for updates to target column
	// will require Delete @ old key + Set @ new key
	for r.Next() {
		sit := SortIndexItem{
		    Key: strings.Clone(r.String()), // alloc required
		    Value: r.Index(),
		}
		switch r.Type {
		case commit.Put:
			// TODO: if idx already stored, delete it first (update)
			c.btree.Set(sit)
		case commit.Delete:
			// TODO: delete commits do not contain original string
			c.btree.Delete(sit)
		}
	}
}


// Value retrieves a value at a specified index.
func (c *columnSortIndex) Value(idx uint32) (v interface{}, ok bool) {
	return nil, false
}

// Contains checks whether the column has a value at a specified index.
func (c *columnSortIndex) Contains(idx uint32) bool {
	return false
}

// Index returns the fill list for the column
func (c *columnSortIndex) Index(chunk commit.Chunk) bitmap.Bitmap {
	return nil
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnSortIndex) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	// No op
}
