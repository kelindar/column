// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Reader ---------------------------

// Reader represents a reader cursor for a specific row/column combination.
type Reader interface {
	Index() uint32
	String() string
	Float() float64
	Int() int
	Uint() uint
	Bool() bool
}

// Assert reader implementations. Both our cursor and commit reader need to implement
// this so that we can feed it to the index transparently.
var _ Reader = new(commit.Reader)

// --------------------------- Index ----------------------------

// computed represents a computed column
type computed interface {
	Column() string
}

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
		case commit.Put, commit.Add:
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
