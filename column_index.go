// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- computed index ----------------------------

// computed represents a computed column
type computed interface {
	Column() string
}

// Index represents the index implementation
type index struct {
	fill bitmap.Bitmap
	prop string
	rule func(v interface{}) bool
}

// newIndex creates a new indexer
func newIndex(indexName, columnName string, rule func(v interface{}) bool) *column {
	return columnFor(indexName, &index{
		fill: make(bitmap.Bitmap, 0, 4),
		prop: columnName,
		rule: rule,
	})
}

// Grow grows the size of the column until we have enough to store
func (c *index) Grow(idx uint32) {
	c.fill.Grow(idx)
}

// Column returns the target name of the column on which this index should apply.
func (c *index) Column() string {
	return c.prop
}

// Update performs a series of updates at once
func (c *index) Update(updates []commit.Update) {

	// Index can only be updated based on the final stored value, so we can only work
	// with put operations here. The trick is to update the final value after applying
	// on the actual column.
	for _, u := range updates {
		if u.Type == commit.Put {
			if c.rule(u.Value) {
				c.fill.Set(u.Index)
			} else {
				c.fill.Remove(u.Index)
			}
		}
	}
}

// Delete deletes a set of items from the column.
func (c *index) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Value retrieves a value at a specified index.
func (c *index) Value(idx uint32) (v interface{}, ok bool) {
	if idx < uint32(len(c.fill))<<6 {
		v, ok = c.fill.Contains(idx), true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *index) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *index) Index() *bitmap.Bitmap {
	return &c.fill
}
