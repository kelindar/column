// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"math"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/kelindar/intmap"
	"github.com/zeebo/xxh3"
)

// --------------------------- Enum ----------------------------

var _ Textual = new(columnEnum)

// columnEnum represents a string column
type columnEnum struct {
	fill bitmap.Bitmap // The fill-list
	locs []uint32      // The list of locations
	seek *intmap.Sync  // The hash->location table
	data []string      // The string data
}

// makeEnum creates a new column
func makeEnum() Column {
	return &columnEnum{
		fill: make(bitmap.Bitmap, 0, 4),
		locs: make([]uint32, 0, 64),
		seek: intmap.NewSync(64, .95),
		data: make([]string, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnEnum) Grow(idx uint32) {
	if idx < uint32(len(c.locs)) {
		return
	}

	if idx < uint32(cap(c.locs)) {
		c.fill.Grow(idx)
		c.locs = c.locs[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint32, idx+1, resize(cap(c.locs), idx+1))
	copy(clone, c.locs)
	c.locs = clone
}

// Apply applies a set of operations to the column.
func (c *columnEnum) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			// Set the value at the index
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.locs[r.Offset] = c.findOrAdd(r.Bytes())

		case commit.Delete:
			c.fill.Remove(r.Index())
			// TODO: remove unused strings, need some reference counting for that
			// and can proably be done during vacuum() instead
		}
	}
}

// Search for the string or adds it and returns the offset
func (c *columnEnum) findOrAdd(v []byte) uint32 {
	target := uint32(xxh3.Hash(v))
	at, _ := c.seek.LoadOrStore(target, func() uint32 {
		c.data = append(c.data, string(v))
		return uint32(len(c.data)) - 1
	})
	return at
}

// readAt reads a string at a location
func (c *columnEnum) readAt(at uint32) string {
	return c.data[at]
}

// Value retrieves a value at a specified index
func (c *columnEnum) Value(idx uint32) (v interface{}, ok bool) {
	return c.LoadString(idx)
}

// LoadString retrieves a value at a specified index
func (c *columnEnum) LoadString(idx uint32) (v string, ok bool) {
	if idx < uint32(len(c.locs)) && c.fill.Contains(idx) {
		v, ok = c.readAt(c.locs[idx]), true
	}
	return
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnEnum) FilterString(offset uint32, index bitmap.Bitmap, predicate func(v string) bool) {
	cache := struct {
		index uint32 // Last seen offset
		value bool   // Last evaluated predicate
	}{
		index: math.MaxUint32,
		value: false,
	}

	// Do a quick ellimination of elements which are NOT contained in this column, this
	// allows us not to check contains during the filter itself
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])

	// Filters down the strings, if strings repeat we avoid reading every time by
	// caching the last seen index/value combination.
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		if at := c.locs[idx]; at != cache.index {
			cache.index = at
			cache.value = predicate(c.readAt(at))
			return cache.value
		}

		// The value is cached, avoid evaluating it
		return cache.value
	})
}

// Contains checks whether the column has a value at a specified index.
func (c *columnEnum) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnEnum) Index() *bitmap.Bitmap {
	return &c.fill
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnEnum) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutString(commit.Put, idx, c.readAt(c.locs[idx]))
	})
}

// slice accessor for enums
type enumSlice struct {
	writer *commit.Buffer
	reader *columnEnum
}

// Set sets the value at the specified index
func (s enumSlice) Set(index uint32, value string) {
	s.writer.PutString(commit.Put, index, value)
}

// Get loads the value at a particular index
func (s enumSlice) Get(index uint32) (string, bool) {
	return s.reader.LoadString(index)
}

// Enum returns a enumerable column accessor
func (txn *Txn) Enum(columnName string) enumSlice {
	writer := txn.bufferFor(columnName)
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*columnEnum)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type enum", columnName))
	}

	return enumSlice{
		writer: writer,
		reader: reader,
	}
}

// --------------------------- String ----------------------------

var _ Textual = new(columnString)

// columnString represents a string column
type columnString struct {
	fill bitmap.Bitmap // The fill-list
	data []string      // The actual values
}

// makeString creates a new string column
func makeStrings() Column {
	return &columnString{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]string, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnString) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]string, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnString) Apply(r *commit.Reader) {

	// Update the values of the column, for this one we can only process stores
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = string(r.Bytes())
		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
}

// Value retrieves a value at a specified index
func (c *columnString) Value(idx uint32) (v interface{}, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *columnString) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnString) Index() *bitmap.Bitmap {
	return &c.fill
}

// LoadString retrieves a value at a specified index
func (c *columnString) LoadString(idx uint32) (string, bool) {
	v, has := c.Value(idx)
	s, ok := v.(string)
	return s, has && ok
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnString) FilterString(offset uint32, index bitmap.Bitmap, predicate func(v string) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(c.data[idx])
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnString) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutString(commit.Put, idx, c.data[idx])
	})
}

// slice accessor for strings
type stringSlice struct {
	writer *commit.Buffer
	reader *columnString
}

// Set sets the value at the specified index
func (s stringSlice) Set(index uint32, value string) {
	s.writer.PutString(commit.Put, index, value)
}

// Get loads the value at a particular index
func (s stringSlice) Get(index uint32) (string, bool) {
	return s.reader.LoadString(index)
}

// String returns a string column accessor
func (txn *Txn) String(columnName string) stringSlice {
	writer := txn.bufferFor(columnName)
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*columnString)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type string", columnName))
	}

	return stringSlice{
		writer: writer,
		reader: reader,
	}
}
