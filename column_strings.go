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
	chunks[uint32]
	seek *intmap.Sync // The hash->location table
	data []string     // The string data
}

// makeEnum creates a new column
func makeEnum() Column {
	return &columnEnum{
		chunks: make(chunks[uint32], 0, 4),
		seek:   intmap.NewSync(64, .95),
		data:   make([]string, 0, 64),
	}
}

// Apply applies a set of operations to the column.
func (c *columnEnum) Apply(chunk commit.Chunk, r *commit.Reader) {
	fill, locs := c.chunkAt(chunk)
	for r.Next() {
		offset := r.IndexAtChunk()
		switch r.Type {
		case commit.Put:
			fill[offset>>6] |= 1 << (offset & 0x3f)
			locs[offset] = c.findOrAdd(r.Bytes())
		case commit.Delete:
			fill.Remove(offset)
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
	chunk := commit.ChunkAt(idx)
	index := idx - chunk.Min()
	if int(chunk) < len(c.chunks) && c.chunks[chunk].fill.Contains(index) {
		v, ok = c.readAt(c.chunks[chunk].data[idx]), true
	}
	return
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnEnum) FilterString(chunk commit.Chunk, index bitmap.Bitmap, predicate func(v string) bool) {
	if int(chunk) >= len(c.chunks) {
		return
	}

	fill, locs := c.chunkAt(chunk)
	cache := struct {
		index uint32 // Last seen offset
		value bool   // Last evaluated predicate
	}{
		index: math.MaxUint32,
		value: false,
	}

	// Do a quick ellimination of elements which are NOT contained in this column, this
	// allows us not to check contains during the filter itself
	index.And(fill)

	// Filters down the strings, if strings repeat we avoid reading every time by
	// caching the last seen index/value combination.
	index.Filter(func(idx uint32) bool {
		if at := locs[idx]; at != cache.index {
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
	chunk := commit.ChunkAt(idx)
	return c.chunks[chunk].fill.Contains(idx - chunk.Min())
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnEnum) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	fill, locs := c.chunkAt(chunk)
	fill.Range(func(idx uint32) {
		dst.PutString(commit.Put, idx, c.readAt(locs[idx]))
	})
}

// enumReader represents a read-only accessor for enum strings
type enumReader struct {
	cursor *uint32
	reader *columnEnum
}

// Get loads the value at the current transaction cursor
func (s enumReader) Get() (string, bool) {
	return s.reader.LoadString(*s.cursor)
}

// enumReaderFor creates a new enum string reader
func enumReaderFor(txn *Txn, columnName string) enumReader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*columnEnum)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type string", columnName))
	}

	return enumReader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// slice accessor for enums
type enumSlice struct {
	enumReader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s enumSlice) Set(value string) {
	s.writer.PutString(commit.Put, *s.cursor, value)
}

// Enum returns a enumerable column accessor
func (txn *Txn) Enum(columnName string) enumSlice {
	return enumSlice{
		enumReader: enumReaderFor(txn, columnName),
		writer:     txn.bufferFor(columnName),
	}
}

// --------------------------- String ----------------------------

var _ Textual = new(columnString)

// columnString represents a string column
type columnString struct {
	chunks[string]
}

// makeString creates a new string column
func makeStrings() Column {
	return &columnString{
		chunks: make(chunks[string], 0, 4),
	}
}

// Apply applies a set of operations to the column.
func (c *columnString) Apply(chunk commit.Chunk, r *commit.Reader) {
	fill, data := c.chunkAt(chunk)
	from := chunk.Min()

	// Update the values of the column, for this one we can only process stores
	for r.Next() {
		offset := r.Offset - int32(from)
		switch r.Type {
		case commit.Put:
			fill[offset>>6] |= 1 << (offset & 0x3f)
			data[offset] = string(r.Bytes())
		case commit.Delete:
			fill.Remove(uint32(offset))
		}
	}
}

// Value retrieves a value at a specified index
func (c *columnString) Value(idx uint32) (v interface{}, ok bool) {
	chunk := commit.ChunkAt(idx)
	index := idx - chunk.Min()

	if int(chunk) < len(c.chunks) && c.chunks[chunk].fill.Contains(index) {
		v, ok = c.chunks[chunk].data[index], true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *columnString) Contains(idx uint32) bool {
	chunk := commit.ChunkAt(idx)
	index := idx - chunk.Min()
	return c.chunks[chunk].fill.Contains(index)

}

// LoadString retrieves a value at a specified index
func (c *columnString) LoadString(idx uint32) (string, bool) {
	v, has := c.Value(idx)
	s, ok := v.(string)
	return s, has && ok
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnString) FilterString(chunk commit.Chunk, index bitmap.Bitmap, predicate func(v string) bool) {
	if int(chunk) < len(c.chunks) {
		fill, data := c.chunkAt(chunk)
		index.And(fill)
		index.Filter(func(idx uint32) bool {
			return predicate(data[idx])
		})
	}
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnString) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	fill, data := c.chunkAt(chunk)
	fill.Range(func(x uint32) {
		dst.PutString(commit.Put, chunk.Min()+x, data[x])
	})
}

// stringReader represents a read-only accessor for strings
type stringReader struct {
	cursor *uint32
	reader *columnString
}

// Get loads the value at the current transaction cursor
func (s stringReader) Get() (string, bool) {
	return s.reader.LoadString(*s.cursor)
}

// stringReaderFor creates a new string reader
func stringReaderFor(txn *Txn, columnName string) stringReader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*columnString)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type string", columnName))
	}

	return stringReader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// stringWriter represents read-write accessor for strings
type stringWriter struct {
	stringReader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s stringWriter) Set(value string) {
	s.writer.PutString(commit.Put, *s.cursor, value)
}

// String returns a string column accessor
func (txn *Txn) String(columnName string) stringWriter {
	return stringWriter{
		stringReader: stringReaderFor(txn, columnName),
		writer:       txn.bufferFor(columnName),
	}
}
