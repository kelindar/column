// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"

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

// --------------------------- Key ----------------------------

// columnKey represents the primary key column implementation
type columnKey struct {
	columnString
	name string            // Name of the column
	lock sync.RWMutex      // Lock to protect the lookup table
	seek map[string]uint32 // Lookup table for O(1) index seek
}

// makeKey creates a new primary key column
func makeKey() Column {
	return &columnKey{
		seek: make(map[string]uint32, 64),
		columnString: columnString{
			data: make([]segment[string], 0, 4),
		},
	}
}

// Apply applies a set of operations to the column.
func (c *columnKey) Apply(chunk commit.Chunk, r *commit.Reader) {
	fill := c.data[chunk].fill
	data := c.data[chunk].data
	from := chunk.Min()

	for r.Next() {
		offset := r.Offset - int32(from)
		switch r.Type {
		case commit.Put:
			value := string(r.Bytes())

			fill[offset>>6] |= 1 << (offset & 0x3f)
			data[offset] = value
			c.lock.Lock()
			c.seek[value] = uint32(r.Offset)
			c.lock.Unlock()

		case commit.Delete:
			fill.Remove(uint32(offset))
			c.lock.Lock()
			delete(c.seek, string(data[offset]))
			c.lock.Unlock()
		}
	}
}

// OffsetOf returns the offset for a particular value
func (c *columnKey) OffsetOf(v string) (uint32, bool) {
	c.lock.RLock()
	idx, ok := c.seek[v]
	c.lock.RUnlock()
	return idx, ok
}

// slice accessor for keys
type keySlice struct {
	cursor *uint32
	writer *commit.Buffer
	reader *columnKey
}

// Set sets the value at the current transaction index
func (s keySlice) Set(value string) {
	s.writer.PutString(commit.Put, *s.cursor, value)
}

// Get loads the value at the current transaction index
func (s keySlice) Get() (string, bool) {
	return s.reader.LoadString(*s.cursor)
}

// Enum returns a enumerable column accessor
func (txn *Txn) Key() keySlice {
	if txn.owner.pk == nil {
		panic(fmt.Errorf("column: primary key column does not exist"))
	}

	return keySlice{
		cursor: &txn.cursor,
		writer: txn.bufferFor(txn.owner.pk.name),
		reader: txn.owner.pk,
	}
}
