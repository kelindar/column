// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"

	"github.com/kelindar/column/commit"
)

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
			chunks: make(chunks[string], 0, 4),
		},
	}
}

// Apply applies a set of operations to the column.
func (c *columnKey) Apply(chunk commit.Chunk, r *commit.Reader) {
	fill, data := c.chunkAt(chunk)
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
