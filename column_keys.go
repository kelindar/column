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
	numericColumn[int64]
	name string           // Name of the column
	lock sync.RWMutex     // Lock to protect the lookup table
	seek map[int64]uint32 // Lookup table for O(1) index seek
}

// makeKey creates a new primary key column
func makeKey() Column {
	col := makeInt64s().(*numericColumn[int64])
	return &columnKey{
		seek:          make(map[int64]uint32, 64),
		numericColumn: *col,
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
			value := r.Int64()

			fill[offset>>6] |= 1 << (offset & 0x3f)
			data[offset] = value
			c.lock.Lock()
			c.seek[value] = uint32(r.Offset)
			c.lock.Unlock()

		case commit.Delete:
			fill.Remove(uint32(offset))
			c.lock.Lock()
			delete(c.seek, data[offset])
			c.lock.Unlock()
		}
	}
}

// OffsetOf returns the offset for a particular value
func (c *columnKey) OffsetOf(v int64) (uint32, bool) {
	c.lock.RLock()
	idx, ok := c.seek[v]
	c.lock.RUnlock()
	return idx, ok
}

// rwKey represents read-write accessor for primary keys.
type rwKey struct {
	cursor *uint32
	writer *commit.Buffer
	reader *columnKey
}

// Set sets the value at the current transaction index
func (s rwKey) Set(value int64) error {
	if _, ok := s.reader.OffsetOf(value); !ok {
		s.writer.PutInt64(commit.Put, *s.cursor, value)
		return nil
	}

	return fmt.Errorf("column: unable to set duplicate key '%d'", value)
}

// Get loads the value at the current transaction index
func (s rwKey) Get() (int64, bool) {
	return s.reader.LoadInt64(*s.cursor)
}

// Enum returns a enumerable column accessor
func (txn *Txn) Key() rwKey {
	if txn.owner.pk == nil {
		panic(fmt.Errorf("column: primary key column does not exist"))
	}

	return rwKey{
		cursor: &txn.cursor,
		writer: txn.bufferFor(txn.owner.pk.name),
		reader: txn.owner.pk,
	}
}
