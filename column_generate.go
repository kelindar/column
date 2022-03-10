//go:build ignore
// +build ignore

package column

import (
	"fmt"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/kelindar/genny/generic"
)

// --------------------------- Numbers ----------------------------

type number = generic.Number

// numberColumn represents a generic column
type numberColumn struct {
	fill bitmap.Bitmap // The fill-list
	data []number      // The actual values
}

// makeNumbers creates a new vector for Numbers
func makeNumbers() Column {
	return &numberColumn{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]number, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *numberColumn) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]number, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *numberColumn) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Number()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Number()
			c.data[r.Offset] = value
			r.SwapNumber(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
}

// Contains checks whether the column has a value at a specified index.
func (c *numberColumn) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *numberColumn) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *numberColumn) Value(idx uint32) (v interface{}, ok bool) {
	v = number(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a number value at a specified index
func (c *numberColumn) load(idx uint32) (v number, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = number(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *numberColumn) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *numberColumn) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *numberColumn) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *numberColumn) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *numberColumn) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *numberColumn) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *numberColumn) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutNumber(idx, c.data[idx])
	})
}

// numberReader represents a read-only accessor for number
type numberReader struct {
	cursor *uint32
	reader *numberColumn
}

// Get loads the value at the current transaction cursor
func (s numberReader) Get() (number, bool) {
	return s.reader.load(*s.cursor)
}

// numberReaderFor creates a new number reader
func numberReaderFor(txn *Txn, columnName string) numberReader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*numberColumn)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, number(0)))
	}

	return numberReader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// numberWriter represents a read-write accessor for number
type numberWriter struct {
	numberReader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s numberWriter) Set(value number) {
	s.writer.PutNumber(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s numberWriter) Add(delta number) {
	s.writer.AddNumber(*s.cursor, delta)
}

// Number returns a read-write accessor for number column
func (txn *Txn) Number(columnName string) numberWriter {
	return numberWriter{
		numberReader: numberReaderFor(txn, columnName),
		writer:       txn.bufferFor(columnName),
	}
}
