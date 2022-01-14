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

// columnNumber represents a generic column
type columnNumber struct {
	fill bitmap.Bitmap // The fill-list
	data []number      // The actual values
}

// makeNumbers creates a new vector for Numbers
func makeNumbers() Column {
	return &columnNumber{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]number, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnNumber) Grow(idx uint32) {
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
func (c *columnNumber) Apply(r *commit.Reader) {
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
func (c *columnNumber) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnNumber) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnNumber) Value(idx uint32) (v interface{}, ok bool) {
	v = number(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadNumber retrieves a number value at a specified index
func (c *columnNumber) LoadNumber(idx uint32) (v number, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = number(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnNumber) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnNumber) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnNumber) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnNumber) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutNumber(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetNumber updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetNumber(value number) {
	cur.update.PutNumber(cur.idx, value)
}

// AddNumber atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddNumber(amount number) {
	cur.update.AddNumber(cur.idx, amount)
}

// SetNumberAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetNumberAt(column string, value number) {
	cur.txn.bufferFor(column).PutNumber(cur.idx, value)
}

// AddNumberAt atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddNumberAt(column string, amount number) {
	cur.txn.bufferFor(column).AddNumber(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type NumberSlice struct {
	Set func(uint32, number)
	Get func(uint32) (number, bool)
}

// Number returns a number column accessor
func (txn *Txn) Number(columnName string) numberSlice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnNumber)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(number)))
	}

	return NumberSlice{
		Set: writer.PutNumber,
		Get: reader.LoadNumber,
	}
}
