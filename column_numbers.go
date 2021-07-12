// Code generated with https://github.com/kelindar/genny DO NOT EDIT.
// Any changes will be lost if this file is regenerated.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Float32s ----------------------------

// columnFloat32 represents a generic column
type columnfloat32 struct {
	fill bitmap.Bitmap // The fill-list
	data []float32     // The actual values
}

// makeFloat32s creates a new vector for Float32s
func makeFloat32s() Column {
	return &columnfloat32{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]float32, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnfloat32) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]float32, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnfloat32) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = float32(r.Float32())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + float32(r.Float32())
			c.data[r.Offset] = value
			r.SwapFloat32(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnfloat32) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnfloat32) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnfloat32) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnfloat32) Value(idx uint32) (v interface{}, ok bool) {
	v = float32(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnfloat32) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnfloat32) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnfloat32) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnfloat32) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnfloat32) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnfloat32) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Float64s ----------------------------

// columnFloat64 represents a generic column
type columnfloat64 struct {
	fill bitmap.Bitmap // The fill-list
	data []float64     // The actual values
}

// makeFloat64s creates a new vector for Float64s
func makeFloat64s() Column {
	return &columnfloat64{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]float64, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnfloat64) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]float64, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnfloat64) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = float64(r.Float64())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + float64(r.Float64())
			c.data[r.Offset] = value
			r.SwapFloat64(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnfloat64) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnfloat64) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnfloat64) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnfloat64) Value(idx uint32) (v interface{}, ok bool) {
	v = float64(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnfloat64) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnfloat64) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnfloat64) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnfloat64) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnfloat64) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnfloat64) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Ints ----------------------------

// columnInt represents a generic column
type columnint struct {
	fill bitmap.Bitmap // The fill-list
	data []int         // The actual values
}

// makeInts creates a new vector for Ints
func makeInts() Column {
	return &columnint{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnint) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]int, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = int(r.Int())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + int(r.Int())
			c.data[r.Offset] = value
			r.SwapInt(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnint) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnint) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnint) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnint) Value(idx uint32) (v interface{}, ok bool) {
	v = int(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnint) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnint) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnint) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnint) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnint) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnint) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Int16s ----------------------------

// columnInt16 represents a generic column
type columnint16 struct {
	fill bitmap.Bitmap // The fill-list
	data []int16       // The actual values
}

// makeInt16s creates a new vector for Int16s
func makeInt16s() Column {
	return &columnint16{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int16, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnint16) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]int16, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint16) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = int16(r.Int16())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + int16(r.Int16())
			c.data[r.Offset] = value
			r.SwapInt16(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnint16) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnint16) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnint16) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnint16) Value(idx uint32) (v interface{}, ok bool) {
	v = int16(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnint16) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnint16) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnint16) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnint16) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnint16) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnint16) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Int32s ----------------------------

// columnInt32 represents a generic column
type columnint32 struct {
	fill bitmap.Bitmap // The fill-list
	data []int32       // The actual values
}

// makeInt32s creates a new vector for Int32s
func makeInt32s() Column {
	return &columnint32{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int32, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnint32) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]int32, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint32) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = int32(r.Int32())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + int32(r.Int32())
			c.data[r.Offset] = value
			r.SwapInt32(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnint32) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnint32) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnint32) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnint32) Value(idx uint32) (v interface{}, ok bool) {
	v = int32(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnint32) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnint32) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnint32) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnint32) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnint32) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnint32) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Int64s ----------------------------

// columnInt64 represents a generic column
type columnint64 struct {
	fill bitmap.Bitmap // The fill-list
	data []int64       // The actual values
}

// makeInt64s creates a new vector for Int64s
func makeInt64s() Column {
	return &columnint64{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int64, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnint64) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]int64, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint64) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = int64(r.Int64())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + int64(r.Int64())
			c.data[r.Offset] = value
			r.SwapInt64(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnint64) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnint64) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnint64) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnint64) Value(idx uint32) (v interface{}, ok bool) {
	v = int64(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnint64) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnint64) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnint64) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnint64) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnint64) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnint64) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Uints ----------------------------

// columnUint represents a generic column
type columnuint struct {
	fill bitmap.Bitmap // The fill-list
	data []uint        // The actual values
}

// makeUints creates a new vector for Uints
func makeUints() Column {
	return &columnuint{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnuint) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = uint(r.Uint())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + uint(r.Uint())
			c.data[r.Offset] = value
			r.SwapUint(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnuint) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnuint) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnuint) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnuint) Value(idx uint32) (v interface{}, ok bool) {
	v = uint(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnuint) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnuint) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnuint) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnuint) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnuint) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnuint) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Uint16s ----------------------------

// columnUint16 represents a generic column
type columnuint16 struct {
	fill bitmap.Bitmap // The fill-list
	data []uint16      // The actual values
}

// makeUint16s creates a new vector for Uint16s
func makeUint16s() Column {
	return &columnuint16{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint16, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnuint16) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint16, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint16) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = uint16(r.Uint16())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + uint16(r.Uint16())
			c.data[r.Offset] = value
			r.SwapUint16(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnuint16) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnuint16) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnuint16) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnuint16) Value(idx uint32) (v interface{}, ok bool) {
	v = uint16(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnuint16) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnuint16) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnuint16) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnuint16) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnuint16) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnuint16) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Uint32s ----------------------------

// columnUint32 represents a generic column
type columnuint32 struct {
	fill bitmap.Bitmap // The fill-list
	data []uint32      // The actual values
}

// makeUint32s creates a new vector for Uint32s
func makeUint32s() Column {
	return &columnuint32{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint32, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnuint32) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint32, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint32) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = uint32(r.Uint32())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + uint32(r.Uint32())
			c.data[r.Offset] = value
			r.SwapUint32(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnuint32) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnuint32) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnuint32) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnuint32) Value(idx uint32) (v interface{}, ok bool) {
	v = uint32(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnuint32) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnuint32) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnuint32) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnuint32) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnuint32) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnuint32) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// --------------------------- Uint64s ----------------------------

// columnUint64 represents a generic column
type columnuint64 struct {
	fill bitmap.Bitmap // The fill-list
	data []uint64      // The actual values
}

// makeUint64s creates a new vector for Uint64s
func makeUint64s() Column {
	return &columnuint64{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint64, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnuint64) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint64, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint64) Apply(r *commit.Reader) {
	for r.Next() {
		c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
		switch r.Type {
		case commit.Put:
			c.data[r.Offset] = uint64(r.Uint64())

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[r.Offset] + uint64(r.Uint64())
			c.data[r.Offset] = value
			r.SwapUint64(value)
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnuint64) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnuint64) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnuint64) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnuint64) Value(idx uint32) (v interface{}, ok bool) {
	v = uint64(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnuint64) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnuint64) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnuint64) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnuint64) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnuint64) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnuint64) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}
