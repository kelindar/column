// Code generated with https://github.com/kelindar/genny DO NOT EDIT.
// Any changes will be lost if this file is regenerated.

package column

import (
	"fmt"

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
	clone := make([]float32, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnfloat32) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Float32()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Float32()
			c.data[r.Offset] = value
			r.SwapFloat32(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadFloat32 retrieves a float32 value at a specified index
func (c *columnfloat32) LoadFloat32(idx uint32) (v float32, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float32(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnfloat32 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnfloat32 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnfloat32 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnfloat32) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutFloat32(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetFloat32 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetFloat32(value float32) {
	cur.update.PutFloat32(cur.idx, value)
}

// AddFloat32 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddFloat32(amount float32) {
	cur.update.AddFloat32(cur.idx, amount)
}

// SetFloat32At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetFloat32At(column string, value float32) {
	cur.txn.bufferFor(column).PutFloat32(cur.idx, value)
}

// AddFloat32At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddFloat32At(column string, amount float32) {
	cur.txn.bufferFor(column).AddFloat32(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type float32Slice struct {
	Set func(uint32, float32)
	Get func(uint32) (float32, bool)
}

// Float32 returns a float32 column accessor
func (txn *Txn) Float32(columnName string) float32Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnfloat32)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(float32)))
	}

	return float32Slice{
		Set: writer.PutFloat32,
		Get: reader.LoadFloat32,
	}
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
	clone := make([]float64, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnfloat64) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Float64()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Float64()
			c.data[r.Offset] = value
			r.SwapFloat64(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnfloat64 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnfloat64 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnfloat64 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnfloat64) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutFloat64(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetFloat64 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetFloat64(value float64) {
	cur.update.PutFloat64(cur.idx, value)
}

// AddFloat64 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddFloat64(amount float64) {
	cur.update.AddFloat64(cur.idx, amount)
}

// SetFloat64At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetFloat64At(column string, value float64) {
	cur.txn.bufferFor(column).PutFloat64(cur.idx, value)
}

// AddFloat64At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddFloat64At(column string, amount float64) {
	cur.txn.bufferFor(column).AddFloat64(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type float64Slice struct {
	Set func(uint32, float64)
	Get func(uint32) (float64, bool)
}

// Float64 returns a float64 column accessor
func (txn *Txn) Float64(columnName string) float64Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnfloat64)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(float64)))
	}

	return float64Slice{
		Set: writer.PutFloat64,
		Get: reader.LoadFloat64,
	}
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
	clone := make([]int, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Int()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Int()
			c.data[r.Offset] = value
			r.SwapInt(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadInt retrieves a int value at a specified index
func (c *columnint) LoadInt(idx uint32) (v int, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnint ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnint ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnint ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnint) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetInt updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt(value int) {
	cur.update.PutInt(cur.idx, value)
}

// AddInt atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt(amount int) {
	cur.update.AddInt(cur.idx, amount)
}

// SetIntAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetIntAt(column string, value int) {
	cur.txn.bufferFor(column).PutInt(cur.idx, value)
}

// AddIntAt atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddIntAt(column string, amount int) {
	cur.txn.bufferFor(column).AddInt(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type intSlice struct {
	Set func(uint32, int)
	Get func(uint32) (int, bool)
}

// Int returns a int column accessor
func (txn *Txn) Int(columnName string) intSlice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnint)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(int)))
	}

	return intSlice{
		Set: writer.PutInt,
		Get: reader.LoadInt,
	}
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
	clone := make([]int16, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint16) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Int16()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Int16()
			c.data[r.Offset] = value
			r.SwapInt16(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadInt16 retrieves a int16 value at a specified index
func (c *columnint16) LoadInt16(idx uint32) (v int16, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int16(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnint16 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnint16 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnint16 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnint16) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt16(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetInt16 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt16(value int16) {
	cur.update.PutInt16(cur.idx, value)
}

// AddInt16 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt16(amount int16) {
	cur.update.AddInt16(cur.idx, amount)
}

// SetInt16At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt16At(column string, value int16) {
	cur.txn.bufferFor(column).PutInt16(cur.idx, value)
}

// AddInt16At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt16At(column string, amount int16) {
	cur.txn.bufferFor(column).AddInt16(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type int16Slice struct {
	Set func(uint32, int16)
	Get func(uint32) (int16, bool)
}

// Int16 returns a int16 column accessor
func (txn *Txn) Int16(columnName string) int16Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnint16)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(int16)))
	}

	return int16Slice{
		Set: writer.PutInt16,
		Get: reader.LoadInt16,
	}
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
	clone := make([]int32, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint32) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Int32()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Int32()
			c.data[r.Offset] = value
			r.SwapInt32(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadInt32 retrieves a int32 value at a specified index
func (c *columnint32) LoadInt32(idx uint32) (v int32, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int32(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnint32 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnint32 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnint32 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnint32) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt32(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetInt32 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt32(value int32) {
	cur.update.PutInt32(cur.idx, value)
}

// AddInt32 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt32(amount int32) {
	cur.update.AddInt32(cur.idx, amount)
}

// SetInt32At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt32At(column string, value int32) {
	cur.txn.bufferFor(column).PutInt32(cur.idx, value)
}

// AddInt32At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt32At(column string, amount int32) {
	cur.txn.bufferFor(column).AddInt32(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type int32Slice struct {
	Set func(uint32, int32)
	Get func(uint32) (int32, bool)
}

// Int32 returns a int32 column accessor
func (txn *Txn) Int32(columnName string) int32Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnint32)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(int32)))
	}

	return int32Slice{
		Set: writer.PutInt32,
		Get: reader.LoadInt32,
	}
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
	clone := make([]int64, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnint64) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Int64()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Int64()
			c.data[r.Offset] = value
			r.SwapInt64(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadInt64 retrieves a int64 value at a specified index
func (c *columnint64) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnint64 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnint64 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnint64 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnint64) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt64(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetInt64 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt64(value int64) {
	cur.update.PutInt64(cur.idx, value)
}

// AddInt64 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt64(amount int64) {
	cur.update.AddInt64(cur.idx, amount)
}

// SetInt64At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetInt64At(column string, value int64) {
	cur.txn.bufferFor(column).PutInt64(cur.idx, value)
}

// AddInt64At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddInt64At(column string, amount int64) {
	cur.txn.bufferFor(column).AddInt64(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type int64Slice struct {
	Set func(uint32, int64)
	Get func(uint32) (int64, bool)
}

// Int64 returns a int64 column accessor
func (txn *Txn) Int64(columnName string) int64Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnint64)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(int64)))
	}

	return int64Slice{
		Set: writer.PutInt64,
		Get: reader.LoadInt64,
	}
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
	clone := make([]uint, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Uint()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Uint()
			c.data[r.Offset] = value
			r.SwapUint(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadUint retrieves a uint value at a specified index
func (c *columnuint) LoadUint(idx uint32) (v uint, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnuint ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnuint ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnuint ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnuint) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetUint updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint(value uint) {
	cur.update.PutUint(cur.idx, value)
}

// AddUint atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint(amount uint) {
	cur.update.AddUint(cur.idx, amount)
}

// SetUintAt updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUintAt(column string, value uint) {
	cur.txn.bufferFor(column).PutUint(cur.idx, value)
}

// AddUintAt atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUintAt(column string, amount uint) {
	cur.txn.bufferFor(column).AddUint(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type uintSlice struct {
	Set func(uint32, uint)
	Get func(uint32) (uint, bool)
}

// Uint returns a uint column accessor
func (txn *Txn) Uint(columnName string) uintSlice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnuint)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(uint)))
	}

	return uintSlice{
		Set: writer.PutUint,
		Get: reader.LoadUint,
	}
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
	clone := make([]uint16, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint16) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Uint16()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Uint16()
			c.data[r.Offset] = value
			r.SwapUint16(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadUint16 retrieves a uint16 value at a specified index
func (c *columnuint16) LoadUint16(idx uint32) (v uint16, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint16(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnuint16 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnuint16 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnuint16 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnuint16) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint16(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetUint16 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint16(value uint16) {
	cur.update.PutUint16(cur.idx, value)
}

// AddUint16 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint16(amount uint16) {
	cur.update.AddUint16(cur.idx, amount)
}

// SetUint16At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint16At(column string, value uint16) {
	cur.txn.bufferFor(column).PutUint16(cur.idx, value)
}

// AddUint16At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint16At(column string, amount uint16) {
	cur.txn.bufferFor(column).AddUint16(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type uint16Slice struct {
	Set func(uint32, uint16)
	Get func(uint32) (uint16, bool)
}

// Uint16 returns a uint16 column accessor
func (txn *Txn) Uint16(columnName string) uint16Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnuint16)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(uint16)))
	}

	return uint16Slice{
		Set: writer.PutUint16,
		Get: reader.LoadUint16,
	}
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
	clone := make([]uint32, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint32) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Uint32()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Uint32()
			c.data[r.Offset] = value
			r.SwapUint32(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadUint32 retrieves a uint32 value at a specified index
func (c *columnuint32) LoadUint32(idx uint32) (v uint32, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint32(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnuint32 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnuint32 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnuint32 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnuint32) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint32(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetUint32 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint32(value uint32) {
	cur.update.PutUint32(cur.idx, value)
}

// AddUint32 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint32(amount uint32) {
	cur.update.AddUint32(cur.idx, amount)
}

// SetUint32At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint32At(column string, value uint32) {
	cur.txn.bufferFor(column).PutUint32(cur.idx, value)
}

// AddUint32At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint32At(column string, amount uint32) {
	cur.txn.bufferFor(column).AddUint32(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type uint32Slice struct {
	Set func(uint32, uint32)
	Get func(uint32) (uint32, bool)
}

// Uint32 returns a uint32 column accessor
func (txn *Txn) Uint32(columnName string) uint32Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnuint32)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(uint32)))
	}

	return uint32Slice{
		Set: writer.PutUint32,
		Get: reader.LoadUint32,
	}
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
	clone := make([]uint64, idx+1, resize(cap(c.data), idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnuint64) Apply(r *commit.Reader) {
	for r.Next() {
		switch r.Type {
		case commit.Put:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = r.Uint64()

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			value := c.data[r.Offset] + r.Uint64()
			c.data[r.Offset] = value
			r.SwapUint64(value)

		case commit.Delete:
			c.fill.Remove(r.Index())
		}
	}
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

// LoadUint64 retrieves a uint64 value at a specified index
func (c *columnuint64) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

/*
// LoadFloat64 retrieves a float64 value at a specified index
func ( c * columnuint64 ) LoadFloat64 ( idx uint32 ) ( v float64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func ( c * columnuint64 ) LoadInt64 ( idx uint32 ) ( v int64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func ( c * columnuint64 ) LoadUint64 ( idx uint32 ) ( v uint64 , ok bool ) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}
*/

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

// Snapshot writes the entire column into the specified destination buffer
func (c *columnuint64) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint64(idx, c.data[idx])
	})
}

// --------------------------- Cursor Update ----------------------------

// SetUint64 updates a column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint64(value uint64) {
	cur.update.PutUint64(cur.idx, value)
}

// AddUint64 atomically increments/decrements the current value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint64(amount uint64) {
	cur.update.AddUint64(cur.idx, amount)
}

// SetUint64At updates a specified column value for the current item. The actual operation
// will be queued and executed once the current the transaction completes.
func (cur *Cursor) SetUint64At(column string, value uint64) {
	cur.txn.bufferFor(column).PutUint64(cur.idx, value)
}

// AddUint64At atomically increments/decrements the column value by the specified amount. Note
// that this only works for numerical values and the type of the value must match.
func (cur *Cursor) AddUint64At(column string, amount uint64) {
	cur.txn.bufferFor(column).AddUint64(cur.idx, amount)
}

// --------------------------- Column Slicer ----------------------------

type uint64Slice struct {
	Set func(uint32, uint64)
	Get func(uint32) (uint64, bool)
}

// Uint64 returns a uint64 column accessor
func (txn *Txn) Uint64(columnName string) uint64Slice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnuint64)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type %T ", columnName, new(uint64)))
	}

	return uint64Slice{
		Set: writer.PutUint64,
		Get: reader.LoadUint64,
	}
}
