// Code generated with https://github.com/kelindar/genny DO NOT EDIT.
// Any changes will be lost if this file is regenerated.

package column

import (
	"fmt"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Float32s ----------------------------

// float32Column represents a generic column
type float32Column struct {
	fill bitmap.Bitmap // The fill-list
	data []float32     // The actual values
}

// makeFloat32s creates a new vector for Float32s
func makeFloat32s() Column {
	return &float32Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]float32, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *float32Column) Grow(idx uint32) {
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
func (c *float32Column) Apply(r *commit.Reader) {
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
func (c *float32Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *float32Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *float32Column) Value(idx uint32) (v interface{}, ok bool) {
	v = float32(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a float32 value at a specified index
func (c *float32Column) load(idx uint32) (v float32, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float32(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *float32Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *float32Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *float32Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *float32Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *float32Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *float32Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *float32Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutFloat32(idx, c.data[idx])
	})
}

// float32Reader represents a read-only accessor for float32
type float32Reader struct {
	cursor *uint32
	reader *float32Column
}

// Get loads the value at the current transaction cursor
func (s float32Reader) Get() (float32, bool) {
	return s.reader.load(*s.cursor)
}

// float32ReaderFor creates a new float32 reader
func float32ReaderFor(txn *Txn, columnName string) float32Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*float32Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, float32(0)))
	}

	return float32Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// float32Writer represents a read-write accessor for float32
type float32Writer struct {
	float32Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s float32Writer) Set(value float32) {
	s.writer.PutFloat32(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s float32Writer) Add(delta float32) {
	s.writer.AddFloat32(*s.cursor, delta)
}

// Float32 returns a read-write accessor for float32 column
func (txn *Txn) Float32(columnName string) float32Writer {
	return float32Writer{
		float32Reader: float32ReaderFor(txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}

// --------------------------- Float64s ----------------------------

// float64Column represents a generic column
type float64Column struct {
	fill bitmap.Bitmap // The fill-list
	data []float64     // The actual values
}

// makeFloat64s creates a new vector for Float64s
func makeFloat64s() Column {
	return &float64Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]float64, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *float64Column) Grow(idx uint32) {
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
func (c *float64Column) Apply(r *commit.Reader) {
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
func (c *float64Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *float64Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *float64Column) Value(idx uint32) (v interface{}, ok bool) {
	v = float64(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a float64 value at a specified index
func (c *float64Column) load(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *float64Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *float64Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *float64Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *float64Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *float64Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *float64Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *float64Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutFloat64(idx, c.data[idx])
	})
}

// float64Reader represents a read-only accessor for float64
type float64Reader struct {
	cursor *uint32
	reader *float64Column
}

// Get loads the value at the current transaction cursor
func (s float64Reader) Get() (float64, bool) {
	return s.reader.load(*s.cursor)
}

// float64ReaderFor creates a new float64 reader
func float64ReaderFor(txn *Txn, columnName string) float64Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*float64Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, float64(0)))
	}

	return float64Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// float64Writer represents a read-write accessor for float64
type float64Writer struct {
	float64Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s float64Writer) Set(value float64) {
	s.writer.PutFloat64(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s float64Writer) Add(delta float64) {
	s.writer.AddFloat64(*s.cursor, delta)
}

// Float64 returns a read-write accessor for float64 column
func (txn *Txn) Float64(columnName string) float64Writer {
	return float64Writer{
		float64Reader: float64ReaderFor(txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}

// --------------------------- Ints ----------------------------

// intColumn represents a generic column
type intColumn struct {
	fill bitmap.Bitmap // The fill-list
	data []int         // The actual values
}

// makeInts creates a new vector for Ints
func makeInts() Column {
	return &intColumn{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *intColumn) Grow(idx uint32) {
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
func (c *intColumn) Apply(r *commit.Reader) {
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
func (c *intColumn) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *intColumn) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *intColumn) Value(idx uint32) (v interface{}, ok bool) {
	v = int(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a int value at a specified index
func (c *intColumn) load(idx uint32) (v int, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *intColumn) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *intColumn) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *intColumn) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *intColumn) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *intColumn) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *intColumn) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *intColumn) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt(idx, c.data[idx])
	})
}

// intReader represents a read-only accessor for int
type intReader struct {
	cursor *uint32
	reader *intColumn
}

// Get loads the value at the current transaction cursor
func (s intReader) Get() (int, bool) {
	return s.reader.load(*s.cursor)
}

// intReaderFor creates a new int reader
func intReaderFor(txn *Txn, columnName string) intReader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*intColumn)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, int(0)))
	}

	return intReader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// intWriter represents a read-write accessor for int
type intWriter struct {
	intReader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s intWriter) Set(value int) {
	s.writer.PutInt(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s intWriter) Add(delta int) {
	s.writer.AddInt(*s.cursor, delta)
}

// Int returns a read-write accessor for int column
func (txn *Txn) Int(columnName string) intWriter {
	return intWriter{
		intReader: intReaderFor(txn, columnName),
		writer:    txn.bufferFor(columnName),
	}
}

// --------------------------- Int16s ----------------------------

// int16Column represents a generic column
type int16Column struct {
	fill bitmap.Bitmap // The fill-list
	data []int16       // The actual values
}

// makeInt16s creates a new vector for Int16s
func makeInt16s() Column {
	return &int16Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int16, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *int16Column) Grow(idx uint32) {
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
func (c *int16Column) Apply(r *commit.Reader) {
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
func (c *int16Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *int16Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *int16Column) Value(idx uint32) (v interface{}, ok bool) {
	v = int16(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a int16 value at a specified index
func (c *int16Column) load(idx uint32) (v int16, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int16(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *int16Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *int16Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *int16Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *int16Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *int16Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *int16Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *int16Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt16(idx, c.data[idx])
	})
}

// int16Reader represents a read-only accessor for int16
type int16Reader struct {
	cursor *uint32
	reader *int16Column
}

// Get loads the value at the current transaction cursor
func (s int16Reader) Get() (int16, bool) {
	return s.reader.load(*s.cursor)
}

// int16ReaderFor creates a new int16 reader
func int16ReaderFor(txn *Txn, columnName string) int16Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*int16Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, int16(0)))
	}

	return int16Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// int16Writer represents a read-write accessor for int16
type int16Writer struct {
	int16Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s int16Writer) Set(value int16) {
	s.writer.PutInt16(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s int16Writer) Add(delta int16) {
	s.writer.AddInt16(*s.cursor, delta)
}

// Int16 returns a read-write accessor for int16 column
func (txn *Txn) Int16(columnName string) int16Writer {
	return int16Writer{
		int16Reader: int16ReaderFor(txn, columnName),
		writer:      txn.bufferFor(columnName),
	}
}

// --------------------------- Int32s ----------------------------

// int32Column represents a generic column
type int32Column struct {
	fill bitmap.Bitmap // The fill-list
	data []int32       // The actual values
}

// makeInt32s creates a new vector for Int32s
func makeInt32s() Column {
	return &int32Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int32, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *int32Column) Grow(idx uint32) {
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
func (c *int32Column) Apply(r *commit.Reader) {
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
func (c *int32Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *int32Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *int32Column) Value(idx uint32) (v interface{}, ok bool) {
	v = int32(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a int32 value at a specified index
func (c *int32Column) load(idx uint32) (v int32, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int32(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *int32Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *int32Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *int32Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *int32Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *int32Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *int32Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *int32Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt32(idx, c.data[idx])
	})
}

// int32Reader represents a read-only accessor for int32
type int32Reader struct {
	cursor *uint32
	reader *int32Column
}

// Get loads the value at the current transaction cursor
func (s int32Reader) Get() (int32, bool) {
	return s.reader.load(*s.cursor)
}

// int32ReaderFor creates a new int32 reader
func int32ReaderFor(txn *Txn, columnName string) int32Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*int32Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, int32(0)))
	}

	return int32Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// int32Writer represents a read-write accessor for int32
type int32Writer struct {
	int32Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s int32Writer) Set(value int32) {
	s.writer.PutInt32(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s int32Writer) Add(delta int32) {
	s.writer.AddInt32(*s.cursor, delta)
}

// Int32 returns a read-write accessor for int32 column
func (txn *Txn) Int32(columnName string) int32Writer {
	return int32Writer{
		int32Reader: int32ReaderFor(txn, columnName),
		writer:      txn.bufferFor(columnName),
	}
}

// --------------------------- Int64s ----------------------------

// int64Column represents a generic column
type int64Column struct {
	fill bitmap.Bitmap // The fill-list
	data []int64       // The actual values
}

// makeInt64s creates a new vector for Int64s
func makeInt64s() Column {
	return &int64Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]int64, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *int64Column) Grow(idx uint32) {
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
func (c *int64Column) Apply(r *commit.Reader) {
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
func (c *int64Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *int64Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *int64Column) Value(idx uint32) (v interface{}, ok bool) {
	v = int64(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a int64 value at a specified index
func (c *int64Column) load(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *int64Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *int64Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *int64Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *int64Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *int64Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *int64Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *int64Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutInt64(idx, c.data[idx])
	})
}

// int64Reader represents a read-only accessor for int64
type int64Reader struct {
	cursor *uint32
	reader *int64Column
}

// Get loads the value at the current transaction cursor
func (s int64Reader) Get() (int64, bool) {
	return s.reader.load(*s.cursor)
}

// int64ReaderFor creates a new int64 reader
func int64ReaderFor(txn *Txn, columnName string) int64Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*int64Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, int64(0)))
	}

	return int64Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// int64Writer represents a read-write accessor for int64
type int64Writer struct {
	int64Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s int64Writer) Set(value int64) {
	s.writer.PutInt64(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s int64Writer) Add(delta int64) {
	s.writer.AddInt64(*s.cursor, delta)
}

// Int64 returns a read-write accessor for int64 column
func (txn *Txn) Int64(columnName string) int64Writer {
	return int64Writer{
		int64Reader: int64ReaderFor(txn, columnName),
		writer:      txn.bufferFor(columnName),
	}
}

// --------------------------- Uints ----------------------------

// uintColumn represents a generic column
type uintColumn struct {
	fill bitmap.Bitmap // The fill-list
	data []uint        // The actual values
}

// makeUints creates a new vector for Uints
func makeUints() Column {
	return &uintColumn{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *uintColumn) Grow(idx uint32) {
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
func (c *uintColumn) Apply(r *commit.Reader) {
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
func (c *uintColumn) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *uintColumn) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *uintColumn) Value(idx uint32) (v interface{}, ok bool) {
	v = uint(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a uint value at a specified index
func (c *uintColumn) load(idx uint32) (v uint, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *uintColumn) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *uintColumn) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *uintColumn) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *uintColumn) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *uintColumn) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *uintColumn) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *uintColumn) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint(idx, c.data[idx])
	})
}

// uintReader represents a read-only accessor for uint
type uintReader struct {
	cursor *uint32
	reader *uintColumn
}

// Get loads the value at the current transaction cursor
func (s uintReader) Get() (uint, bool) {
	return s.reader.load(*s.cursor)
}

// uintReaderFor creates a new uint reader
func uintReaderFor(txn *Txn, columnName string) uintReader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*uintColumn)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, uint(0)))
	}

	return uintReader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// uintWriter represents a read-write accessor for uint
type uintWriter struct {
	uintReader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uintWriter) Set(value uint) {
	s.writer.PutUint(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s uintWriter) Add(delta uint) {
	s.writer.AddUint(*s.cursor, delta)
}

// Uint returns a read-write accessor for uint column
func (txn *Txn) Uint(columnName string) uintWriter {
	return uintWriter{
		uintReader: uintReaderFor(txn, columnName),
		writer:     txn.bufferFor(columnName),
	}
}

// --------------------------- Uint16s ----------------------------

// uint16Column represents a generic column
type uint16Column struct {
	fill bitmap.Bitmap // The fill-list
	data []uint16      // The actual values
}

// makeUint16s creates a new vector for Uint16s
func makeUint16s() Column {
	return &uint16Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint16, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *uint16Column) Grow(idx uint32) {
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
func (c *uint16Column) Apply(r *commit.Reader) {
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
func (c *uint16Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *uint16Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *uint16Column) Value(idx uint32) (v interface{}, ok bool) {
	v = uint16(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a uint16 value at a specified index
func (c *uint16Column) load(idx uint32) (v uint16, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint16(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *uint16Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *uint16Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *uint16Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *uint16Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *uint16Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *uint16Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *uint16Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint16(idx, c.data[idx])
	})
}

// uint16Reader represents a read-only accessor for uint16
type uint16Reader struct {
	cursor *uint32
	reader *uint16Column
}

// Get loads the value at the current transaction cursor
func (s uint16Reader) Get() (uint16, bool) {
	return s.reader.load(*s.cursor)
}

// uint16ReaderFor creates a new uint16 reader
func uint16ReaderFor(txn *Txn, columnName string) uint16Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*uint16Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, uint16(0)))
	}

	return uint16Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// uint16Writer represents a read-write accessor for uint16
type uint16Writer struct {
	uint16Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uint16Writer) Set(value uint16) {
	s.writer.PutUint16(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s uint16Writer) Add(delta uint16) {
	s.writer.AddUint16(*s.cursor, delta)
}

// Uint16 returns a read-write accessor for uint16 column
func (txn *Txn) Uint16(columnName string) uint16Writer {
	return uint16Writer{
		uint16Reader: uint16ReaderFor(txn, columnName),
		writer:       txn.bufferFor(columnName),
	}
}

// --------------------------- Uint32s ----------------------------

// uint32Column represents a generic column
type uint32Column struct {
	fill bitmap.Bitmap // The fill-list
	data []uint32      // The actual values
}

// makeUint32s creates a new vector for Uint32s
func makeUint32s() Column {
	return &uint32Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint32, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *uint32Column) Grow(idx uint32) {
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
func (c *uint32Column) Apply(r *commit.Reader) {
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
func (c *uint32Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *uint32Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *uint32Column) Value(idx uint32) (v interface{}, ok bool) {
	v = uint32(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a uint32 value at a specified index
func (c *uint32Column) load(idx uint32) (v uint32, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint32(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *uint32Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *uint32Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *uint32Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *uint32Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *uint32Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *uint32Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *uint32Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint32(idx, c.data[idx])
	})
}

// uint32Reader represents a read-only accessor for uint32
type uint32Reader struct {
	cursor *uint32
	reader *uint32Column
}

// Get loads the value at the current transaction cursor
func (s uint32Reader) Get() (uint32, bool) {
	return s.reader.load(*s.cursor)
}

// uint32ReaderFor creates a new uint32 reader
func uint32ReaderFor(txn *Txn, columnName string) uint32Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*uint32Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, uint32(0)))
	}

	return uint32Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// uint32Writer represents a read-write accessor for uint32
type uint32Writer struct {
	uint32Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uint32Writer) Set(value uint32) {
	s.writer.PutUint32(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s uint32Writer) Add(delta uint32) {
	s.writer.AddUint32(*s.cursor, delta)
}

// Uint32 returns a read-write accessor for uint32 column
func (txn *Txn) Uint32(columnName string) uint32Writer {
	return uint32Writer{
		uint32Reader: uint32ReaderFor(txn, columnName),
		writer:       txn.bufferFor(columnName),
	}
}

// --------------------------- Uint64s ----------------------------

// uint64Column represents a generic column
type uint64Column struct {
	fill bitmap.Bitmap // The fill-list
	data []uint64      // The actual values
}

// makeUint64s creates a new vector for Uint64s
func makeUint64s() Column {
	return &uint64Column{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]uint64, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *uint64Column) Grow(idx uint32) {
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
func (c *uint64Column) Apply(r *commit.Reader) {
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
func (c *uint64Column) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *uint64Column) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *uint64Column) Value(idx uint32) (v interface{}, ok bool) {
	v = uint64(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// load retrieves a uint64 value at a specified index
func (c *uint64Column) load(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *uint64Column) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *uint64Column) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *uint64Column) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *uint64Column) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *uint64Column) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *uint64Column) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}

// Snapshot writes the entire column into the specified destination buffer
func (c *uint64Column) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	chunk.Range(c.fill, func(idx uint32) {
		dst.PutUint64(idx, c.data[idx])
	})
}

// uint64Reader represents a read-only accessor for uint64
type uint64Reader struct {
	cursor *uint32
	reader *uint64Column
}

// Get loads the value at the current transaction cursor
func (s uint64Reader) Get() (uint64, bool) {
	return s.reader.load(*s.cursor)
}

// uint64ReaderFor creates a new uint64 reader
func uint64ReaderFor(txn *Txn, columnName string) uint64Reader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*uint64Column)
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, uint64(0)))
	}

	return uint64Reader{
		cursor: &txn.cursor,
		reader: reader,
	}
}

// uint64Writer represents a read-write accessor for uint64
type uint64Writer struct {
	uint64Reader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uint64Writer) Set(value uint64) {
	s.writer.PutUint64(*s.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s uint64Writer) Add(delta uint64) {
	s.writer.AddUint64(*s.cursor, delta)
}

// Uint64 returns a read-write accessor for uint64 column
func (txn *Txn) Uint64(columnName string) uint64Writer {
	return uint64Writer{
		uint64Reader: uint64ReaderFor(txn, columnName),
		writer:       txn.bufferFor(columnName),
	}
}
