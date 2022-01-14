// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=column -in=column_generate.go -out=column_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// columnType represents a type of a column.
type columnType uint8

const (
	typeGeneric = columnType(0)      // Generic column, every column should support this
	typeNumeric = columnType(1 << 0) // Numeric column supporting float64, int64 or uint64
	typeTextual = columnType(1 << 1) // Textual column supporting strings
)

// typeOf resolves all supported types of the column
func typeOf(column Column) (typ columnType) {
	if _, ok := column.(Numeric); ok {
		typ = typ | typeNumeric
	}
	if _, ok := column.(Textual); ok {
		typ = typ | typeTextual
	}
	return
}

// --------------------------- Contracts ----------------------------

// Column represents a column implementation
type Column interface {
	Grow(idx uint32)
	Apply(*commit.Reader)
	Value(idx uint32) (interface{}, bool)
	Contains(idx uint32) bool
	Index() *bitmap.Bitmap
	Snapshot(chunk commit.Chunk, dst *commit.Buffer)
}

// Numeric represents a column that stores numbers.
type Numeric interface {
	Column
	LoadFloat64(uint32) (float64, bool)
	LoadUint64(uint32) (uint64, bool)
	LoadInt64(uint32) (int64, bool)
	FilterFloat64(uint32, bitmap.Bitmap, func(v float64) bool)
	FilterUint64(uint32, bitmap.Bitmap, func(v uint64) bool)
	FilterInt64(uint32, bitmap.Bitmap, func(v int64) bool)
}

// Textual represents a column that stores strings.
type Textual interface {
	Column
	LoadString(uint32) (string, bool)
	FilterString(uint32, bitmap.Bitmap, func(v string) bool)
}

// --------------------------- Constructors ----------------------------

// Various column constructor functions for a specific types.
var (
	ForString  = makeStrings
	ForFloat32 = makeFloat32s
	ForFloat64 = makeFloat64s
	ForInt     = makeInts
	ForInt16   = makeInt16s
	ForInt32   = makeInt32s
	ForInt64   = makeInt64s
	ForUint    = makeUints
	ForUint16  = makeUint16s
	ForUint32  = makeUint32s
	ForUint64  = makeUint64s
	ForBool    = makeBools
	ForEnum    = makeEnum
	ForKey     = makeKey
)

// ForKind creates a new column instance for a specified reflect.Kind
func ForKind(kind reflect.Kind) (Column, error) {
	switch kind {
	case reflect.Float32:
		return makeFloat32s(), nil
	case reflect.Float64:
		return makeFloat64s(), nil
	case reflect.Int:
		return makeInts(), nil
	case reflect.Int16:
		return makeInt16s(), nil
	case reflect.Int32:
		return makeInt32s(), nil
	case reflect.Int64:
		return makeInt64s(), nil
	case reflect.Uint:
		return makeUints(), nil
	case reflect.Uint16:
		return makeUint16s(), nil
	case reflect.Uint32:
		return makeUint32s(), nil
	case reflect.Uint64:
		return makeUint64s(), nil
	case reflect.Bool:
		return makeBools(), nil
	case reflect.String:
		return makeStrings(), nil
	default:
		return nil, fmt.Errorf("column: unsupported column kind (%v)", kind)
	}
}

// --------------------------- Column ----------------------------

// column represents a column wrapper that synchronizes operations
type column struct {
	Column
	lock sync.RWMutex // The lock to protect the entire column
	kind columnType   // The type of the colum
	name string       // The name of the column
}

// columnFor creates a synchronized column for a column implementation
func columnFor(name string, v Column) *column {
	return &column{
		kind:   typeOf(v),
		name:   name,
		Column: v,
	}
}

// IsIndex returns whether the column is an index
func (c *column) IsIndex() bool {
	_, ok := c.Column.(*columnIndex)
	return ok
}

// Is checks whether a column type supports certain numerical operations.
func (c *column) IsNumeric() bool {
	return (c.kind & typeNumeric) == typeNumeric
}

// Is checks whether a column type supports certain string operations.
func (c *column) IsTextual() bool {
	return (c.kind & typeTextual) == typeTextual
}

// Grow grows the size of the column
func (c *column) Grow(idx uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.Column.Grow(idx)
}

// Apply performs a series of operations on a column.
func (c *column) Apply(r *commit.Reader) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	r.Rewind()
	c.Column.Apply(r)
}

// Snapshot takes a snapshot of a column, skipping indexes
func (c *column) Snapshot(chunk commit.Chunk, buffer *commit.Buffer) bool {
	if c.IsIndex() {
		return false
	}

	buffer.Reset(c.name)
	c.Column.Snapshot(chunk, buffer)
	return true
}

// Value retrieves a value at a specified index
func (c *column) Value(idx uint32) (v interface{}, ok bool) {
	v, ok = c.Column.Value(idx)
	return
}

// Value retrieves a value at a specified index
func (c *column) String(idx uint32) (v string, ok bool) {
	if column, text := c.Column.(Textual); text {
		v, ok = column.LoadString(idx)
	}
	return
}

// Float64 retrieves a float64 value at a specified index
func (c *column) Float64(idx uint32) (v float64, ok bool) {
	if n, contains := c.Column.(Numeric); contains {
		v, ok = n.LoadFloat64(idx)
	}
	return
}

// Int64 retrieves an int64 value at a specified index
func (c *column) Int64(idx uint32) (v int64, ok bool) {
	if n, contains := c.Column.(Numeric); contains {
		v, ok = n.LoadInt64(idx)
	}
	return
}

// Uint64 retrieves an uint64 value at a specified index
func (c *column) Uint64(idx uint32) (v uint64, ok bool) {
	if n, contains := c.Column.(Numeric); contains {
		v, ok = n.LoadUint64(idx)
	}
	return
}

// --------------------------- booleans ----------------------------

// columnBool represents a boolean column
type columnBool struct {
	data bitmap.Bitmap
}

// makeBools creates a new boolean column
func makeBools() Column {
	return &columnBool{
		data: make(bitmap.Bitmap, 0, 4),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnBool) Grow(idx uint32) {
	c.data.Grow(idx)
}

// Apply applies a set of operations to the column.
func (c *columnBool) Apply(r *commit.Reader) {
	for r.Next() {
		v := uint64(1) << (r.Offset & 0x3f)
		switch r.Type {
		case commit.PutTrue:
			c.data[r.Offset>>6] |= v
		case commit.PutFalse: // also "delete"
			c.data[r.Offset>>6] &^= v
		}
	}
}

// Value retrieves a value at a specified index
func (c *columnBool) Value(idx uint32) (interface{}, bool) {
	value := c.data.Contains(idx)
	return value, value
}

// Contains checks whether the column has a value at a specified index.
func (c *columnBool) Contains(idx uint32) bool {
	return c.data.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnBool) Index() *bitmap.Bitmap {
	return &c.data
}

// Snapshot writes the entire column into the specified destination buffer
func (c *columnBool) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	dst.PutBitmap(commit.PutTrue, chunk, c.data)
}

// slice accessor for boolean values
type boolSlice struct {
	writer *commit.Buffer
	reader *columnBool
}

// Set sets the value at the specified index
func (s *boolSlice) Set(index uint32, value string) {
	s.writer.PutString(commit.Put, index, value)
}

// Get loads the value at a particular index
func (s *boolSlice) Get(index uint32) bool {
	return s.reader.Contains(index)
}

// String returns a string column accessor
func (txn *Txn) Bool(columnName string) boolSlice {
	writer := txn.bufferFor(columnName)
	column, _ := txn.columnAt(columnName)
	reader, ok := column.Column.(*columnBool)
	if !ok {
		panic(fmt.Errorf("column: column %s is not of type boolean", columnName))
	}

	return boolSlice{
		writer: writer,
		reader: reader,
	}
}

// --------------------------- funcs ----------------------------

// resize calculates the new required capacity and a new index
func resize(capacity int, v uint32) int {
	const threshold = 256
	if v < threshold {
		v |= v >> 1
		v |= v >> 2
		v |= v >> 4
		v |= v >> 8
		v |= v >> 16
		v++
		return int(v)
	}

	if capacity < threshold {
		capacity = threshold
	}

	for 0 < capacity && capacity < int(v+1) {
		capacity += (capacity + 3*threshold) / 4
	}
	return capacity
}
