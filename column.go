// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=column -in=column_generate.go -out=column_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"fmt"
	"io"
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
	Snapshot(*commit.Buffer)
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
func ForKind(kind reflect.Kind) Column {
	switch kind {
	case reflect.Float32:
		return makeFloat32s()
	case reflect.Float64:
		return makeFloat64s()
	case reflect.Int:
		return makeInts()
	case reflect.Int16:
		return makeInt16s()
	case reflect.Int32:
		return makeInt32s()
	case reflect.Int64:
		return makeInt64s()
	case reflect.Uint:
		return makeUints()
	case reflect.Uint16:
		return makeUint16s()
	case reflect.Uint32:
		return makeUint32s()
	case reflect.Uint64:
		return makeUint64s()
	case reflect.Bool:
		return makeBools()
	case reflect.String:
		return makeStrings()
	default:
		panic(fmt.Errorf("column: unsupported column kind (%v)", kind))
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

// Snapshot snapshots the column into a temporary buffer and writes the content into the
// destionation io.Writer.
func (c *column) WriteTo(w io.Writer, tmp *commit.Buffer) (int64, error) {
	tmp.Reset(c.name)
	c.Column.Snapshot(tmp)
	return tmp.WriteTo(w)
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
func (c *columnBool) Snapshot(dst *commit.Buffer) {
	c.data.Range(func(idx uint32) {
		dst.PutOperation(commit.PutTrue, idx)
	})
}

// --------------------------- funcs ----------------------------

// capacityFor computes the next power of 2 for a given index
func capacityFor(v uint32) int {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return int(v)
}
