// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=column -in=column_generate.go -out=column_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"reflect"

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
	Update(updates []commit.Update)
	Delete(offset int, items bitmap.Bitmap)
	Value(idx uint32) (interface{}, bool)
	Contains(idx uint32) bool
	Index() *bitmap.Bitmap
}

// Numeric represents a column that stores numbers.
type Numeric interface {
	Column
	LoadFloat64(uint32) (float64, bool)
	LoadUint64(uint32) (uint64, bool)
	LoadInt64(uint32) (int64, bool)
	FilterFloat64(*bitmap.Bitmap, func(v float64) bool)
	FilterUint64(*bitmap.Bitmap, func(v uint64) bool)
	FilterInt64(*bitmap.Bitmap, func(v int64) bool)
}

// Textual represents a column that stores strings.
type Textual interface {
	Column
	LoadString(uint32) (string, bool)
	FilterString(*bitmap.Bitmap, func(v string) bool)
}

// --------------------------- Constructors ----------------------------

// Various column constructor functions for a specific types.
var (
	ForAny     = makeAny
	ForString  = makeAny
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
	default:
		return makeAny()
	}
}

// --------------------------- Column ----------------------------

// column represents a column wrapper that synchronizes operations
type column struct {
	Column
	kind columnType // The type of the colum
	name string     // The name of the column
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

// Update performs a series of updates at once
func (c *column) Update(updates []commit.Update, growUntil uint32) {
	c.Column.Grow(growUntil)
	c.Column.Update(updates)
}

// Delete deletes a set of items from the column.
func (c *column) Delete(offset int, items bitmap.Bitmap) {
	c.Column.Delete(offset, items)
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

// --------------------------- Any ----------------------------

// columnAny represents a generic column
type columnAny struct {
	fill bitmap.Bitmap // The fill-list
	data []interface{} // The actual values
}

// makeAny creates a new generic column
func makeAny() Column {
	return &columnAny{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]interface{}, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnAny) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]interface{}, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Update performs a series of updates at once
func (c *columnAny) Update(updates []commit.Update) {

	// Update the values of the column, for this one we can only process stores
	for _, u := range updates {
		if u.Type == commit.Put {
			c.fill.Set(u.Index)
			c.data[u.Index] = u.Value
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnAny) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Value retrieves a value at a specified index
func (c *columnAny) Value(idx uint32) (v interface{}, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *columnAny) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnAny) Index() *bitmap.Bitmap {
	return &c.fill
}

// LoadString retrieves a value at a specified index
func (c *columnAny) LoadString(idx uint32) (string, bool) {
	v, has := c.Value(idx)
	s, ok := v.(string)
	return s, has && ok
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnAny) FilterString(index *bitmap.Bitmap, predicate func(v string) bool) {
	index.And(c.fill)
	index.Filter(func(idx uint32) (match bool) {
		return idx < uint32(len(c.data)) && predicate(c.data[idx].(string))
	})
}

// --------------------------- booleans ----------------------------

// columnBool represents a boolean column
type columnBool struct {
	fill bitmap.Bitmap // The fill-list
	data bitmap.Bitmap // The actual values
}

// makeBools creates a new boolean column
func makeBools() Column {
	return &columnBool{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make(bitmap.Bitmap, 0, 4),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnBool) Grow(idx uint32) {
	c.fill.Grow(idx)
}

// Update performs a series of updates at once
func (c *columnBool) Update(updates []commit.Update) {
	for _, u := range updates {
		c.fill.Set(u.Index)
		if u.Value.(bool) {
			c.data.Set(u.Index)
		} else {
			c.data.Remove(u.Index)
		}
	}
}

// Value retrieves a value at a specified index
func (c *columnBool) Value(idx uint32) (interface{}, bool) {
	return c.data.Contains(idx), c.fill.Contains(idx)
}

// Delete deletes a set of items from the column.
func (c *columnBool) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	//data := c.data[offset:]
	fill.AndNot(items)
	//c.data.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnBool) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnBool) Index() *bitmap.Bitmap {
	return &c.data
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
