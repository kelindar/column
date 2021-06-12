// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=column -in=generic.go -out=z_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"
//go:generate genny -pkg=column -in=generic_test.go -out=z_numbers_test.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
)

// Column represents a column implementation
type Column interface {
	Update(idx uint32, value interface{})
	Delete(idx uint32)
	DeleteMany(items *bitmap.Bitmap)
	Value(idx uint32) (interface{}, bool)
	Contains(idx uint32) bool
	And(dst *bitmap.Bitmap)
	AndNot(dst *bitmap.Bitmap)
	Or(dst *bitmap.Bitmap)
}

// Numerical represents a numerical column implementation
type numerical interface {
	Float64(uint32) (float64, bool)
	Uint64(uint32) (uint64, bool)
	Int64(uint32) (int64, bool)
}

// columnFor creates a new column instance for a specified type
func columnFor(columnName string, typ reflect.Type) Column {
	switch typ.Kind() {
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

// --------------------------- Any ----------------------------

// columnAny represents a generic column
type columnAny struct {
	sync.RWMutex
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

// Update sets a value at a specified index
func (c *columnAny) Update(idx uint32, value interface{}) {
	c.Lock()
	defer c.Unlock()

	size := uint32(len(c.data))
	for i := size; i <= idx; i++ {
		c.data = append(c.data, nil)
	}

	// Set the data at index
	c.fill.Set(idx)
	c.data[idx] = value.(interface{})
}

// Value retrieves a value at a specified index
func (c *columnAny) Value(idx uint32) (v interface{}, ok bool) {
	c.RLock()
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	c.RUnlock()
	return
}

// Delete removes a value at a specified index
func (c *columnAny) Delete(idx uint32) {
	c.Lock()
	c.fill.Remove(idx)
	c.data[idx] = nil
	c.Unlock()
}

// DeleteMany deletes a set of items from the column.
func (c *columnAny) DeleteMany(items *bitmap.Bitmap) {
	c.Lock()
	c.fill.AndNot(*items)
	c.Unlock()
}

// Contains checks whether the column has a value at a specified index.
func (c *columnAny) Contains(idx uint32) bool {
	c.RLock()
	defer c.RUnlock()
	return c.fill.Contains(idx)
}

// And performs a logical and operation and updates the destination bitmap.
func (c *columnAny) And(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(c.fill)
	c.RUnlock()
}

// And performs a logical and not operation and updates the destination bitmap.
func (c *columnAny) AndNot(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(c.fill)
	c.RUnlock()
}

// Or performs a logical or operation and updates the destination bitmap.
func (c *columnAny) Or(dst *bitmap.Bitmap) {
	c.RLock()
	dst.Or(c.fill)
	c.RUnlock()
}

// --------------------------- booleans ----------------------------

// columnBool represents a boolean column
type columnBool struct {
	sync.RWMutex
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

// Update sets a value at a specified index
func (c *columnBool) Update(idx uint32, value interface{}) {
	c.Lock()
	defer c.Unlock()

	c.fill.Set(idx)
	if value.(bool) {
		c.data.Set(idx)
	} else {
		c.data.Remove(idx)
	}
}

// Value retrieves a value at a specified index
func (c *columnBool) Value(idx uint32) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	if !c.fill.Contains(idx) {
		return false, false
	}

	return c.data.Contains(idx), true
}

// Delete removes a value at a specified index
func (c *columnBool) Delete(idx uint32) {
	c.Lock()
	c.fill.Remove(idx)
	c.data.Remove(idx)
	c.Unlock()
}

// DeleteMany deletes a set of items from the column.
func (c *columnBool) DeleteMany(items *bitmap.Bitmap) {
	c.Lock()
	c.fill.AndNot(*items)
	c.data.AndNot(*items)
	c.Unlock()
}

// Contains checks whether the column has a value at a specified index.
func (c *columnBool) Contains(idx uint32) bool {
	c.RLock()
	defer c.RUnlock()
	return c.fill.Contains(idx)
}

// And performs a logical and operation and updates the destination bitmap.
func (c *columnBool) And(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(c.data)
	c.RUnlock()
}

// And performs a logical and not operation and updates the destination bitmap.
func (c *columnBool) AndNot(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(c.data)
	c.RUnlock()
}

// Or performs a logical or operation and updates the destination bitmap.
func (c *columnBool) Or(dst *bitmap.Bitmap) {
	c.RLock()
	dst.Or(c.data)
	c.RUnlock()
}

// --------------------------- computed index ----------------------------

// computed represents a computed column
type computed interface {
	Column
	Column() string
}

// IndexFunc represents a function which can be used to build an index
type IndexFunc = func(v interface{}) bool

// Index represents the index implementation
type index struct {
	sync.RWMutex
	prop string
	rule func(v interface{}) bool
	fill bitmap.Bitmap
}

// newIndex creates a new indexer
func newIndex(prop string, rule func(v interface{}) bool) *index {
	return &index{
		prop: prop,
		rule: rule,
		fill: make(bitmap.Bitmap, 0, 8),
	}
}

// Column returns the target name of the column on which this index should apply.
func (c *index) Column() string {
	return c.prop
}

// Update keeps the index up-to-date when a new value is added.
func (c *index) Update(idx uint32, value interface{}) {
	c.Lock()
	defer c.Unlock()
	if c.rule(value) {
		c.fill.Set(idx)
	} else {
		c.fill.Remove(idx)
	}
}

// Value retrieves a value at a specified index.
func (c *index) Value(idx uint32) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()
	if idx >= uint32(len(c.fill))*64 {
		return false, false
	}

	return c.fill.Contains(idx), true
}

// Delete deletes the element from the index.
func (c *index) Delete(idx uint32) {
	c.Lock()
	c.fill.Remove(idx)
	c.Unlock()
}

// DeleteMany deletes a set of items from the column.
func (c *index) DeleteMany(items *bitmap.Bitmap) {
	c.Lock()
	c.fill.AndNot(*items)
	c.Unlock()
}

// Contains checks whether the column has a value at a specified index.
func (c *index) Contains(idx uint32) bool {
	c.RLock()
	defer c.RUnlock()
	return c.fill.Contains(idx)
}

// And performs a logical and operation and updates the destination bitmap.
func (c *index) And(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(c.fill)
	c.RUnlock()
}

// And performs a logical and not operation and updates the destination bitmap.
func (c *index) AndNot(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(c.fill)
	c.RUnlock()
}

// Or performs a logical or operation and updates the destination bitmap.
func (c *index) Or(dst *bitmap.Bitmap) {
	c.RLock()
	dst.Or(c.fill)
	c.RUnlock()
}
