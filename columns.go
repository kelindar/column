// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=column -in=generic.go -out=z_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"
//go:generate genny -pkg=column -in=generic_test.go -out=z_numbers_test.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/kelindar/bitmap"
)

// Column represents a column implementation
type Column interface {
	Update(idx uint32, value interface{})
	UpdateMany(updates []Update)
	Delete(idx uint32)
	DeleteMany(items *bitmap.Bitmap)
	Value(idx uint32) (interface{}, bool)
	Contains(idx uint32) bool
	Intersect(*bitmap.Bitmap)
	Difference(*bitmap.Bitmap)
	Union(*bitmap.Bitmap)

	Hits() uint64 // TEST
	HitsReset()
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

// --------------------------- Base ----------------------------

// column represents a base column implementation with a lock and a fill list
type column struct {
	sync.RWMutex
	fill bitmap.Bitmap
	hits uint64
}

// Hits ...
func (c *column) Hits() uint64 {
	return atomic.AddUint64(&c.hits, 1)
}

func (c *column) HitsReset() {
	atomic.StoreUint64(&c.hits, 0)
	return
}

// Delete removes a value at a specified index
func (c *column) Delete(idx uint32) {
	c.Lock()
	c.fill.Remove(idx)
	c.Unlock()
}

// DeleteMany deletes a set of items from the column.
func (c *column) DeleteMany(items *bitmap.Bitmap) {
	c.Lock()
	c.fill.AndNot(*items)
	c.Unlock()
}

// Contains checks whether the column has a value at a specified index.
func (c *column) Contains(idx uint32) (exists bool) {
	c.RLock()
	exists = c.fill.Contains(idx)
	c.RUnlock()
	return
}

// Intersect performs a logical and operation and updates the destination bitmap.
func (c *column) Intersect(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(c.fill)
	c.RUnlock()
}

// Difference performs a logical and not operation and updates the destination bitmap.
func (c *column) Difference(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(c.fill)
	c.RUnlock()
}

// Union performs a logical or operation and updates the destination bitmap.
func (c *column) Union(dst *bitmap.Bitmap) {
	c.RLock()
	dst.Or(c.fill)
	c.RUnlock()
}

// --------------------------- Any ----------------------------

// columnAny represents a generic column
type columnAny struct {
	column
	data []interface{} // The actual values
}

// makeAny creates a new generic column
func makeAny() Column {
	return &columnAny{
		data: make([]interface{}, 0, 64),
		column: column{
			fill: make(bitmap.Bitmap, 0, 4),
		},
	}
}

// Update sets a value at a specified index
func (c *columnAny) Update(idx uint32, value interface{}) {
	c.Lock()
	size := uint32(len(c.data))
	for i := size; i <= idx; i++ {
		c.data = append(c.data, nil)
	}

	// Set the data at index
	c.fill.Set(idx)
	c.data[idx] = value
	c.Unlock()
}

// UpdateMany performs a series of updates at once
func (c *columnAny) UpdateMany(updates []Update) {
	c.Lock()
	defer c.Unlock()
	for _, u := range updates {
		c.fill.Set(u.Index)
		c.data[u.Index] = u.Value
	}
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

// --------------------------- booleans ----------------------------

// columnBool represents a boolean column
type columnBool struct {
	sync.RWMutex
	fill bitmap.Bitmap // The fill-list
	data bitmap.Bitmap // The actual values

	hits uint64 // TEST
}

// makeBools creates a new boolean column
func makeBools() Column {
	return &columnBool{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make(bitmap.Bitmap, 0, 4),
	}
}

func (c *columnBool) Hits() uint64 {
	return atomic.AddUint64(&c.hits, 1)
}

func (c *columnBool) HitsReset() {
	atomic.StoreUint64(&c.hits, 0)
	return
}

// Update sets a value at a specified index
func (c *columnBool) Update(idx uint32, value interface{}) {
	c.Lock()
	c.fill.Set(idx)
	if value.(bool) {
		c.data.Set(idx)
	} else {
		c.data.Remove(idx)
	}
	c.Unlock()
}

// UpdateMany performs a series of updates at once
func (c *columnBool) UpdateMany(updates []Update) {
	c.Lock()
	defer c.Unlock()

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
func (c *columnBool) Contains(idx uint32) (exists bool) {
	c.RLock()
	exists = c.fill.Contains(idx)
	c.RUnlock()
	return
}

// Intersect performs a logical and operation and updates the destination bitmap.
func (c *columnBool) Intersect(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(c.data)
	c.RUnlock()
}

// Difference performs a logical and not operation and updates the destination bitmap.
func (c *columnBool) Difference(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(c.data)
	c.RUnlock()
}

// Union performs a logical or operation and updates the destination bitmap.
func (c *columnBool) Union(dst *bitmap.Bitmap) {
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

// Index represents the index implementation
type index struct {
	column
	prop string
	rule func(v interface{}) bool
}

// newIndex creates a new indexer
func newIndex(prop string, rule func(v interface{}) bool) *index {
	return &index{
		prop: prop,
		rule: rule,
		column: column{
			fill: make(bitmap.Bitmap, 0, 4),
		},
	}
}

// Column returns the target name of the column on which this index should apply.
func (c *index) Column() string {
	return c.prop
}

// Update keeps the index up-to-date when a new value is added.
func (c *index) Update(idx uint32, value interface{}) {
	c.Lock()
	if c.rule(value) {
		c.fill.Set(idx)
	} else {
		c.fill.Remove(idx)
	}
	c.Unlock()
}

// UpdateMany performs a series of updates at once
func (c *index) UpdateMany(updates []Update) {
	c.Lock()
	defer c.Unlock()

	for _, u := range updates {
		if c.rule(u.Value) {
			c.fill.Set(u.Index)
		} else {
			c.fill.Remove(u.Index)
		}
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
