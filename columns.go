// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=column -in=generic.go -out=z_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"
//go:generate genny -pkg=column -in=generic_test.go -out=z_numbers_test.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// Column represents a column implementation
type Column interface {
	Grow(idx uint32)
	Update(updates []commit.Update)
	Delete(items *bitmap.Bitmap)
	Value(idx uint32) (interface{}, bool)
	Contains(idx uint32) bool
	Index() *bitmap.Bitmap
}

// Numerical represents a numerical column implementation
type numerical interface {
	Float64(uint32) (float64, bool)
	Uint64(uint32) (uint64, bool)
	Int64(uint32) (int64, bool)
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
	sync.RWMutex
	name string
	Column
}

// columnFor creates a synchronized column for a column implementation
func columnFor(name string, v Column) *column {
	return &column{
		name:   name,
		Column: v,
	}
}

// Intersect performs a logical and operation and updates the destination bitmap.
func (c *column) Intersect(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(*c.Index())
	c.RUnlock()
}

// Difference performs a logical and not operation and updates the destination bitmap.
func (c *column) Difference(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(*c.Index())
	c.RUnlock()
}

// Union performs a logical or operation and updates the destination bitmap.
func (c *column) Union(dst *bitmap.Bitmap) {
	c.RLock()
	dst.Or(*c.Index())
	c.RUnlock()
}

// Update performs a series of updates at once
func (c *column) Update(updates []commit.Update, growUntil uint32) {
	c.Lock()
	c.Column.Grow(growUntil)
	c.Column.Update(updates)
	c.Unlock()
}

// Delete deletes a set of items from the column.
func (c *column) Delete(items *bitmap.Bitmap) {
	c.Lock()
	c.Column.Delete(items)
	c.Unlock()
}

// Contains checks whether the column has a value at a specified index.
func (c *column) Contains(idx uint32) (exists bool) {
	c.RLock()
	exists = c.Column.Contains(idx)
	c.RUnlock()
	return
}

// Value retrieves a value at a specified index
func (c *column) Value(idx uint32) (v interface{}, ok bool) {
	c.RLock()
	v, ok = c.loadValue(idx)
	c.RUnlock()
	return
}

// Float64 retrieves a float64 value at a specified index
func (c *column) Float64(idx uint32) (v float64, ok bool) {
	c.RLock()
	v, ok = c.loadFloat64(idx)
	c.RUnlock()
	return
}

// Int64 retrieves an int64 value at a specified index
func (c *column) Int64(idx uint32) (v int64, ok bool) {
	c.RLock()
	v, ok = c.loadInt64(idx)
	c.RUnlock()
	return
}

// Uint64 retrieves an uint64 value at a specified index
func (c *column) Uint64(idx uint32) (v uint64, ok bool) {
	c.RLock()
	v, ok = c.loadUint64(idx)
	c.RUnlock()
	return
}

// loadValue (unlocked) retrieves a value at a specified index
func (c *column) loadValue(idx uint32) (v interface{}, ok bool) {
	v, ok = c.Column.Value(idx)
	return
}

// loadFloat64 (unlocked)  retrieves a float64 value at a specified index
func (c *column) loadFloat64(idx uint32) (v float64, ok bool) {
	if n, contains := c.Column.(numerical); contains {
		v, ok = n.Float64(idx)
	}
	return
}

// loadInt64 (unlocked)  retrieves an int64 value at a specified index
func (c *column) loadInt64(idx uint32) (v int64, ok bool) {
	if n, contains := c.Column.(numerical); contains {
		v, ok = n.Int64(idx)
	}
	return
}

// loadUint64 (unlocked)  retrieves an uint64 value at a specified index
func (c *column) loadUint64(idx uint32) (v uint64, ok bool) {
	if n, contains := c.Column.(numerical); contains {
		v, ok = n.Uint64(idx)
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
	// TODO: also grow the bitmap
	size := uint32(len(c.data))
	for i := size; i <= idx; i++ {
		c.data = append(c.data, nil)
	}
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
func (c *columnAny) Delete(items *bitmap.Bitmap) {
	c.fill.AndNot(*items)
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
	// TODO
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
func (c *columnBool) Delete(items *bitmap.Bitmap) {
	c.fill.AndNot(*items)
	c.data.AndNot(*items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnBool) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnBool) Index() *bitmap.Bitmap {
	return &c.data
}

// --------------------------- computed index ----------------------------

// computed represents a computed column
type computed interface {
	Column() string
}

// Index represents the index implementation
type index struct {
	fill bitmap.Bitmap
	prop string
	rule func(v interface{}) bool
}

// newIndex creates a new indexer
func newIndex(indexName, columnName string, rule func(v interface{}) bool) *column {
	return columnFor(indexName, &index{
		fill: make(bitmap.Bitmap, 0, 4),
		prop: columnName,
		rule: rule,
	})
}

// Grow grows the size of the column until we have enough to store
func (c *index) Grow(idx uint32) {
	// TODO
}

// Column returns the target name of the column on which this index should apply.
func (c *index) Column() string {
	return c.prop
}

// Update performs a series of updates at once
func (c *index) Update(updates []commit.Update) {

	// Index can only be updated based on the final stored value, so we can only work
	// with put operations here. The trick is to update the final value after applying
	// on the actual column.
	for _, u := range updates {
		if u.Type == commit.Put {
			if c.rule(u.Value) {
				c.fill.Set(u.Index)
			} else {
				c.fill.Remove(u.Index)
			}
		}
	}
}

// Delete deletes a set of items from the column.
func (c *index) Delete(items *bitmap.Bitmap) {
	c.fill.AndNot(*items)
}

// Value retrieves a value at a specified index.
func (c *index) Value(idx uint32) (v interface{}, ok bool) {
	if idx < uint32(len(c.fill))<<6 {
		v, ok = c.fill.Contains(idx), true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *index) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *index) Index() *bitmap.Bitmap {
	return &c.fill
}
