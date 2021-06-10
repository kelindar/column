// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"reflect"

	"github.com/kelindar/bitmap"
)

// Column represents a column implementation
type Column interface {
	Set(idx uint32, value interface{})
	Get(idx uint32) (interface{}, bool)
	Del(idx uint32)
	Bitmap() bitmap.Bitmap
}

// columnFor creates a new column instance for a specified type
func columnFor(columnName string, typ reflect.Type) Column {
	switch typ.Kind() {
	case reflect.Bool:
		return newColumnBool()
	default:
		return newColumnAny()
	}
}

// ------------------------------------------------------------------------

// columnAny represents a generic column
type columnAny struct {
	fill bitmap.Bitmap // The fill-list
	data []interface{} // The actual values
}

// newColumnAny creates a new generic column
func newColumnAny() Column {
	return &columnAny{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]interface{}, 0, 64),
	}
}

// Set sets a value at a specified index
func (p *columnAny) Set(idx uint32, value interface{}) {
	size := uint32(len(p.data))
	for i := size; i <= idx; i++ {
		p.data = append(p.data, nil)
	}

	// Set the data at index
	p.fill.Set(idx)
	p.data[idx] = value
}

// Get retrieves a value at a specified index
func (p *columnAny) Get(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(p.data)) || !p.fill.Contains(idx) {
		return nil, false
	}

	return p.data[idx], true
}

// Del removes a value at a specified index
func (p *columnAny) Del(idx uint32) {
	p.fill.Remove(idx)
	p.data[idx] = nil
}

// Bitmap returns the associated index bitmap.
func (p *columnAny) Bitmap() bitmap.Bitmap {
	return p.fill
}

// ------------------------------------------------------------------------

// columnBool represents a boolean column
type columnBool struct {
	free bitmap.Bitmap // The free-list
	data bitmap.Bitmap // The actual values
}

// newColumnBool creates a new property column
func newColumnBool() Column {
	return &columnBool{
		free: make(bitmap.Bitmap, 0, 4),
		data: make(bitmap.Bitmap, 0, 4),
	}
}

// Set sets a value at a specified index
func (p *columnBool) Set(idx uint32, value interface{}) {
	size := uint32(len(p.data))
	for i := size; i <= idx; i++ {
		p.free.Set(i)
	}

	// If this is a replacement, remove
	if p.free.Contains(idx) {
		p.free.Remove(idx)
	}

	// Set the data at index
	if value.(bool) {
		p.data.Set(idx)
	} else {
		p.data.Remove(idx)
	}
}

// Get retrieves a value at a specified index
func (p *columnBool) Get(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(p.data)) || p.free.Contains(idx) {
		return false, false
	}

	return p.data.Contains(idx), true
}

// Del removes a value at a specified index
func (p *columnBool) Del(idx uint32) {
	p.free.Set(idx)
}

// Bitmap returns the associated index bitmap.
func (p *columnBool) Bitmap() bitmap.Bitmap {
	return p.data
}
