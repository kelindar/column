// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate genny -pkg=columnar -in=generic.go -out=z_numbers.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"
//go:generate genny -pkg=columnar -in=generic_test.go -out=z_numbers_test.go gen "number=float32,float64,int,int16,int32,int64,uint,uint16,uint32,uint64"

package column

import (
	"reflect"

	"github.com/kelindar/bitmap"
)

// Column represents a column implementation
type Column interface {
	Set(idx uint32, value interface{})
	Del(idx uint32)
	Value(idx uint32) (interface{}, bool)
	Bitmap() bitmap.Bitmap
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
		return newColumnAny()
	}
}

// --------------------------- Any ----------------------------

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
	p.data[idx] = value.(interface{})
}

// Value retrieves a value at a specified index
func (p *columnAny) Value(idx uint32) (interface{}, bool) {
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

// Set sets a value at a specified index
func (p *columnBool) Set(idx uint32, value interface{}) {
	p.fill.Set(idx)
	if value.(bool) {
		p.data.Set(idx)
	} else {
		p.data.Remove(idx)
	}
}

// Value retrieves a value at a specified index
func (p *columnBool) Value(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(p.data)) || !p.fill.Contains(idx) {
		return false, false
	}

	return p.data.Contains(idx), true
}

// Del removes a value at a specified index
func (p *columnBool) Del(idx uint32) {
	p.fill.Remove(idx)
	p.data.Remove(idx)
}

// Bitmap returns the associated index bitmap.
func (p *columnBool) Bitmap() bitmap.Bitmap {
	return p.data
}
