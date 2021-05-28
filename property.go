// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"github.com/kelindar/bitmap"
)

type Mutator interface {
	Indexer
	Set(idx uint32, value interface{})
	Get(idx uint32) (interface{}, bool)
	Del(idx uint32)
}

// Assert interface compliance
var _ Mutator = newProperty()

// Property represents a generic column
type property struct {
	free bitmap.Bitmap // The free-list
	data []interface{} // The actual values
}

// newProperty creates a new property column
func newProperty() *property {
	return &property{
		free: make(bitmap.Bitmap, 0, 4),
		data: make([]interface{}, 0, 64),
	}
}

// Index returns the associated index bitmap.
func (p *property) Index() bitmap.Bitmap {
	return p.free // TODO: should be "fill-list"
}

// Set sets a value at a specified index
func (p *property) Set(idx uint32, value interface{}) {
	size := uint32(len(p.data))
	for i := size; i <= idx; i++ {
		p.free.Set(i)
		p.data = append(p.data, nil)
	}

	// If this is a replacement, remove
	if p.free.Contains(idx) {
		p.free.Remove(idx)
	}

	// Set the data at index
	p.data[idx] = value
}

// Get retrieves a value at a specified index
func (p *property) Get(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(p.data)) || p.free.Contains(idx) {
		return nil, false
	}

	return p.data[idx], true
}

// Del removes a value at a specified index
func (p *property) Del(idx uint32) {
	p.free.Set(idx)
}
