// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"github.com/kelindar/bitmap"
)

// Index represents an index contract
type Index interface {
	Column
	Column() string
	Bitmap() bitmap.Bitmap
}

// IndexFunc represents a function which can be used to build an index
type IndexFunc = func(v interface{}) bool

// ---------------------------------------------------------------------------------

// Index represents the index implementation
type index struct {
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

// Bitmap returns the associated index bitmap.
func (i *index) Bitmap() bitmap.Bitmap {
	return i.fill
}

// Column returns the target name of the column on which this index should apply.
func (i *index) Column() string {
	return i.prop
}

// Set keeps the index up-to-date when a new value is added.
func (i *index) Set(idx uint32, value interface{}) {
	if i.rule(value) {
		i.fill.Set(idx)
	} else {
		i.fill.Remove(idx)
	}
}

// Get retrieves a value at a specified index.
func (i *index) Get(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(i.fill)) {
		return false, false
	}

	return i.fill.Contains(idx), true
}

// Del deletes the element from the index.
func (i *index) Del(idx uint32) {
	i.fill.Remove(idx)
}
