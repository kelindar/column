// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
)

// Object represents a single object
type Object = map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock sync.RWMutex      // The collection lock
	fill bitmap.Bitmap     // The fill-list
	cols map[string]Column // The map of properties
}

// NewCollection creates a new columnar collection.
func NewCollection() *Collection {
	return &Collection{
		fill: make(bitmap.Bitmap, 0, 4),
		cols: make(map[string]Column, 8),
	}
}

// Add adds an object to a collection and returns the allocated index
func (c *Collection) Add(obj Object) uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Find the index for the add
	idx, ok := c.fill.FirstZero()
	if !ok {
		idx = uint32(len(c.fill)) * 64
	}

	// Mark the current index in the fill list
	c.fill.Set(idx)

	// For each registered column, assign the appropriate object property. If the
	// column is actually an indirect index, use that column.
	for columnName, column := range c.cols {
		if i, ok := column.(computed); ok {
			columnName = i.Column()
		}

		if v, ok := obj[columnName]; ok {
			column.Set(idx, v)
		}
	}

	return idx
}

// Remove removes the object
func (c *Collection) Remove(idx uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove from global index
	c.fill.Remove(idx)

	// Remove the data for this element
	for _, column := range c.cols {
		column.Del(idx)
	}
}

// Count returns the total number of elements in the collection.
func (c *Collection) Count() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.fill.Count()
}

// AddColumnsOf registers a set of columns that are present in the target object
func (c *Collection) AddColumnsOf(object Object) {
	for k, v := range object {
		c.AddColumn(k, reflect.TypeOf(v))
	}
}

// AddColumn registers a column of a specified type to the collection
func (c *Collection) AddColumn(columnName string, columnType reflect.Type) {
	c.lock.Lock()
	defer c.lock.Unlock()

	column := columnFor(columnName, columnType)
	c.cols[columnName] = column
}

// AddIndex creates an index on a specified property
func (c *Collection) AddIndex(indexName, columnName string, fn IndexFunc) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if fn != nil {
		index := newIndex(columnName, fn)
		c.cols[indexName] = index
	} else { // Remove the index
		delete(c.cols, indexName)
	}
}

// View creates a read-only transaction which allows for filtering and iteration
// over the columns.
func (c *Collection) View(fn func(txn Txn) error) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	r := aquireBitmap()
	defer releaseBitmap(r)
	c.fill.Clone(r)

	return fn(Txn{
		owner: c,
		index: r,
	})
}

// Fetch retrieves an object by its handle and returns a selector for it.
func (c *Collection) Fetch(idx uint32) (Selector, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// If it's empty or over the sequence, not found
	if idx >= uint32(len(c.fill))*64 || !c.fill.Contains(idx) {
		return Selector{}, false
	}

	return Selector{
		index: idx,
		owner: c,
	}, true
}
