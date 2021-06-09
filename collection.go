// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
)

// Object represents a single object
type Object map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock  sync.RWMutex        // The collection lock
	size  uint32              // The current size
	fill  bitmap.Bitmap       // The fill-list
	props map[string]Column   // The map of properties
	index map[string]computed // The set of indexes by index name
}

// New creates a new columnar collection.
func New() *Collection {
	return &Collection{
		props: make(map[string]Column, 8),
		index: make(map[string]computed, 8),
		fill:  make(bitmap.Bitmap, 0, 4),
	}
}

// Fetch retrieves an object by its handle
func (c *Collection) Fetch(index uint32) (Object, bool) {
	obj := make(Object, 8)
	if ok := c.FetchTo(index, &obj); !ok {
		return nil, false
	}
	return obj, true
}

// FetchTo retrieves an object by its handle into a existing object
func (c *Collection) FetchTo(idx uint32, dest *Object) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// If it's empty or over the sequence, not found
	if idx >= c.size || !c.fill.Contains(idx) {
		return false
	}

	// Reassemble the object from its properties
	obj := *dest
	for name, prop := range c.props {
		if v, ok := prop.Get(idx); ok {
			obj[name] = v
		}
	}
	return true
}

// Add adds an object to a collection and returns the allocated index
func (c *Collection) Add(obj Object) uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Find the index for the add
	idx, ok := c.fill.FirstZero()
	if !ok {
		idx = c.size
		c.size++
	}

	// Mark the current index in the fill list
	c.fill.Set(idx)

	// For each registered column, assign the appropriate object property. If the
	// column is actually an indirect index, use that column.
	for columnName, column := range c.props {
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
	for _, column := range c.props {
		column.Del(idx)
	}
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
	c.props[columnName] = column
}

// AddIndex creates an index on a specified property
func (c *Collection) AddIndex(indexName, columnName string, fn IndexFunc) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if fn != nil {
		index := newIndex(columnName, fn)
		c.props[indexName] = index
		c.index[indexName] = index
	} else { // Remove the index
		delete(c.index, indexName)
		delete(c.props, indexName)
	}
}

// Count counts the number of elements which match the specified filter function. If
// there is no specified filter function, it returns the total count of elements in
// the collection.
func (c *Collection) Count(where func(where Query)) int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// If there's no filter specified, simply count everything
	if where == nil {
		return c.fill.Count()
	}

	q := c.query(where)
	defer releaseBitmap(q.index)
	return q.count()
}

// Find ...
func (c *Collection) Find(where func(where Query), fn func(Object) bool, props ...string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	q := c.query(where)
	defer releaseBitmap(q.index)
	q.iterate(fn, props)
}

func (c *Collection) query(where func(Query)) Query {
	r := aquireBitmap()
	c.fill.Clone(r)
	q := Query{
		owner: c,
		index: r,
	}
	where(q)
	return q
}
