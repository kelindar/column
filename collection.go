// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
)

// Object represents a single object
type Object = map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock  sync.RWMutex      // The collection lock
	fill  bitmap.Bitmap     // The fill-list
	cols  map[string]Column // The map of properties
	qlock sync.Mutex        // The modification queue lock
	queue []update          // The modification queue
}

// NewCollection creates a new columnar collection.
func NewCollection() *Collection {
	return &Collection{
		fill:  make(bitmap.Bitmap, 0, 4),
		cols:  make(map[string]Column, 8),
		queue: make([]update, 0, 64),
	}
}

// Insert adds an object to a collection and returns the allocated index
func (c *Collection) Insert(obj Object) uint32 {
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

// UpdateAt updates a specific row/column combination and sets the value. It is also
// possible to update during the query, which is much more convenient to use.
func (c *Collection) UpdateAt(idx uint32, columnName string, value interface{}) uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()

	// TODO: improve this to avoid iteration

	// Here we need to iterate over all of the columns, since we need to figure out
	// whether there are indices that need to be updated.
	for n, column := range c.cols {
		if i, ok := column.(computed); ok {
			n = i.Column()
		}

		if n == columnName {
			column.Set(idx, value)
		}
	}

	return idx
}

// DeleteAt removes the object at the specified index.
func (c *Collection) DeleteAt(idx uint32) {
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
		c.CreateColumn(k, reflect.TypeOf(v))
	}
}

// CreateColumn creates a column of a specified type and adds it to the collection.
func (c *Collection) CreateColumn(columnName string, columnType reflect.Type) {
	c.lock.Lock()
	defer c.lock.Unlock()

	column := columnFor(columnName, columnType)
	c.cols[columnName] = column
}

// DropColumn removes the column (or an index) with the specified name. If the column with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropColumn(columnName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.cols, columnName)
}

// CreateIndex creates an index column with a specified name which depends on a given
// column. The index function will be applied on the values of the column whenever
// a new row is added or updated.
func (c *Collection) CreateIndex(indexName, columnName string, fn IndexFunc) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if fn == nil || columnName == "" || indexName == "" {
		return fmt.Errorf("column: create index must specify name, column and function")
	}

	// Create and add the index column,
	index := newIndex(columnName, fn)
	c.cols[indexName] = index

	// If a column with this name already exists, iterate through all of the values
	// that we have in the collection and apply the filter.
	if column, ok := c.cols[columnName]; ok {
		c.fill.Clone(&index.fill)
		index.fill.Filter(func(x uint32) bool {
			if v, ok := column.Value(x); ok {
				return fn(v)
			}
			return false
		})
	}
	return nil
}

// DropIndex removes the index column with the specified name. If the index with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropIndex(indexName string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure it's an actual index, otherwise we might delete a column here
	if i, ok := c.cols[indexName]; ok {
		if _, ok := i.(computed); ok {
			delete(c.cols, indexName)
		}
	}
}

// Query creates a transaction which allows for filtering and iteration over the
// columns in this collection. It also allows for individual rows to be modified or
// deleted during iteration (range), but the actual operations will be queued and
// executed after the iteration.
func (c *Collection) Query(fn func(txn Txn) error) error {
	c.lock.RLock()
	r := aquireBitmap(&c.fill)
	defer releaseBitmap(r)

	// Execute the query and keep the error for later
	err := fn(Txn{
		owner: c,
		index: r,
	})

	// We're done with the reading part
	c.lock.RUnlock()

	// TODO: should we have txn.Commit() ?

	// Now that the iteration has finished, we can range over the pending action
	// queue and apply all of the actions that were requested by the cursor.
	c.qlock.Lock()
	defer c.qlock.Unlock()
	for _, u := range c.queue {
		//apply()
		if u.delete {
			c.DeleteAt(u.index)
			continue
		}

		c.UpdateAt(u.index, u.column, u.value)
	}

	/*for _, idx := range c.purge {
		c.DeleteAt(idx)
	}*/

	// Clean up the action queue
	c.queue = c.queue[:0]
	//c.purge = c.purge[:0]
	return err
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
