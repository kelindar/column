// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/kelindar/bitmap"
)

// Object represents a single object
type Object = map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock sync.RWMutex  // The lock for fill list
	cols columns       // The map of columns
	fill bitmap.Bitmap // The fill-list
}

// NewCollection creates a new columnar collection.
func NewCollection() *Collection {
	return &Collection{
		cols: makeColumns(8),
		fill: make(bitmap.Bitmap, 0, 4),
	}
}

// Insert adds an object to a collection and returns the allocated index
func (c *Collection) Insert(obj Object) uint32 {
	c.lock.Lock()

	// Find the index for the add
	idx, ok := c.fill.FirstZero()
	if !ok {
		idx = uint32(len(c.fill)) * 64
	}

	// Mark the current index in the fill list
	c.fill.Set(idx)
	c.lock.Unlock()

	// For each registered column, assign the appropriate object property. If the
	// column is actually an indirect index, use that column.
	c.cols.RangeName(func(columnName string, column Column) {
		if i, ok := column.(computed); ok {
			columnName = i.Column()
		}

		if v, ok := obj[columnName]; ok {
			column.Update(idx, v)
		}
	})

	return idx
}

// UpdateAt updates a specific row/column combination and sets the value. It is also
// possible to update during the query, which is much more convenient to use.
func (c *Collection) UpdateAt(idx uint32, columnName string, value interface{}) {
	if columns, ok := c.cols.LoadWithIndex(columnName); ok {
		for _, v := range columns {
			v.Update(idx, value)
		}
	}
}

// DeleteAt removes the object at the specified index.
func (c *Collection) DeleteAt(idx uint32) {

	// Remove from global index
	c.lock.Lock()
	c.fill.Remove(idx)
	c.lock.Unlock()

	// Remove the data for this element
	c.cols.Range(func(column Column) {
		column.Delete(idx)
	})
}

// Count returns the total number of elements in the collection.
func (c *Collection) Count() (count int) {
	c.lock.RLock()
	count = c.fill.Count()
	c.lock.RUnlock()
	return
}

// CreateColumnsOf registers a set of columns that are present in the target object.
func (c *Collection) CreateColumnsOf(object Object) {
	for k, v := range object {
		c.CreateColumn(k, reflect.TypeOf(v).Kind())
	}
}

// CreateColumn creates a column of a specified type and adds it to the collection.
func (c *Collection) CreateColumn(columnName string, columnType reflect.Kind) {
	column := columnFor(columnName, columnType)
	c.cols.Store(columnName, column)
}

// DropColumn removes the column (or an index) with the specified name. If the column with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropColumn(columnName string) {
	c.cols.DeleteColumn(columnName)
}

// CreateIndex creates an index column with a specified name which depends on a given
// column. The index function will be applied on the values of the column whenever
// a new row is added or updated.
func (c *Collection) CreateIndex(indexName, columnName string, fn func(v interface{}) bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if fn == nil || columnName == "" || indexName == "" {
		return fmt.Errorf("column: create index must specify name, column and function")
	}

	// Create and add the index column,
	index := newIndex(columnName, fn)
	c.cols.Store(indexName, index)
	c.cols.Store(columnName, nil, index)

	// If a column with this name already exists, iterate through all of the values
	// that we have in the collection and apply the filter.
	if column, ok := c.cols.Load(columnName); ok {
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

	// Get the specified index to drop
	column, _ := c.cols.Load(indexName)
	if _, ok := column.(computed); !ok {
		return
	}

	// Figure out the associated column and delete the index from that
	columnName := column.(computed).Column()
	c.cols.DeleteIndex(columnName, indexName)
	c.cols.DeleteColumn(indexName)
}

// Query creates a transaction which allows for filtering and iteration over the
// columns in this collection. It also allows for individual rows to be modified or
// deleted during iteration (range), but the actual operations will be queued and
// executed after the iteration.
func (c *Collection) Query(fn func(txn *Txn) error) error {
	c.lock.RLock()
	txn := aquireTxn(c)
	c.lock.RUnlock()

	// Execute the query and keep the error for later
	if err := fn(txn); err != nil {
		txn.Rollback()
		releaseTxn(txn)
		return err
	}

	// Now that the iteration has finished, we can range over the pending action
	// queue and apply all of the actions that were requested by the Selector.
	txn.Commit()
	releaseTxn(txn)
	return nil
}

// Fetch retrieves an object by its handle and returns a Selector for it.
func (c *Collection) Fetch(idx uint32) (Selector, bool) {
	c.lock.RLock()
	contains := c.fill.Contains(idx)
	c.lock.RUnlock()

	// If it's empty or over the sequence, not found
	if idx >= uint32(len(c.fill))*64 || !contains {
		return Selector{}, false
	}

	return Selector{
		index: idx,
		owner: c,
	}, true
}

// --------------------------- column registry ---------------------------

// columns represents a concurrent column registry.
type columns struct {
	cols *atomic.Value
}

func makeColumns(capacity int) columns {
	data := columns{
		cols: &atomic.Value{},
	}

	data.cols.Store(make([]columnEntry, 0, capacity))
	return data
}

// columnEntry represents a column entry in the registry.
type columnEntry struct {
	name string   // The column name
	cols []Column // The columns and its computed
}

// Range iterates over columns in the registry.
func (c *columns) Range(fn func(column Column)) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		fn(v.cols[0])
	}
}

// RangeName iterates over columns in the registry together with their names.
func (c *columns) RangeName(fn func(columnName string, column Column)) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		fn(v.name, v.cols[0])
	}
}

// Load loads a column by its name.
func (c *columns) Load(columnName string) (Column, bool) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if v.name == columnName {
			col := v.cols[0]
			return col, col != nil
		}
	}
	return nil, false
}

// LoadWithIndex loads a column by its name along with their computed indices.
func (c *columns) LoadWithIndex(columnName string) ([]Column, bool) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if v.name == columnName {
			return v.cols, true
		}
	}
	return nil, false
}

// Store stores a column into the registry.
func (c *columns) Store(columnName string, column Column, index ...Column) {

	// Try to update an existing entry
	columns := c.cols.Load().([]columnEntry)
	for i, v := range columns {
		if v.name != columnName {
			continue
		}

		// If we found an existing entry, update it and we're done
		if column != nil {
			columns[i].cols[0] = column
		}
		if index != nil {
			columns[i].cols = append(columns[i].cols, index...)
		}
		c.cols.Store(columns)

		return
	}

	// No entry found, create a new one
	value := []Column{column}
	value = append(value, index...)
	columns = append(columns, columnEntry{
		name: columnName,
		cols: value,
	})
	c.cols.Store(columns)
	return
}

// DeleteColumn deletes a column from the registry.
func (c *columns) DeleteColumn(columnName string) {
	columns := c.cols.Load().([]columnEntry)
	filtered := make([]columnEntry, 0, cap(columns))
	for _, v := range columns {
		if v.name != columnName {
			filtered = append(filtered, v)
		}
	}
	c.cols.Store(filtered)
}

// Delete deletes a column from the registry.
func (c *columns) DeleteIndex(columnName, indexName string) {
	index, ok := c.Load(indexName)
	if !ok {
		return
	}

	columns := c.cols.Load().([]columnEntry)
	for i, v := range columns {
		if v.name != columnName {
			continue
		}

		// If this is the target column, update its computed columns
		filtered := make([]Column, 0, cap(columns[i].cols))
		filtered = append(filtered, columns[i].cols[0])
		for _, idx := range v.cols[1:] {
			if idx != index {
				filtered = append(filtered, idx)
			}
		}
		columns[i].cols = filtered
	}

	c.cols.Store(columns)
}
