// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"fmt"
	"math/bits"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// Object represents a single object
type Object = map[string]interface{}

const (
	expireColumn = "expire"
)

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock   sync.RWMutex       // The lock for fill list
	cols   columns            // The map of columns
	fill   bitmap.Bitmap      // The fill-list
	size   int                // The initial size for new columns
	count  int                // The current count of elements
	writer commit.Writer      // The commit writer
	cancel context.CancelFunc // The cancellation function for the context
}

// Options represents the options for a collection.
type Options struct {
	Capacity int           // The initial capacity when creating columns
	Writer   commit.Writer // The writer for the commit log (optional)
	Vacuum   time.Duration // The interval at which the vacuum of expired entries will be done
}

// NewCollection creates a new columnar collection.
func NewCollection(opts ...Options) *Collection {
	options := Options{
		Capacity: 1024,
		Vacuum:   1 * time.Second,
		Writer:   nil,
	}

	// Merge options together
	for _, o := range opts {
		if o.Capacity > 0 {
			options.Capacity = o.Capacity
		}
		if o.Vacuum > 0 {
			options.Vacuum = o.Vacuum
		}
		if o.Writer != nil {
			options.Writer = o.Writer
		}
	}

	// Create a new collection
	ctx, cancel := context.WithCancel(context.Background())
	store := &Collection{
		cols:   makeColumns(8),
		size:   options.Capacity,
		fill:   make(bitmap.Bitmap, 0, options.Capacity>>6),
		writer: options.Writer,
		cancel: cancel,
	}

	// Create an expiration column and start the cleanup goroutine
	store.CreateColumn(expireColumn, ForInt64())
	go store.vacuum(ctx, options.Vacuum)
	return store
}

// next finds the next free index in the collection, atomically.
func (c *Collection) next() uint32 {
	c.lock.Lock()
	idx := c.findFreeIndex()
	c.fill.Set(idx)
	c.count++
	c.lock.Unlock()
	return idx
}

// findFreeIndex finds a free index for insertion
func (c *Collection) findFreeIndex() uint32 {

	// Check if we have space at the end, since if we're inserting a lot of data it's more
	// likely that we're full in the beginning.
	fillSize := len(c.fill)
	if fillSize > 0 {
		if tail := c.fill[fillSize-1]; tail != 0xffffffffffffffff {
			return uint32((fillSize-1)<<6 + bits.TrailingZeros64(^tail))
		}
	}

	// Otherwise, we scan the fill bitmap until we find the first zero. If we don't have it
	// then we set the index at the size of the fill list.
	idx, ok := c.fill.FirstZero()
	if !ok {
		idx = uint32(len(c.fill)) << 6
	}
	return idx
}

// Insert adds an object to a collection and returns the allocated index.
func (c *Collection) Insert(obj Object) (index uint32) {
	c.Query(func(txn *Txn) error {
		index = txn.Insert(obj)
		return nil
	})
	return
}

// InsertWithTTL adds an object to a collection, sets the expiration time
// based on the specified time-to-live and returns the allocated index.
func (c *Collection) InsertWithTTL(obj Object, ttl time.Duration) (index uint32) {
	c.Query(func(txn *Txn) error {
		index = txn.InsertWithTTL(obj, ttl)
		return nil
	})
	return
}

// UpdateAt updates a specific row/column combination and sets the value. It is also
// possible to update during the query, which is much more convenient to use.
func (c *Collection) UpdateAt(idx uint32, columnName string, value interface{}) {
	c.Query(func(txn *Txn) error {
		if cursor, err := txn.cursorFor(columnName); err == nil {
			cursor.idx = idx
			cursor.Update(value)
		}
		return nil
	})
}

// DeleteAt attempts to delete an item at the specified index for this collection. If the item
// exists, it marks at as deleted and returns true, otherwise it returns false.
func (c *Collection) DeleteAt(idx uint32) (deleted bool) {
	c.Query(func(txn *Txn) error {
		deleted = txn.DeleteAt(idx)
		return nil
	})
	return
}

// Count returns the total number of elements in the collection.
func (c *Collection) Count() (count int) {
	c.lock.RLock()
	count = c.count
	c.lock.RUnlock()
	return
}

// CreateColumnsOf registers a set of columns that are present in the target object.
func (c *Collection) CreateColumnsOf(object Object) {
	for k, v := range object {
		c.CreateColumn(k, ForKind(reflect.TypeOf(v).Kind()))
	}
}

// CreateColumn creates a column of a specified type and adds it to the collection.
func (c *Collection) CreateColumn(columnName string, column Column) {
	column.Grow(uint32(c.size))
	c.cols.Store(columnName, columnFor(columnName, column))
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
	index := newIndex(indexName, columnName, fn)
	index.Grow(uint32(c.size))
	c.cols.Store(indexName, index)
	c.cols.Store(columnName, nil, index)

	// If a column with this name already exists, iterate through all of the values
	// that we have in the collection and apply the filter.
	if column, ok := c.cols.Load(columnName); ok {
		fill := index.Index()
		c.fill.Clone(fill)
		fill.Filter(func(x uint32) (match bool) {
			if v, ok := column.Value(x); ok {
				match = fn(v)
			}
			return
		})
	}
	return nil
}

// DropIndex removes the index column with the specified name. If the index with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropIndex(indexName string) {

	// Get the specified index to drop
	column, _ := c.cols.Load(indexName)
	if _, ok := column.Column.(computed); !ok {
		return
	}

	// Figure out the associated column and delete the index from that
	columnName := column.Column.(computed).Column()
	c.cols.DeleteIndex(columnName, indexName)
	c.cols.DeleteColumn(indexName)
}

// Fetch retrieves an object by its handle and returns a Selector for it.
func (c *Collection) Fetch(idx uint32) (Selector, bool) {
	c.lock.RLock()
	contains := c.fill.Contains(idx)
	c.lock.RUnlock()

	// If it's empty or over the sequence, not found
	if idx >= uint32(len(c.fill))<<6 || !contains {
		return Selector{}, false
	}

	return Selector{
		idx: idx,
		col: c,
	}, true
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

// Close closes the collection and clears up all of the resources.
func (c *Collection) Close() error {
	c.cancel()
	return nil
}

// vacuum cleans up the expired objects on a specified interval.
func (c *Collection) vacuum(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			now := time.Now().UnixNano()
			c.Query(func(txn *Txn) error {
				return txn.With(expireColumn).Range(expireColumn, func(v Cursor) bool {
					if expirateAt := v.Int(); expirateAt != 0 && now >= v.Int() {
						v.Delete()
					}
					return true
				})
			})
		}
	}
}

// Replay replays a commit on a collection, applying the changes.
func (c *Collection) Replay(change commit.Commit) error {
	return c.Query(func(txn *Txn) error {
		switch change.Type {
		case commit.TypeInsert:
			txn.inserts = append(txn.inserts, change.Inserts...)
		case commit.TypeDelete:
			txn.deletes = append(txn.deletes, change.Deletes...)

		// Apply updates for this commit
		case commit.TypeStore:

			// If we already  have an existing update queue, append to that
			for i, c := range txn.updates {
				if c.name == change.Column {
					txn.updates[i].update = append(txn.updates[i].update, change.Updates...)
					return nil
				}
			}

			// Create a new update queue, we need to copy all of the updates since both
			// transaction and commits are pooled.
			updates := make([]commit.Update, 0, len(change.Updates))
			updates = append(updates, change.Updates...)

			// Add a new update queue
			txn.updates = append(txn.updates, updateQueue{
				name:   change.Column,
				update: updates,
			})

		default:
			return fmt.Errorf("column: unsupported commit type %v", change.Type)
		}
		return nil
	})
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
	name string    // The column name
	cols []*column // The columns and its computed
}

// Range iterates over columns in the registry.
func (c *columns) Range(fn func(column *column)) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		fn(v.cols[0])
	}
}

// Load loads a column by its name.
func (c *columns) Load(columnName string) (*column, bool) {
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
func (c *columns) LoadWithIndex(columnName string) ([]*column, bool) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if v.name == columnName {
			return v.cols, true
		}
	}
	return nil, false
}

// Store stores a column into the registry.
func (c *columns) Store(columnName string, main *column, index ...*column) {

	// Try to update an existing entry
	columns := c.cols.Load().([]columnEntry)
	for i, v := range columns {
		if v.name != columnName {
			continue
		}

		// If we found an existing entry, update it and we're done
		if main != nil {
			columns[i].cols[0] = main
		}
		if index != nil {
			columns[i].cols = append(columns[i].cols, index...)
		}
		c.cols.Store(columns)

		return
	}

	// No entry found, create a new one
	value := []*column{main}
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
		filtered := make([]*column, 0, cap(columns[i].cols))
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
