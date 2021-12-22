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
	"github.com/kelindar/smutex"
)

// Object represents a single object
type Object = map[string]interface{}

const (
	expireColumn = "expire"
	rowColumn    = "row"
)

// Collection represents a collection of objects in a columnar format
type Collection struct {
	count   uint64                  // The current count of elements
	txns    *txnPool                // The transaction pool
	lock    sync.RWMutex            // The mutex to guard the fill-list
	slock   *smutex.SMutex128       // The sharded mutex for the collection
	cols    columns                 // The map of columns
	fill    bitmap.Bitmap           // The fill-list
	opts    Options                 // The options configured
	codec   codec                   // The compression codec
	logger  commit.Logger           // The commit logger for CDC
	record  *commit.Log             // The commit logger for snapshot
	pk      *columnKey              // The primary key column
	cancel  context.CancelFunc      // The cancellation function for the context
	commits map[commit.Chunk]uint64 // The array of commit IDs for corresponding chunk
}

// Options represents the options for a collection.
type Options struct {
	Capacity int           // The initial capacity when creating columns
	Writer   commit.Logger // The writer for the commit log (optional)
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
		cols:    makeColumns(8),
		txns:    newTxnPool(),
		opts:    options,
		slock:   new(smutex.SMutex128),
		fill:    make(bitmap.Bitmap, 0, options.Capacity>>6),
		logger:  options.Writer,
		codec:   newCodec(&options),
		cancel:  cancel,
		commits: make(map[commit.Chunk]uint64, 64),
	}

	// Create an expiration column and start the cleanup goroutine
	store.CreateColumn(expireColumn, ForInt64())
	go store.vacuum(ctx, options.Vacuum)
	return store
}

// next finds the next free index in the collection, atomically.
func (c *Collection) next() uint32 {
	c.lock.Lock()
	idx := c.findFreeIndex(atomic.AddUint64(&c.count, 1))
	c.fill.Set(idx)
	c.lock.Unlock()
	return idx
}

// findFreeIndex finds a free index for insertion
func (c *Collection) findFreeIndex(count uint64) uint32 {
	fillSize := len(c.fill)

	// If the collection is full, we need to add at the end
	if count > uint64(fillSize)<<6 {
		return uint32(len(c.fill)) << 6
	}

	// Check if we have space at the end, since if we're inserting a lot of data it's more
	// likely that we're full in the beginning.
	if fillSize > 0 {
		if tail := c.fill[fillSize-1]; tail != 0xffffffffffffffff {
			return uint32((fillSize-1)<<6 + bits.TrailingZeros64(^tail))
		}
	}

	// Otherwise, we scan the fill bitmap until we find the first zero.
	idx, _ := c.fill.MinZero()
	return idx
}

// InsertObject adds an object to a collection and returns the allocated index.
func (c *Collection) InsertObject(obj Object) (index uint32) {
	c.Query(func(txn *Txn) error {
		index, _ = txn.InsertObject(obj)
		return nil
	})
	return
}

// InsertObjectWithTTL adds an object to a collection, sets the expiration time
// based on the specified time-to-live and returns the allocated index.
func (c *Collection) InsertObjectWithTTL(obj Object, ttl time.Duration) (index uint32) {
	c.Query(func(txn *Txn) error {
		index, _ = txn.InsertObjectWithTTL(obj, ttl)
		return nil
	})
	return
}

// Insert executes a mutable cursor trasactionally at a new offset.
func (c *Collection) Insert(columnName string, fn func(v Cursor) error) (index uint32, err error) {
	err = c.Query(func(txn *Txn) (innerErr error) {
		index, innerErr = txn.Insert(columnName, fn)
		return
	})
	return
}

// InsertWithTTL executes a mutable cursor trasactionally at a new offset and sets the expiration time
// based on the specified time-to-live and returns the allocated index.
func (c *Collection) InsertWithTTL(columnName string, ttl time.Duration, fn func(v Cursor) error) (index uint32, err error) {
	err = c.Query(func(txn *Txn) (innerErr error) {
		index, innerErr = txn.InsertWithTTL(columnName, ttl, fn)
		return
	})
	return
}

// UpdateAt updates a specific row by initiating a separate transaction for the update.
func (c *Collection) UpdateAt(idx uint32, columnName string, fn func(v Cursor) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.UpdateAt(idx, columnName, fn)
	})
}

// UpdateAtKey updates a specific row by initiating a separate transaction for the update.
func (c *Collection) UpdateAtKey(key, columnName string, fn func(v Cursor) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.UpdateAtKey(key, columnName, fn)
	})
}

// SelectAt performs a selection on a specific row specified by its index. It returns
// a boolean value indicating whether an element is present at the index or not.
func (c *Collection) SelectAt(idx uint32, fn func(v Selector)) bool {
	chunk := uint(idx >> chunkShift)
	if idx >= uint32(len(c.fill))<<6 || !c.fill.Contains(idx) {
		return false
	}

	// Lock the chunk which we are about to read and call the selector delegate
	c.slock.RLock(chunk)
	fn(Selector{idx: idx, col: c})
	c.slock.RUnlock(chunk)
	return true
}

// SelectAtKey performs a selection on a specific row specified by its key. It returns
// a boolean value indicating whether an element is present at the key or not.
func (c *Collection) SelectAtKey(key string, fn func(v Selector)) (found bool) {
	if c.pk == nil {
		return false
	}

	if idx, ok := c.pk.OffsetOf(key); ok {
		found = c.SelectAt(idx, fn)
	}
	return
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
	return int(atomic.LoadUint64(&c.count))
}

// createColumnKey attempts to create a primary key column
func (c *Collection) createColumnKey(columnName string, column *columnKey) error {
	if c.pk != nil {
		return fmt.Errorf("column: unable to create key column '%s', another one exists", columnName)
	}

	c.pk = column
	c.pk.name = columnName
	return nil
}

// CreateColumnsOf registers a set of columns that are present in the target object.
func (c *Collection) CreateColumnsOf(object Object) error {
	for k, v := range object {
		column, err := ForKind(reflect.TypeOf(v).Kind())
		if err != nil {
			return err
		}

		if err := c.CreateColumn(k, column); err != nil {
			return err
		}
	}
	return nil
}

// CreateColumn creates a column of a specified type and adds it to the collection.
func (c *Collection) CreateColumn(columnName string, column Column) error {
	column.Grow(uint32(c.opts.Capacity))
	c.cols.Store(columnName, columnFor(columnName, column))

	// If necessary, create a primary key column
	if pk, ok := column.(*columnKey); ok {
		return c.createColumnKey(columnName, pk)
	}
	return nil
}

// DropColumn removes the column (or an index) with the specified name. If the column with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropColumn(columnName string) {
	c.cols.DeleteColumn(columnName)
}

// CreateIndex creates an index column with a specified name which depends on a given
// column. The index function will be applied on the values of the column whenever
// a new row is added or updated.
func (c *Collection) CreateIndex(indexName, columnName string, fn func(r Reader) bool) error {
	if fn == nil || columnName == "" || indexName == "" {
		return fmt.Errorf("column: create index must specify name, column and function")
	}

	// Prior to creating an index, we should have a column
	column, ok := c.cols.Load(columnName)
	if !ok {
		return fmt.Errorf("column: unable to create index, column '%v' does not exist", columnName)
	}

	// Create and add the index column,
	index := newIndex(indexName, columnName, fn)
	c.lock.Lock()
	index.Grow(uint32(c.opts.Capacity))
	c.cols.Store(indexName, index)
	c.cols.Store(columnName, column, index)
	c.lock.Unlock()

	// If a column with this name already exists, iterate through all of the values
	// that we have in the collection and apply the filter.
	return c.Query(func(txn *Txn) error {
		impl := index.Column.(*columnIndex)
		return txn.With(columnName).Range(columnName, func(v Cursor) {
			impl.Update(&v)
		})
	})
}

// DropIndex removes the index column with the specified name. If the index with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropIndex(indexName string) error {
	column, exists := c.cols.Load(indexName)
	if !exists {
		return fmt.Errorf("column: unable to drop index, index '%v' does not exist", indexName)
	}

	if _, ok := column.Column.(computed); !ok {
		return fmt.Errorf("column: unable to drop index, '%v' is not an index", indexName)
	}

	// Figure out the associated column and delete the index from that
	columnName := column.Column.(computed).Column()
	c.cols.DeleteIndex(columnName, indexName)
	c.cols.DeleteColumn(indexName)
	return nil
}

// Query creates a transaction which allows for filtering and iteration over the
// columns in this collection. It also allows for individual rows to be modified or
// deleted during iteration (range), but the actual operations will be queued and
// executed after the iteration.
func (c *Collection) Query(fn func(txn *Txn) error) error {
	c.lock.RLock()
	txn := c.txns.acquire(c)
	c.lock.RUnlock()

	// Execute the query and keep the error for later
	if err := fn(txn); err != nil {
		txn.rollback()
		c.txns.release(txn)
		return err
	}

	// Now that the iteration has finished, we can range over the pending action
	// queue and apply all of the actions that were requested by the Selector.
	txn.commit()
	c.txns.release(txn)
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
			now := int(time.Now().UnixNano())
			c.Query(func(txn *Txn) error {
				return txn.With(expireColumn).Range(expireColumn, func(v Cursor) {
					if expirateAt := v.Int(); expirateAt != 0 && now >= v.Int() {
						v.Delete()
					}
				})
			})
		}
	}
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

// Count returns the number of columns, excluding indexes.
func (c *columns) Count() (count int) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if !v.cols[0].IsIndex() {
			count++
		}
	}
	return
}

// Range iterates over columns in the registry. This is faster than RangeUntil
// method.
func (c *columns) Range(fn func(column *column)) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		fn(v.cols[0])
	}
}

// RangeUntil iterates over columns in the registry until an error occurs.
func (c *columns) RangeUntil(fn func(column *column) error) error {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if err := fn(v.cols[0]); err != nil {
			return err
		}
	}
	return nil
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
	index, _ := c.Load(indexName)
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
