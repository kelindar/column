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

const (
	expireColumn = "expire"
	rowColumn    = "row"
)

// Collection represents a collection of objects in a columnar format
type Collection struct {
	count   uint64             // The current count of elements
	txns    *txnPool           // The transaction pool
	lock    sync.RWMutex       // The mutex to guard the fill-list
	slock   *smutex.SMutex128  // The sharded mutex for the collection
	cols    columns            // The map of columns
	fill    bitmap.Bitmap      // The fill-list
	opts    Options            // The options configured
	logger  commit.Logger      // The commit logger for CDC
	record  *commit.Log        // The commit logger for snapshot
	pk      *columnKey         // The primary key column
	cancel  context.CancelFunc // The cancellation function for the context
	commits []uint64           // The array of commit IDs for corresponding chunk
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
		cols:   makeColumns(8),
		txns:   newTxnPool(),
		opts:   options,
		slock:  new(smutex.SMutex128),
		fill:   make(bitmap.Bitmap, 0, options.Capacity>>6),
		logger: options.Writer,
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
	idx := c.findFreeIndex(atomic.AddUint64(&c.count, 1))
	c.fill.Set(idx)
	c.lock.Unlock()
	return idx
}

// free marks the index as free, atomically.
func (c *Collection) free(idx uint32) {
	c.lock.Lock()
	c.fill.Remove(idx)
	atomic.StoreUint64(&c.count, uint64(c.fill.Count()))
	c.lock.Unlock()
	return
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
	if tailAt := int((count - 1) >> 6); fillSize > tailAt {
		if tail := c.fill[tailAt]; tail != 0xffffffffffffffff {
			return uint32((tailAt)<<6 + bits.TrailingZeros64(^tail))
		}
	}

	// Otherwise, we scan the fill bitmap until we find the first zero.
	idx, _ := c.fill.MinZero()
	return idx
}

// Insert executes a mutable cursor transactionally at a new offset.
func (c *Collection) Insert(fn func(Row) error) (index uint32, err error) {
	err = c.Query(func(txn *Txn) (innerErr error) {
		index, innerErr = txn.Insert(fn)
		return
	})
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

// CreateColumnsOf registers a set of columns that are present in the target map.
func (c *Collection) CreateColumnsOf(value map[string]any) error {
	for k, v := range value {
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
	if _, ok := c.cols.Load(columnName); ok {
		return fmt.Errorf("column: unable to create column '%s', already exists", columnName)
	}

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

// CreateTrigger creates an trigger column with a specified name which depends on a given
// column. The trigger function will be applied on the values of the column whenever
// a new row is added, updated or deleted.
func (c *Collection) CreateTrigger(triggerName, columnName string, fn func(r Reader)) error {
	if fn == nil || columnName == "" || triggerName == "" {
		return fmt.Errorf("column: create trigger must specify name, column and function")
	}

	// Prior to creating an index, we should have a column
	column, ok := c.cols.Load(columnName)
	if !ok {
		return fmt.Errorf("column: unable to create trigger, column '%v' does not exist", columnName)
	}

	// Create and add the trigger column
	trigger := newTrigger(triggerName, columnName, fn)
	c.lock.Lock()
	c.cols.Store(triggerName, trigger)
	c.cols.Store(columnName, column, trigger)
	c.lock.Unlock()
	return nil
}

// DropTrigger removes the trigger column with the specified name. If the trigger with this
// name does not exist, this operation is a no-op.
func (c *Collection) DropTrigger(triggerName string) error {
	column, exists := c.cols.Load(triggerName)
	if !exists {
		return fmt.Errorf("column: unable to drop index, index '%v' does not exist", triggerName)
	}

	if _, ok := column.Column.(computed); !ok {
		return fmt.Errorf("column: unable to drop index, '%v' is not a trigger", triggerName)
	}

	// Figure out the associated column and delete the index from that
	columnName := column.Column.(computed).Column()
	c.cols.DeleteIndex(columnName, triggerName)
	c.cols.DeleteColumn(triggerName)
	return nil
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

	// Iterate over all of the values of the target column, chunk by chunk and fill
	// the index accordingly.
	chunks := c.chunks()
	buffer := commit.NewBuffer(c.Count())
	reader := commit.NewReader()
	for chunk := commit.Chunk(0); int(chunk) < chunks; chunk++ {
		if column.Snapshot(chunk, buffer) {
			reader.Seek(buffer)
			index.Apply(chunk, reader)
		}
	}

	return nil
}

func (c *Collection) CreateSortIndex(indexName, columnName string) error {
	if columnName == "" || indexName == "" {
		return fmt.Errorf("column: create index must specify name & column")
	}

	// Prior to creating an index, we should have a column
	column, ok := c.cols.Load(columnName)
	if !ok {
		return fmt.Errorf("column: unable to create index, column '%v' does not exist", columnName)
	}

	// Check to make sure index does not already exist
	_, ok = c.cols.Load(indexName)
	if ok {
		return fmt.Errorf("column: unable to create index, index '%v' already exist", indexName)
	}

	// Create and add the index column,
	index := newSortIndex(indexName, columnName)
	c.lock.Lock()
	// index.Grow(uint32(c.opts.Capacity))
	c.cols.Store(indexName, index)
	c.cols.Store(columnName, column, index)
	c.lock.Unlock()

	// Iterate over all of the values of the target column, chunk by chunk and fill
	// the index accordingly.
	chunks := c.chunks()
	buffer := commit.NewBuffer(c.Count())
	reader := commit.NewReader()
	for chunk := commit.Chunk(0); int(chunk) < chunks; chunk++ {
		if column.Snapshot(chunk, buffer) {
			reader.Seek(buffer)
			index.Apply(chunk, reader)
		}
	}

	return nil
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

// QueryAt jumps at a particular offset in the collection, sets the cursor to the
// provided position and executes given callback fn.
func (c *Collection) QueryAt(idx uint32, fn func(Row) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.QueryAt(idx, fn)
	})
}

// Query creates a transaction which allows for filtering and iteration over the
// columns in this collection. It also allows for individual rows to be modified or
// deleted during iteration (range), but the actual operations will be queued and
// executed after the iteration.
func (c *Collection) Query(fn func(txn *Txn) error) error {
	txn := c.txns.acquire(c)

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

// --------------------------- Primary Key ----------------------------

// InsertKey inserts a row given its corresponding primary key.
func (c *Collection) InsertKey(key string, fn func(Row) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.InsertKey(key, fn)
	})
}

// UpsertKey inserts or updates a row given its corresponding primary key.
func (c *Collection) UpsertKey(key string, fn func(Row) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.UpsertKey(key, fn)
	})
}

// QueryKey queries/updates a row given its corresponding primary key.
func (c *Collection) QueryKey(key string, fn func(Row) error) error {
	return c.Query(func(txn *Txn) error {
		return txn.QueryKey(key, fn)
	})
}

// DeleteKey deletes a row for a given primary key.
func (c *Collection) DeleteKey(key string) error {
	return c.Query(func(txn *Txn) error {
		return txn.DeleteKey(key)
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

// LoadWithIndex loads a column by its name along with the triggers.
func (c *columns) LoadWithIndex(columnName string) ([]*column, bool) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if v.name == columnName {
			return v.cols, true
		}
	}
	return nil, false
}

// LoadIndex loads an index column by its name.
func (c *columns) LoadIndex(indexName string) (*column, bool) {
	cols := c.cols.Load().([]columnEntry)
	for _, v := range cols {
		if v.name == indexName {
			col := v.cols[0]
			return col, col != nil
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
