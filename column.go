// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

//go:generate go run ./codegen/main.go

package column

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/kelindar/simd"
)

// columnType represents a type of a column.
type columnType uint8

const (
	typeGeneric = columnType(0)      // Generic column, every column should support this
	typeNumeric = columnType(1 << 0) // Numeric column supporting float64, int64 or uint64
	typeTextual = columnType(1 << 1) // Textual column supporting strings
)

// typeOf resolves all supported types of the column
func typeOf(column Column) (typ columnType) {
	if _, ok := column.(Numeric); ok {
		typ = typ | typeNumeric
	}
	if _, ok := column.(Textual); ok {
		typ = typ | typeTextual
	}
	return
}

// --------------------------- Contracts ----------------------------

// Column represents a column implementation
type Column interface {
	Grow(idx uint32)
	Apply(commit.Chunk, *commit.Reader)
	Value(idx uint32) (interface{}, bool)
	Contains(idx uint32) bool
	Index(commit.Chunk) bitmap.Bitmap
	Snapshot(chunk commit.Chunk, dst *commit.Buffer)
}

// Numeric represents a column that stores numbers.
type Numeric interface {
	Column
	LoadFloat64(uint32) (float64, bool)
	LoadUint64(uint32) (uint64, bool)
	LoadInt64(uint32) (int64, bool)
	FilterFloat64(commit.Chunk, bitmap.Bitmap, func(v float64) bool)
	FilterUint64(commit.Chunk, bitmap.Bitmap, func(v uint64) bool)
	FilterInt64(commit.Chunk, bitmap.Bitmap, func(v int64) bool)
}

// Textual represents a column that stores strings.
type Textual interface {
	Column
	LoadString(uint32) (string, bool)
	FilterString(commit.Chunk, bitmap.Bitmap, func(v string) bool)
}

// --------------------------- Constructors ----------------------------

// Various column constructor functions for a specific types.
var (
	ForString  = makeStrings
	ForFloat32 = makeFloat32s
	ForFloat64 = makeFloat64s
	ForInt     = makeInts
	ForInt16   = makeInt16s
	ForInt32   = makeInt32s
	ForInt64   = makeInt64s
	ForUint    = makeUints
	ForUint16  = makeUint16s
	ForUint32  = makeUint32s
	ForUint64  = makeUint64s
	ForBool    = makeBools
	ForEnum    = makeEnum
	ForKey     = makeKey
)

// ForKind creates a new column instance for a specified reflect.Kind
func ForKind(kind reflect.Kind) (Column, error) {
	switch kind {
	case reflect.Float32:
		return makeFloat32s(), nil
	case reflect.Float64:
		return makeFloat64s(), nil
	case reflect.Int:
		return makeInts(), nil
	case reflect.Int16:
		return makeInt16s(), nil
	case reflect.Int32:
		return makeInt32s(), nil
	case reflect.Int64:
		return makeInt64s(), nil
	case reflect.Uint:
		return makeUints(), nil
	case reflect.Uint16:
		return makeUint16s(), nil
	case reflect.Uint32:
		return makeUint32s(), nil
	case reflect.Uint64:
		return makeUint64s(), nil
	case reflect.Bool:
		return makeBools(), nil
	case reflect.String:
		return makeStrings(), nil
	default:
		return nil, fmt.Errorf("column: unsupported column kind (%v)", kind)
	}
}

// --------------------------- Generic Options ----------------------------

type optionType interface {
	simd.Number | ~string
}

// optInt represents options for variouos columns.
type option[T optionType] struct {
	Merge func(value, delta T) T
}

// configure applies options
func configure[T optionType](opts []func(*option[T]), dst option[T]) option[T] {
	for _, fn := range opts {
		fn(&dst)
	}
	return dst
}

// WithMerge sets an optional merge function that allows you to merge a delta value to
// an existing value, atomically. The operation is performed transactionally.
func WithMerge[T optionType](fn func(value, delta T) T) func(*option[T]) {
	return func(v *option[T]) {
		v.Merge = fn
	}
}

// --------------------------- Column ----------------------------

// column represents a column wrapper that synchronizes operations
type column struct {
	Column
	lock sync.RWMutex // The lock to protect the entire column
	kind columnType   // The type of the colum
	name string       // The name of the column
}

// columnFor creates a synchronized column for a column implementation
func columnFor(name string, v Column) *column {
	return &column{
		kind:   typeOf(v),
		name:   name,
		Column: v,
	}
}

// IsIndex returns whether the column is an index
func (c *column) IsIndex() bool {
	_, ok := c.Column.(*columnIndex)
	return ok
}

// IsNumeric checks whether a column type supports certain numerical operations.
func (c *column) IsNumeric() bool {
	return (c.kind & typeNumeric) == typeNumeric
}

// IsTextual checks whether a column type supports certain string operations.
func (c *column) IsTextual() bool {
	return (c.kind & typeTextual) == typeTextual
}

// Grow grows the size of the column
func (c *column) Grow(idx uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.Column.Grow(idx)
}

// Apply performs a series of operations on a column.
func (c *column) Apply(chunk commit.Chunk, r *commit.Reader) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	r.Rewind()
	c.Column.Apply(chunk, r)
}

// Index loads the appropriate column index for a given chunk
func (c *column) Index(chunk commit.Chunk) bitmap.Bitmap {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.Column.Index(chunk)
}

// Snapshot takes a snapshot of a column, skipping indexes
func (c *column) Snapshot(chunk commit.Chunk, buffer *commit.Buffer) bool {
	if c.IsIndex() {
		return false
	}

	buffer.Reset(c.name)
	c.Column.Snapshot(chunk, buffer)
	return true
}

// Value retrieves a value at a specified index
func (c *column) Value(idx uint32) (v interface{}, ok bool) {
	v, ok = c.Column.Value(idx)
	return
}

// --------------------------- Accessor ----------------------------

// anyReader represents a read-only accessor for any value
type anyReader struct {
	cursor *uint32
	reader Column
}

// Get loads the value at the current transaction cursor
func (s anyReader) Get() (any, bool) {
	return s.reader.Value(*s.cursor)
}

// anyReaderFor creates a new any reader
func anyReaderFor(txn *Txn, columnName string) anyReader {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	return anyReader{
		cursor: &txn.cursor,
		reader: column.Column,
	}
}

// anyWriter represents read-write accessor for any column type
type anyWriter struct {
	anyReader
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s anyWriter) Set(value any) {
	s.writer.PutAny(commit.Put, *s.cursor, value)
}

// Any returns a column accessor
func (txn *Txn) Any(columnName string) anyWriter {
	return anyWriter{
		anyReader: anyReaderFor(txn, columnName),
		writer:    txn.bufferFor(columnName),
	}
}

// --------------------------- segment list ----------------------------

// Chunks represents a chunked array storage
type chunks[T any] []struct {
	fill bitmap.Bitmap // The fill-list
	data []T           // The actual values
}

// chunkAt loads the fill and data list at a particular chunk
func (s chunks[T]) chunkAt(chunk commit.Chunk) (bitmap.Bitmap, []T) {
	fill := s[chunk].fill
	data := s[chunk].data
	return fill, data
}

// Grow grows a segment list
func (s *chunks[T]) Grow(idx uint32) {
	chunk := int(commit.ChunkAt(idx))
	for i := len(*s); i <= chunk; i++ {
		*s = append(*s, struct {
			fill bitmap.Bitmap
			data []T
		}{
			fill: make(bitmap.Bitmap, chunkSize/64),
			data: make([]T, chunkSize),
		})
	}
}

// Index returns the fill list for the segment
func (s chunks[T]) Index(chunk commit.Chunk) (fill bitmap.Bitmap) {
	if int(chunk) < len(s) {
		fill = s[chunk].fill
	}
	return
}
