package column

import (
	"fmt"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

type numericType interface {
	~int | ~int16 | ~int32 | ~int64 | ~uint | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// readNumber is a helper function for point reads
func readNumber[T numericType](txn *Txn, columnName string) (value T, found bool) {
	if column, ok := txn.columnAt(columnName); ok {
		if rdr, ok := column.Column.(*numericColumn[T]); ok {
			value, found = rdr.load(txn.cursor)
		}
	}
	return
}

// --------------------------- Generic Column ----------------------------

// numericColumn represents a numeric column
type numericColumn[T numericType] struct {
	chunks[T]
	write func(*commit.Buffer, uint32, T)
	apply func(*commit.Reader, bitmap.Bitmap, []T)
}

// makeNumeric creates a new vector for numericTypes
func makeNumeric[T numericType](
	write func(*commit.Buffer, uint32, T),
	apply func(*commit.Reader, bitmap.Bitmap, []T),
) *numericColumn[T] {
	return &numericColumn[T]{
		chunks: make(chunks[T], 0, 4),
		write:  write,
		apply:  apply,
	}
}

// --------------------------- Accessors ----------------------------

// Contains checks whether the column has a value at a specified index.
func (c *numericColumn[T]) Contains(idx uint32) bool {
	chunk := commit.ChunkAt(idx)
	return c.chunks[chunk].fill.Contains(idx - chunk.Min())
}

// load retrieves a float64 value at a specified index
func (c *numericColumn[T]) load(idx uint32) (v T, ok bool) {
	chunk := commit.ChunkAt(idx)
	index := idx - chunk.Min()
	if int(chunk) < len(c.chunks) && c.chunks[chunk].fill.Contains(index) {
		v, ok = c.chunks[chunk].data[index], true
	}
	return
}

// Value retrieves a value at a specified index
func (c *numericColumn[T]) Value(idx uint32) (any, bool) {
	return c.load(idx)
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *numericColumn[T]) LoadFloat64(idx uint32) (float64, bool) {
	v, ok := c.load(idx)
	return float64(v), ok
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *numericColumn[T]) LoadInt64(idx uint32) (int64, bool) {
	v, ok := c.load(idx)
	return int64(v), ok
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *numericColumn[T]) LoadUint64(idx uint32) (uint64, bool) {
	v, ok := c.load(idx)
	return uint64(v), ok
}

// --------------------------- Filtering ----------------------------

// filterNumbers filters down the values based on the specified predicate.
func filterNumbers[T, C numericType](column *numericColumn[T], chunk commit.Chunk, index bitmap.Bitmap, predicate func(C) bool) {
	if int(chunk) < len(column.chunks) {
		fill, data := column.chunkAt(chunk)
		index.And(fill)
		index.Filter(func(idx uint32) bool {
			return predicate(C(data[idx]))
		})
	}
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *numericColumn[T]) FilterFloat64(chunk commit.Chunk, index bitmap.Bitmap, predicate func(float64) bool) {
	filterNumbers(c, chunk, index, predicate)
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *numericColumn[T]) FilterInt64(chunk commit.Chunk, index bitmap.Bitmap, predicate func(int64) bool) {
	filterNumbers(c, chunk, index, predicate)
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *numericColumn[T]) FilterUint64(chunk commit.Chunk, index bitmap.Bitmap, predicate func(uint64) bool) {
	filterNumbers(c, chunk, index, predicate)
}

// --------------------------- Apply & Snapshot ----------------------------

// Apply applies a set of operations to the column.
func (c *numericColumn[T]) Apply(chunk commit.Chunk, r *commit.Reader) {
	fill, data := c.chunkAt(chunk)
	c.apply(r, fill, data)
}

// Snapshot writes the entire column into the specified destination buffer
func (c *numericColumn[T]) Snapshot(chunk commit.Chunk, dst *commit.Buffer) {
	fill, data := c.chunkAt(chunk)
	fill.Range(func(x uint32) {
		c.write(dst, chunk.Min()+x, data[x])
	})
}

// --------------------------- Reader/Writer ----------------------------

// numericReader represents a read-only accessor for numericTypes
type numericReader[T numericType] struct {
	reader *numericColumn[T]
	txn    *Txn
}

// Get loads the value at the current transaction cursor
func (s numericReader[T]) Get() (T, bool) {
	return s.reader.load(s.txn.cursor)
}

// Sum computes the sum of the column values selected by the transaction
func (s numericReader[T]) Sum() (r T) {
	s.txn.Range(func(idx uint32) {
		v, _ := s.Get()
		r += v
	})
	return
}

// numericReaderFor creates a new numeric reader
func numericReaderFor[T numericType](txn *Txn, columnName string) numericReader[T] {
	column, ok := txn.columnAt(columnName)
	if !ok {
		panic(fmt.Errorf("column: column '%s' does not exist", columnName))
	}

	reader, ok := column.Column.(*numericColumn[T])
	if !ok {
		panic(fmt.Errorf("column: column '%s' is not of type %T", columnName, float64(0)))
	}

	return numericReader[T]{
		reader: reader,
		txn:    txn,
	}
}
