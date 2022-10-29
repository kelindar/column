// This code was generated, DO NOT EDIT.
// Any changes will be lost if this file is regenerated.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)


// --------------------------- Int ----------------------------

// makeInts creates a new vector for ints
func makeInts(opts ...func(*option[int])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value int) { buffer.PutInt(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []int, opts option[int]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Int()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapInt(opts.Merge(data[offset], r.Int()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// intWriter represents a read-write accessor for int
type intWriter struct {
	numericReader[int]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s intWriter) Set(value int) {
	s.writer.PutInt(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s intWriter) Merge(delta int) {
	s.writer.PutInt(commit.Merge, s.txn.cursor, delta)
}

// Int returns a read-write accessor for int column
func (txn *Txn) Int(columnName string) intWriter {
	return intWriter{
		numericReader: numericReaderFor[int](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Int16 ----------------------------

// makeInt16s creates a new vector for int16s
func makeInt16s(opts ...func(*option[int16])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value int16) { buffer.PutInt16(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []int16, opts option[int16]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Int16()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapInt16(opts.Merge(data[offset], r.Int16()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// int16Writer represents a read-write accessor for int16
type int16Writer struct {
	numericReader[int16]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s int16Writer) Set(value int16) {
	s.writer.PutInt16(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s int16Writer) Merge(delta int16) {
	s.writer.PutInt16(commit.Merge, s.txn.cursor, delta)
}

// Int16 returns a read-write accessor for int16 column
func (txn *Txn) Int16(columnName string) int16Writer {
	return int16Writer{
		numericReader: numericReaderFor[int16](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Int32 ----------------------------

// makeInt32s creates a new vector for int32s
func makeInt32s(opts ...func(*option[int32])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value int32) { buffer.PutInt32(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []int32, opts option[int32]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Int32()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapInt32(opts.Merge(data[offset], r.Int32()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// int32Writer represents a read-write accessor for int32
type int32Writer struct {
	numericReader[int32]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s int32Writer) Set(value int32) {
	s.writer.PutInt32(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s int32Writer) Merge(delta int32) {
	s.writer.PutInt32(commit.Merge, s.txn.cursor, delta)
}

// Int32 returns a read-write accessor for int32 column
func (txn *Txn) Int32(columnName string) int32Writer {
	return int32Writer{
		numericReader: numericReaderFor[int32](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Int64 ----------------------------

// makeInt64s creates a new vector for int64s
func makeInt64s(opts ...func(*option[int64])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value int64) { buffer.PutInt64(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []int64, opts option[int64]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Int64()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapInt64(opts.Merge(data[offset], r.Int64()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// int64Writer represents a read-write accessor for int64
type int64Writer struct {
	numericReader[int64]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s int64Writer) Set(value int64) {
	s.writer.PutInt64(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s int64Writer) Merge(delta int64) {
	s.writer.PutInt64(commit.Merge, s.txn.cursor, delta)
}

// Int64 returns a read-write accessor for int64 column
func (txn *Txn) Int64(columnName string) int64Writer {
	return int64Writer{
		numericReader: numericReaderFor[int64](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Uint ----------------------------

// makeUints creates a new vector for uints
func makeUints(opts ...func(*option[uint])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value uint) { buffer.PutUint(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []uint, opts option[uint]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Uint()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapUint(opts.Merge(data[offset], r.Uint()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// uintWriter represents a read-write accessor for uint
type uintWriter struct {
	numericReader[uint]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uintWriter) Set(value uint) {
	s.writer.PutUint(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s uintWriter) Merge(delta uint) {
	s.writer.PutUint(commit.Merge, s.txn.cursor, delta)
}

// Uint returns a read-write accessor for uint column
func (txn *Txn) Uint(columnName string) uintWriter {
	return uintWriter{
		numericReader: numericReaderFor[uint](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Uint16 ----------------------------

// makeUint16s creates a new vector for uint16s
func makeUint16s(opts ...func(*option[uint16])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value uint16) { buffer.PutUint16(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []uint16, opts option[uint16]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Uint16()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapUint16(opts.Merge(data[offset], r.Uint16()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// uint16Writer represents a read-write accessor for uint16
type uint16Writer struct {
	numericReader[uint16]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uint16Writer) Set(value uint16) {
	s.writer.PutUint16(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s uint16Writer) Merge(delta uint16) {
	s.writer.PutUint16(commit.Merge, s.txn.cursor, delta)
}

// Uint16 returns a read-write accessor for uint16 column
func (txn *Txn) Uint16(columnName string) uint16Writer {
	return uint16Writer{
		numericReader: numericReaderFor[uint16](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Uint32 ----------------------------

// makeUint32s creates a new vector for uint32s
func makeUint32s(opts ...func(*option[uint32])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value uint32) { buffer.PutUint32(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []uint32, opts option[uint32]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Uint32()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapUint32(opts.Merge(data[offset], r.Uint32()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// uint32Writer represents a read-write accessor for uint32
type uint32Writer struct {
	numericReader[uint32]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uint32Writer) Set(value uint32) {
	s.writer.PutUint32(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s uint32Writer) Merge(delta uint32) {
	s.writer.PutUint32(commit.Merge, s.txn.cursor, delta)
}

// Uint32 returns a read-write accessor for uint32 column
func (txn *Txn) Uint32(columnName string) uint32Writer {
	return uint32Writer{
		numericReader: numericReaderFor[uint32](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Uint64 ----------------------------

// makeUint64s creates a new vector for uint64s
func makeUint64s(opts ...func(*option[uint64])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value uint64) { buffer.PutUint64(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []uint64, opts option[uint64]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Uint64()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapUint64(opts.Merge(data[offset], r.Uint64()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// uint64Writer represents a read-write accessor for uint64
type uint64Writer struct {
	numericReader[uint64]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s uint64Writer) Set(value uint64) {
	s.writer.PutUint64(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s uint64Writer) Merge(delta uint64) {
	s.writer.PutUint64(commit.Merge, s.txn.cursor, delta)
}

// Uint64 returns a read-write accessor for uint64 column
func (txn *Txn) Uint64(columnName string) uint64Writer {
	return uint64Writer{
		numericReader: numericReaderFor[uint64](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Float32 ----------------------------

// makeFloat32s creates a new vector for float32s
func makeFloat32s(opts ...func(*option[float32])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value float32) { buffer.PutFloat32(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []float32, opts option[float32]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Float32()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapFloat32(opts.Merge(data[offset], r.Float32()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// float32Writer represents a read-write accessor for float32
type float32Writer struct {
	numericReader[float32]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s float32Writer) Set(value float32) {
	s.writer.PutFloat32(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s float32Writer) Merge(delta float32) {
	s.writer.PutFloat32(commit.Merge, s.txn.cursor, delta)
}

// Float32 returns a read-write accessor for float32 column
func (txn *Txn) Float32(columnName string) float32Writer {
	return float32Writer{
		numericReader: numericReaderFor[float32](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}


// --------------------------- Float64 ----------------------------

// makeFloat64s creates a new vector for float64s
func makeFloat64s(opts ...func(*option[float64])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value float64) { buffer.PutFloat64(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []float64, opts option[float64]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Float64()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.SwapFloat64(opts.Merge(data[offset], r.Float64()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// float64Writer represents a read-write accessor for float64
type float64Writer struct {
	numericReader[float64]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s float64Writer) Set(value float64) {
	s.writer.PutFloat64(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s float64Writer) Merge(delta float64) {
	s.writer.PutFloat64(commit.Merge, s.txn.cursor, delta)
}

// Float64 returns a read-write accessor for float64 column
func (txn *Txn) Float64(columnName string) float64Writer {
	return float64Writer{
		numericReader: numericReaderFor[float64](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}

