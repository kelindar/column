// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"fmt"
	"math"

	"github.com/kelindar/bitmap"
)

const (
	size0    = 0      // 0 byte in size
	size2    = 1 << 4 // 2 bytes in size
	size4    = 2 << 4 // 4 bytes in size
	size8    = 3 << 4 // 8 bytes in size
	isNext   = 1 << 7 // is immediate next
	isString = 1 << 6 // is variable-size string
)

// --------------------------- Operation Type ----------------------------

// OpType represents a type of an operation.
type OpType uint8

// Various update operations supported.
const (
	Delete   OpType = 0 // Delete deletes an entire row or a set of rows
	Insert   OpType = 1 // Insert inserts a new row or a set of rows
	PutFalse OpType = 0 // PutFalse is a combination of Put+False for boolean values
	PutTrue  OpType = 2 // PutTrue is a combination of Put+True for boolean values
	Put      OpType = 2 // Put stores a value regardless of a previous value
	Add      OpType = 3 // Add increments the current stored value by the amount
)

// --------------------------- Delta log ----------------------------

// Buffer represents a buffer of delta operations.
type Buffer struct {
	last   int32    // The last offset written
	chunk  Chunk    // The current chunk
	buffer []byte   // The destination buffer
	chunks []header // The offsets of chunks
	_      [8]byte  // padding
	Column string   // The column for the queue
}

// header represents a chunk metadata header.
type header struct {
	Chunk Chunk  // The chunk number
	Start uint32 // The offset at which the chunk starts in the buffer
	Value uint32 // The previous offset value for delta
}

// NewBuffer creates a new queue to store individual operations.
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		chunk:  math.MaxUint32,
		buffer: make([]byte, 0, capacity),
	}
}

// Clone clones the buffer
func (b *Buffer) Clone() *Buffer {
	buffer := make([]byte, len(b.buffer))
	copy(buffer, b.buffer)

	chunks := make([]header, 0, len(b.chunks))
	chunks = append(chunks, b.chunks...)
	return &Buffer{
		Column: b.Column,
		buffer: buffer,
		chunks: chunks,
		last:   b.last,
		chunk:  b.chunk,
	}
}

// Reset resets the queue so it can be reused.
func (b *Buffer) Reset(column string) {
	b.last = 0
	b.chunk = math.MaxUint32
	b.buffer = b.buffer[:0]
	b.chunks = b.chunks[:0]
	b.Column = column
}

// IsEmpty returns whether the buffer is empty or not.
func (b *Buffer) IsEmpty() bool {
	return len(b.buffer) == 0
}

// Range iterates over the chunks present in the buffer
func (b *Buffer) RangeChunks(fn func(chunk Chunk)) {
	for _, c := range b.chunks {
		fn(c.Chunk)
	}
}

// PutAny appends a supported value onto the buffer.
func (b *Buffer) PutAny(op OpType, idx uint32, value interface{}) {
	switch v := value.(type) {
	case uint64:
		b.PutUint64(idx, v)
	case uint32:
		b.PutUint32(idx, v)
	case uint16:
		b.PutUint16(idx, v)
	case uint8:
		b.PutUint16(idx, uint16(v))
	case int64:
		b.PutInt64(idx, v)
	case int32:
		b.PutInt32(idx, v)
	case int16:
		b.PutInt16(idx, v)
	case int8:
		b.PutInt16(idx, int16(v))
	case string:
		b.PutString(op, idx, v)
	case []byte:
		b.PutBytes(op, idx, v)
	case float32:
		b.PutFloat32(idx, v)
	case float64:
		b.PutFloat64(idx, v)
	case int:
		b.PutInt64(idx, int64(v))
	case uint:
		b.PutUint64(idx, uint64(v))
	case bool:
		b.PutBool(idx, v)
	case nil:
		b.PutOperation(op, idx)
	default:
		panic(fmt.Errorf("column: unsupported type (%T)", value))
	}
}

// --------------------------- Numbers ----------------------------

// PutUint64 appends an uint64 value.
func (b *Buffer) PutUint64(idx uint32, value uint64) {
	b.writeUint64(Put, idx, value)
}

// PutUint32 appends an uint32 value.
func (b *Buffer) PutUint32(idx uint32, value uint32) {
	b.writeUint32(Put, idx, value)
}

// PutUint16 appends an uint16 value.
func (b *Buffer) PutUint16(idx uint32, value uint16) {
	b.writeUint16(Put, idx, value)
}

// PutUint appends a uint64 value.
func (b *Buffer) PutUint(idx uint32, value uint) {
	b.writeUint64(Put, idx, uint64(value))
}

// PutInt64 appends an int64 value.
func (b *Buffer) PutInt64(idx uint32, value int64) {
	b.writeUint64(Put, idx, uint64(value))
}

// PutInt32 appends an int32 value.
func (b *Buffer) PutInt32(idx uint32, value int32) {
	b.writeUint32(Put, idx, uint32(value))
}

// PutInt16 appends an int16 value.
func (b *Buffer) PutInt16(idx uint32, value int16) {
	b.writeUint16(Put, idx, uint16(value))
}

// PutInt appends a int64 value.
func (b *Buffer) PutInt(idx uint32, value int) {
	b.writeUint64(Put, idx, uint64(value))
}

// PutFloat64 appends a float64 value.
func (b *Buffer) PutFloat64(idx uint32, value float64) {
	b.writeUint64(Put, idx, math.Float64bits(value))
}

// PutFloat32 appends an int32 value.
func (b *Buffer) PutFloat32(idx uint32, value float32) {
	b.writeUint32(Put, idx, math.Float32bits(value))
}

// PutNumber appends a float64 value.
func (b *Buffer) PutNumber(idx uint32, value float64) {
	b.writeUint64(Put, idx, math.Float64bits(value))
}

// --------------------------- Additions ----------------------------

// AddUint64 appends an addition of uint64 value.
func (b *Buffer) AddUint64(idx uint32, value uint64) {
	b.writeUint64(Add, idx, value)
}

// AddUint32 appends an addition of uint32 value.
func (b *Buffer) AddUint32(idx uint32, value uint32) {
	b.writeUint32(Add, idx, value)
}

// AddUint16 appends an addition of uint16 value.
func (b *Buffer) AddUint16(idx uint32, value uint16) {
	b.writeUint16(Add, idx, value)
}

// AddUint appends an addition of uint64 value.
func (b *Buffer) AddUint(idx uint32, value uint) {
	b.writeUint64(Add, idx, uint64(value))
}

// AddInt64 appends an addition of int64 value.
func (b *Buffer) AddInt64(idx uint32, value int64) {
	b.writeUint64(Add, idx, uint64(value))
}

// AddInt32 appends an addition of int32 value.
func (b *Buffer) AddInt32(idx uint32, value int32) {
	b.writeUint32(Add, idx, uint32(value))
}

// AddInt16 appends an addition of int16 value.
func (b *Buffer) AddInt16(idx uint32, value int16) {
	b.writeUint16(Add, idx, uint16(value))
}

// AddInt appends an addition of int64 value.
func (b *Buffer) AddInt(idx uint32, value int) {
	b.writeUint64(Add, idx, uint64(value))
}

// AddFloat64 appends a float64 value.
func (b *Buffer) AddFloat64(idx uint32, value float64) {
	b.writeUint64(Add, idx, math.Float64bits(value))
}

// AddFloat32 appends an addition of int32 value.
func (b *Buffer) AddFloat32(idx uint32, value float32) {
	b.writeUint32(Add, idx, math.Float32bits(value))
}

// AddNumber appends an addition of float64 value.
func (b *Buffer) AddNumber(idx uint32, value float64) {
	b.writeUint64(Add, idx, math.Float64bits(value))
}

// --------------------------- Others ----------------------------

// PutOperation appends an operation type without a value.
func (b *Buffer) PutOperation(op OpType, idx uint32) {
	delta := b.writeChunk(idx)
	switch delta {
	case 1:
		b.buffer = append(b.buffer, byte(op)|size0|isNext)
	default:
		b.buffer = append(b.buffer, byte(op)|size0)
		b.writeOffset(uint32(delta))
	}
}

// PutBool appends a boolean value.
func (b *Buffer) PutBool(idx uint32, value bool) {

	// let the compiler do its magic: https://github.com/golang/go/issues/6011
	op := PutFalse
	if value {
		op = PutTrue
	}

	b.PutOperation(op, idx)
}

// PutBytes appends a binary value.
func (b *Buffer) PutBytes(op OpType, idx uint32, value []byte) {
	delta := b.writeChunk(idx)
	length := len(value) // max 65K slices
	switch delta {
	case 1:
		b.buffer = append(b.buffer,
			byte(op)|size2|isString|isNext,
			byte(length>>8), byte(length),
		)
		b.buffer = append(b.buffer, value...)
	default:
		b.buffer = append(b.buffer,
			byte(op)|size2|isString,
			byte(length>>8), byte(length),
		)

		// Write the the data itself and the offset
		b.buffer = append(b.buffer, value...)
		b.writeOffset(uint32(delta))
	}
}

// PutString appends a string value.
func (b *Buffer) PutString(op OpType, idx uint32, value string) {
	b.PutBytes(op, idx, toBytes(value))
}

// PutBitmap iterates over the bitmap values and appends an operation for each bit set to one
func (b *Buffer) PutBitmap(op OpType, chunk Chunk, value bitmap.Bitmap) {
	chunk.Range(value, func(idx uint32) {
		b.PutOperation(op, idx)
	})
}

// writeUint64 appends a uint64 value.
func (b *Buffer) writeUint64(op OpType, idx uint32, value uint64) {
	delta := b.writeChunk(idx)
	switch delta {
	case 1:
		b.buffer = append(b.buffer,
			byte(op)|size8|isNext,
			byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
		)
	default:
		b.buffer = append(b.buffer,
			byte(op)|size8,
			byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
		)
		b.writeOffset(uint32(delta))
	}
}

// writeUint32 appends a uint32 value.
func (b *Buffer) writeUint32(op OpType, idx uint32, value uint32) {
	delta := b.writeChunk(idx)
	switch delta {
	case 1:
		b.buffer = append(b.buffer,
			byte(op)|size4|isNext,
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
		)
	default:
		b.buffer = append(b.buffer,
			byte(op)|size4,
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
		)
		b.writeOffset(uint32(delta))
	}
}

// writeUint16 appends a uint16 value.
func (b *Buffer) writeUint16(op OpType, idx uint32, value uint16) {
	delta := b.writeChunk(idx)
	switch delta {
	case 1:
		b.buffer = append(b.buffer, byte(op)|size2|isNext, byte(value>>8), byte(value))
	default:
		b.buffer = append(b.buffer, byte(op)|size2, byte(value>>8), byte(value))
		b.writeOffset(uint32(delta))
	}
}

// writeOffset writes the offset at the current head.
func (b *Buffer) writeOffset(delta uint32) {
	for delta >= 0x80 {
		b.buffer = append(b.buffer, byte(delta)|0x80)
		delta >>= 7
	}

	b.buffer = append(b.buffer, byte(delta))
}

// writeChunk writes a chunk if changed and returns the delta
func (b *Buffer) writeChunk(idx uint32) int32 {
	if chunk := Chunk(idx >> chunkShift); b.chunk != chunk {
		b.chunk = chunk
		b.chunks = append(b.chunks, header{
			Chunk: Chunk(chunk),
			Start: uint32(len(b.buffer)),
			Value: uint32(b.last),
		})
	}

	delta := int32(idx) - b.last
	b.last = int32(idx)
	return delta
}
