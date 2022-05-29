// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"encoding/binary"
	"math"
	"unsafe"
)

// Reader represnts a commit log reader (iterator).
type Reader struct {
	head   int    // The read position
	i0, i1 int    // The value start and end
	Type   OpType // The current operation type
	buffer []byte // The log slice
	Offset int32  // The current offset
	start  int32  // The start offset
}

// NewReader creates a new reader for a commit log.
func NewReader() *Reader {
	return &Reader{}
}

// Seek resets the reader so it can be reused.
func (r *Reader) Seek(b *Buffer) {
	r.use(b.buffer)
}

// Rewind rewinds the reader back to zero.
func (r *Reader) Rewind() {
	r.use(r.buffer)
	r.Offset = r.start
}

// Use sets the buffer and resets the reader.
func (r *Reader) use(buffer []byte) {
	r.buffer = buffer
	r.head = 0
	r.i0 = 0
	r.i1 = 0
	r.Offset = 0
	r.Type = Put
}

// --------------------------- Value Read ----------------------------

// Int16 reads a uint16 value.
func (r *Reader) Int16() int16 {
	return int16(binary.BigEndian.Uint16(r.buffer[r.i0:r.i1]))
}

// Int32 reads a uint32 value.
func (r *Reader) Int32() int32 {
	return int32(binary.BigEndian.Uint32(r.buffer[r.i0:r.i1]))
}

// Int64 reads a uint64 value.
func (r *Reader) Int64() int64 {
	return int64(binary.BigEndian.Uint64(r.buffer[r.i0:r.i1]))
}

// Uint16 reads a uint16 value.
func (r *Reader) Uint16() uint16 {
	return binary.BigEndian.Uint16(r.buffer[r.i0:r.i1])
}

// Uint32 reads a uint32 value.
func (r *Reader) Uint32() uint32 {
	return binary.BigEndian.Uint32(r.buffer[r.i0:r.i1])
}

// Uint64 reads a uint64 value.
func (r *Reader) Uint64() uint64 {
	return binary.BigEndian.Uint64(r.buffer[r.i0:r.i1])
}

// Float32 reads a float32 value.
func (r *Reader) Float32() float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(r.buffer[r.i0:r.i1]))
}

// Float64 reads a float64 value.
func (r *Reader) Float64() float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(r.buffer[r.i0:r.i1]))
}

// Number reads a float64 value. This is used for codegen, equivalent to Float64().
func (r *Reader) Number() float64 {
	return r.Float64()
}

// Bytes reads a binary value.
func (r *Reader) Bytes() []byte {
	return r.buffer[r.i0:r.i1]
}

// --------------------------- Reader Interface ----------------------------

// Index returns the current index of the reader.
func (r *Reader) Index() uint32 {
	return uint32(r.Offset)
}

// IndexAtChunk returns the current index assuming chunk starts at 0.
func (r *Reader) IndexAtChunk() uint32 {
	return uint32(r.Offset) - ((uint32(r.Offset) >> chunkShift) << chunkShift)
}

// Int reads a int value of any size.
func (r *Reader) Int() int {
	return int(r.Uint())
}

// Uint reads a uint value of any size.
func (r *Reader) Uint() uint {
	switch r.i1 - r.i0 {
	case 2:
		return uint(binary.BigEndian.Uint16(r.buffer[r.i0:r.i1]))
	case 4:
		return uint(binary.BigEndian.Uint32(r.buffer[r.i0:r.i1]))
	case 8:
		return uint(binary.BigEndian.Uint64(r.buffer[r.i0:r.i1]))
	default:
		panic("column: unable to read, unsupported integer size")
	}
}

// Float reads a floating-point value of any size.
func (r *Reader) Float() float64 {
	switch r.i1 - r.i0 {
	case 4:
		return float64(r.Float32())
	case 8:
		return r.Float64()
	default:
		panic("column: unable to read, unsupported float size")
	}
}

// String reads a string value.
func (r *Reader) String() string {
	b := r.buffer[r.i0:r.i1]
	return *(*string)(unsafe.Pointer(&b))
}

// Bool reads a boolean value.
func (r *Reader) Bool() bool {
	return r.Type == PutTrue
}

// --------------------------- Value Swap ----------------------------

// SwapInt16 swaps a uint16 value with a new one.
func (r *Reader) SwapInt16(v int16) {
	binary.BigEndian.PutUint16(r.buffer[r.i0:r.i1], uint16(v))
}

// SwapInt32 swaps a uint32 value with a new one.
func (r *Reader) SwapInt32(v int32) {
	binary.BigEndian.PutUint32(r.buffer[r.i0:r.i1], uint32(v))
}

// SwapInt64 swaps a uint64 value with a new one.
func (r *Reader) SwapInt64(v int64) {
	binary.BigEndian.PutUint64(r.buffer[r.i0:r.i1], uint64(v))
}

// SwapInt swaps a uint64 value with a new one.
func (r *Reader) SwapInt(v int) {
	binary.BigEndian.PutUint64(r.buffer[r.i0:r.i1], uint64(v))
}

// SwapUint16 swaps a uint16 value with a new one.
func (r *Reader) SwapUint16(v uint16) {
	binary.BigEndian.PutUint16(r.buffer[r.i0:r.i1], v)
}

// SwapUint32 swaps a uint32 value with a new one.
func (r *Reader) SwapUint32(v uint32) {
	binary.BigEndian.PutUint32(r.buffer[r.i0:r.i1], v)
}

// SwapUint64 swaps a uint64 value with a new one.
func (r *Reader) SwapUint64(v uint64) {
	binary.BigEndian.PutUint64(r.buffer[r.i0:r.i1], v)
}

// SwapUint swaps a uint64 value with a new one.
func (r *Reader) SwapUint(v uint) {
	binary.BigEndian.PutUint64(r.buffer[r.i0:r.i1], uint64(v))
}

// SwapFloat32 swaps a float32 value with a new one.
func (r *Reader) SwapFloat32(v float32) {
	binary.BigEndian.PutUint32(r.buffer[r.i0:r.i1], math.Float32bits(v))
}

// SwapFloat64 swaps a float64 value with a new one.
func (r *Reader) SwapFloat64(v float64) {
	binary.BigEndian.PutUint64(r.buffer[r.i0:r.i1], math.Float64bits(v))
}

// SwapBool swaps a boolean value with a new one.
func (r *Reader) SwapBool(b bool) {
	r.buffer[r.i0] = 0
	if b {
		r.buffer[r.i0] = 1
	}
}

// --------------------------- Value Increment ----------------------------

// AddToInt adds and swaps a int value with a new one.
func (r *Reader) AddToInt(value int) int {
	value += r.Int()
	r.SwapInt(value)
	return value
}

// AddToInt16 adds and swaps a int16 value with a new one.
func (r *Reader) AddToInt16(value int16) int16 {
	value += r.Int16()
	r.SwapInt16(value)
	return value
}

// AddToInt32 adds and swaps a int32 value with a new one.
func (r *Reader) AddToInt32(value int32) int32 {
	value += r.Int32()
	r.SwapInt32(value)
	return value
}

// AddToInt64 adds and swaps a int64 value with a new one.
func (r *Reader) AddToInt64(value int64) int64 {
	value += r.Int64()
	r.SwapInt64(value)
	return value
}

// AddToUint adds and swaps a uint value with a new one.
func (r *Reader) AddToUint(value uint) uint {
	value += r.Uint()
	r.SwapUint(value)
	return value
}

// AddToUint16 adds and swaps a uint16 value with a new one.
func (r *Reader) AddToUint16(value uint16) uint16 {
	value += r.Uint16()
	r.SwapUint16(value)
	return value
}

// AddToUint32 adds and swaps a uint32 value with a new one.
func (r *Reader) AddToUint32(value uint32) uint32 {
	value += r.Uint32()
	r.SwapUint32(value)
	return value
}

// AddToUint64 adds and swaps a uint64 value with a new one.
func (r *Reader) AddToUint64(value uint64) uint64 {
	value += r.Uint64()
	r.SwapUint64(value)
	return value
}

// AddToFloat32 adds and swaps a float32 value with a new one.
func (r *Reader) AddToFloat32(value float32) float32 {
	value += r.Float32()
	r.SwapFloat32(value)
	return value
}

// AddToFloat64 adds and swaps a float64 value with a new one.
func (r *Reader) AddToFloat64(value float64) float64 {
	value += r.Float64()
	r.SwapFloat64(value)
	return value
}

// --------------------------- Chunk Iterator ----------------------------

// Range iterates over parts of the buffer which match the specified chunk.
func (r *Reader) Range(buf *Buffer, chunk Chunk, fn func(*Reader)) {
	for i, c := range buf.chunks {
		if c.Chunk != chunk {
			continue // Not the right chunk, skip it
		}

		// Find the next offset
		offset := c.Start
		buffer := buf.buffer[offset:]
		if len(buf.chunks) > i+1 {
			until := uint32(buf.chunks[i+1].Start)
			buffer = buf.buffer[offset:until]
		}

		// Set the reader to the subset buffer and call the delegate
		r.use(buffer)
		r.Offset = int32(c.Value)
		r.start = int32(c.Value)
		fn(r)
	}
}

// --------------------------- Next Iterator ----------------------------

// Next reads the current operation and returns false if there is no more
// operations in the log.
func (r *Reader) Next() bool {
	if r.head >= len(r.buffer) {
		return false
	}

	head := r.buffer[r.head]
	switch head & 0xc0 {

	// If this is a variable-size value but not a next neighbour, read the
	// string and its offset.
	case isString:
		r.readString(head)
		r.readOffset()
		return true

	// If this is both a variable-size value and a next neighbour, read the
	// string and skip the offset.
	case isNext | isString:
		r.readString(head)
		r.Offset++
		return true

	// If the first bit is set, this means that the delta is one and we
	// can skip reading the actual offset. (special case)
	case isNext:
		r.readFixed(head)
		r.Offset++
		return true

	// If it's not a string nor it is an immediate neighbor, we need to read
	// the full offset.
	default:
		r.readFixed(head)
		r.readOffset()
		return true
	}
}

// readOffset reads the signed variable-size integer at the current tail. While
// this is a signed integer, it is encoded as a variable-size unsigned integer.
// This would lead to negative values not being packed well, but given the
// rarity of negative values in the data, this is acceptable.
func (r *Reader) readOffset() {
	b := uint32(r.buffer[r.head])
	if b < 0x80 {
		r.head++
		r.Offset += int32(b)
		return
	}

	x := b & 0x7f
	b = uint32(r.buffer[r.head+1])
	if b < 0x80 {
		r.head += 2
		r.Offset += int32(x | (b << 7))
		return
	}

	x |= (b & 0x7f) << 7
	b = uint32(r.buffer[r.head+2])
	if b < 0x80 {
		r.head += 3
		r.Offset += int32(x | (b << 14))
		return
	}

	x |= (b & 0x7f) << 14
	b = uint32(r.buffer[r.head+3])
	if b < 0x80 {
		r.head += 4
		r.Offset += int32(x | (b << 21))
		return
	}

	x |= (b & 0x7f) << 21
	b = uint32(r.buffer[r.head+4])
	if b < 0x80 {
		r.head += 5
		r.Offset += int32(x | (b << 28))
		return
	}
}

// readFixed reads the fixed-size value at the current position.
func (r *Reader) readFixed(v byte) {
	size := int(1 << (v >> 4 & 0b11) & 0b1110)
	r.head++
	r.i0 = r.head
	r.head += size
	r.i1 = r.head
	r.Type = OpType(v & 0xf)
}

// readString reads the operation type and the value at the current position.
func (r *Reader) readString(v byte) {
	size := int(r.buffer[r.head+2]) | int(r.buffer[r.head+1])<<8
	r.head += 3
	r.i0 = r.head
	r.head += size
	r.i1 = r.head
	r.Type = OpType(v & 0xf)
}
