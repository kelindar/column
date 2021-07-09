// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"encoding/binary"
	"math"
)

// Reader represnts a commit log reader (iterator).
type Reader struct {
	head   int        // The read position
	i0, i1 int        // The value start and end
	buffer []byte     // The log slice
	Offset int32      // The current offset
	Kind   UpdateType // The current operation type
}

// NewReader creates a new reader for a commit log.
func NewReader() *Reader {
	return &Reader{
		head: 0,
	}
}

// Seek resets the reader so it can be reused.
func (r *Reader) Seek(q *Queue) {
	r.buffer = q.buffer
	r.head = 0
	r.i0 = 0
	r.i1 = 0
	r.Offset = 0
	r.Kind = Put
}

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

// Next reads the current operation and returns false if there is no more
// operations in the log.
func (r *Reader) Next() bool {
	if r.head >= len(r.buffer) {
		return false
	}

	// If the first bit is set, this means that the delta is one and we
	// can skip reading the actual offset. (special case)
	head := r.buffer[r.head]
	if head >= 0x80 {
		r.readValue(head)
		r.Offset++
		return true
	}

	r.readValue(head)
	r.readOffset()
	return true
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

// readValue reads the operation type and the value at the current position.
func (r *Reader) readValue(v byte) {
	size := int(2 << ((v & 0x60) >> 5))
	r.Kind = UpdateType(v & 0x1f)
	r.head++
	r.i0 = r.head
	r.head += size
	r.i1 = r.head
}
