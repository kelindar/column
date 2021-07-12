package commit

import (
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

const (
	chunkShift = 14     // 16K elements
	size1      = 0      // 1 byte in size
	size2      = 1 << 4 // 2 bytes in size
	size4      = 2 << 4 // 4 bytes in size
	size8      = 3 << 4 // 8 bytes in size
	isNext     = 1 << 7 // is immediate next
	isString   = 1 << 6 // is variable-size string
)

// --------------------------- Operation Type ----------------------------

// OpType represents a type of an operation.
type OpType uint8

// Various update operations supported.
const (
	Put OpType = iota // Put stores a value regardless of a previous value
	Add               // Add increments the current stored value by the amount
)

// --------------------------- Delta log ----------------------------

// Buffer represents a buffer of delta operations.
type Buffer struct {
	last   int32    // The last offset writte
	chunk  uint32   // The current chunk
	buffer []byte   // The destination buffer
	chunks []header // The offsets of chunks
	_      [8]byte  // padding
	Column string   // The column for the queue
}

// header represents a chunk metadata header.
type header struct {
	Chunk uint32 // The chunk number
	Start uint32 // The offset at which the chunk starts in the buffer
	Value uint32 // The previous offset value for delta
}

// NewBuffer creates a new queue to store individual operations.
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		buffer: make([]byte, 0, capacity),
	}
}

// Reset resets the queue so it can be reused.
func (b *Buffer) Reset(column string) {
	b.chunks = b.chunks[:0]
	b.buffer = b.buffer[:0]
	b.last = 0
	b.chunk = math.MaxUint32
	b.Column = column
}

// IsEmpty returns whether the buffer is empty or not.
func (b *Buffer) IsEmpty() bool {
	return len(b.buffer) == 0
}

// Range iterates over the chunks present in the buffer
func (b *Buffer) RangeChunks(fn func(chunk uint32)) {
	for _, c := range b.chunks {
		fn(c.Chunk)
	}
}

// PutAny appends a supported value onto the buffer.
func (b *Buffer) PutAny(op OpType, idx uint32, value interface{}) {
	switch v := value.(type) {
	case uint64:
		b.PutUint64(op, idx, v)
	case uint32:
		b.PutUint32(op, idx, v)
	case uint16:
		b.PutUint16(op, idx, v)
	case uint8:
		b.PutUint16(op, idx, uint16(v))
	case int64:
		b.PutInt64(op, idx, v)
	case int32:
		b.PutInt32(op, idx, v)
	case int16:
		b.PutUint16(op, idx, uint16(v))
	case int8:
		b.PutUint16(op, idx, uint16(v))
	case string:
		b.PutString(op, idx, v)
	case []byte:
		b.PutBytes(op, idx, v)
	case float32:
		b.PutFloat32(op, idx, v)
	case float64:
		b.PutFloat64(op, idx, v)
	case int:
		b.PutInt64(op, idx, int64(v))
	case uint:
		b.PutUint64(op, idx, uint64(v))
	case bool:
		b.PutBool(op, idx, v)
	default:
		panic(fmt.Errorf("column: unsupported type (%T)", value))
	}
}

// PutUint64 appends a uint64 value.
func (b *Buffer) PutUint64(op OpType, idx uint32, value uint64) {
	b.writeChunk(idx)
	delta := int32(idx) - b.last
	b.last = int32(idx)
	if delta == 1 {
		b.buffer = append(b.buffer,
			byte(op)|size8|isNext,
			byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
		)
		return
	}

	b.buffer = append(b.buffer,
		byte(op)|size8,
		byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
		byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
	)
	b.writeOffset(uint32(delta))
}

// PutUint32 appends a uint32 value.
func (b *Buffer) PutUint32(op OpType, idx uint32, value uint32) {
	b.writeChunk(idx)
	delta := int32(idx) - b.last
	b.last = int32(idx)
	if delta == 1 {
		b.buffer = append(b.buffer,
			byte(op)|size4|isNext,
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
		)
		return
	}

	b.buffer = append(b.buffer,
		byte(op)|size4,
		byte(value>>24), byte(value>>16), byte(value>>8), byte(value),
	)
	b.writeOffset(uint32(delta))
}

// PutUint16 appends a uint16 value.
func (b *Buffer) PutUint16(op OpType, idx uint32, value uint16) {
	b.writeChunk(idx)
	delta := int32(idx) - b.last
	b.last = int32(idx)
	if delta == 1 {
		b.buffer = append(b.buffer, byte(op)|size2|isNext, byte(value>>8), byte(value))
		return
	}

	b.buffer = append(b.buffer, byte(op)|size2, byte(value>>8), byte(value))
	b.writeOffset(uint32(delta))
}

// PutBool appends a boolean value.
func (b *Buffer) PutBool(op OpType, idx uint32, value bool) {
	b.writeChunk(idx)
	delta := int32(idx) - b.last
	b.last = int32(idx)

	// let the compiler do its magic: https://github.com/golang/go/issues/6011
	v := 0
	if value {
		v = 1
	}

	if delta == 1 {
		b.buffer = append(b.buffer, byte(op)|size1|isNext, byte(v))
		return
	}

	b.buffer = append(b.buffer, byte(op)|size1, byte(v))
	b.writeOffset(uint32(delta))
}

// PutInt64 appends an int64 value.
func (b *Buffer) PutInt64(op OpType, idx uint32, value int64) {
	b.PutUint64(op, idx, uint64(value))
}

// PutInt32 appends an int32 value.
func (b *Buffer) PutInt32(op OpType, idx uint32, value int32) {
	b.PutUint32(op, idx, uint32(value))
}

// PutInt16 appends an int16 value.
func (b *Buffer) PutInt16(op OpType, idx uint32, value int16) {
	b.PutUint16(op, idx, uint16(value))
}

// PutFloat64 appends a float64 value.
func (b *Buffer) PutFloat64(op OpType, idx uint32, value float64) {
	b.PutUint64(op, idx, math.Float64bits(value))
}

// PutFloat32 appends an int32 value.
func (b *Buffer) PutFloat32(op OpType, idx uint32, value float32) {
	b.PutUint32(op, idx, math.Float32bits(value))
}

// PutBytes appends a binary value.
func (b *Buffer) PutBytes(op OpType, idx uint32, value []byte) {
	b.writeChunk(idx)
	delta := int32(idx) - b.last
	b.last = int32(idx)

	// Write a 2-byte length (max 65K slices)
	length := len(value)
	if delta == 1 {
		b.buffer = append(b.buffer,
			byte(op)|size2|isString|isNext,
			byte(length>>8), byte(length),
		)
		b.buffer = append(b.buffer, value...)
		return
	}

	b.buffer = append(b.buffer,
		byte(op)|size2|isString,
		byte(length>>8), byte(length),
	)

	// Write the the data itself and the offset
	b.buffer = append(b.buffer, value...)
	b.writeOffset(uint32(delta))
}

// PutString appends a string value.
func (b *Buffer) PutString(op OpType, idx uint32, value string) {
	b.PutBytes(op, idx, toBytes(value))
}

// writeOffset writes the offset at the current head.
func (b *Buffer) writeOffset(delta uint32) {
	for delta >= 0x80 {
		b.buffer = append(b.buffer, byte(delta)|0x80)
		delta >>= 7
	}

	b.buffer = append(b.buffer, byte(delta))
}

// writeChunk writes a chunk if changed
func (b *Buffer) writeChunk(idx uint32) {
	if chunk := idx >> chunkShift; b.chunk != chunk {
		b.chunk = chunk
		b.chunks = append(b.chunks, header{
			Chunk: chunk,
			Start: uint32(len(b.buffer)),
			Value: uint32(b.last),
		})
	}
}

// toBytes converts a string to a byte slice without allocating.
func toBytes(v string) (b []byte) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&v))
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = strHeader.Data

	l := len(v)
	byteHeader.Len = l
	byteHeader.Cap = l
	return
}
