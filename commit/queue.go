package commit

import (
	"encoding/binary"
	"fmt"
)

// --------------------------- Single Op ----------------------------

// Operation represnts an operation in the queue.
type Operation struct {
	Kind   UpdateType
	Offset int32
	Data   []byte
}

// Uint16 reads a uint16 value.
func (op Operation) Uint16() uint16 {
	return binary.BigEndian.Uint16(op.Data)
}

// Uint32 reads a uint32 value.
func (op Operation) Uint32() uint32 {
	return binary.BigEndian.Uint32(op.Data)
}

// Uint64 reads a uint64 value.
func (op Operation) Uint64() uint64 {
	return binary.BigEndian.Uint64(op.Data)
}

// --------------------------- Delta log ----------------------------

// Queue represents a queue of delta operations.
type Queue struct {
	tail   int    // The tail (read) offset
	last   int    // The last offset written
	buffer []byte // The destination buffer
	Chunk  int    // The chunk for the queue
	Column string // The column for the queue
}

// NewQueue creates a new queue to store individual operations.
func NewQueue(capacity int) *Queue {
	return &Queue{
		buffer: make([]byte, 0, capacity),
	}
}

// Reset ressets the queue so it can be re-used.
func (q *Queue) Reset() {
	q.Column = ""
	q.Chunk = 0
	q.buffer = q.buffer[:0]
	q.tail = 0
	q.last = 0
}

// Next reads the current operation and returns false if there is no more
// operations in the queue.
func (q *Queue) Next(dst *Operation) bool {
	if head := len(q.buffer); q.tail >= head {
		return false // TODO: can just keep the number of elements somewhere to avoid this branch
	}

	// If the first bit is set, this means that the delta is one and we
	// can skip reading the actual offset. (special case)
	if q.buffer[q.tail] >= 0x80 {
		size := int(2 << ((q.buffer[q.tail] & 0x60) >> 5))
		dst.Kind = UpdateType(q.buffer[q.tail] & 0x1f)
		dst.Data = q.buffer[q.tail+1 : q.tail+1+size]
		dst.Offset++
		q.tail += size + 1
		return true
	}

	dst.Kind, dst.Data = q.readValue()
	dst.Offset += q.readOffset()
	return true
}

// Put appends a value of any supported type onto the queue.
func (q *Queue) Put(op UpdateType, idx uint32, value interface{}) {
	switch v := value.(type) {
	case uint64:
		q.PutUint64(op, idx, v)
	case uint32:
		q.PutUint32(op, idx, v)
	case uint16:
		q.PutUint16(op, idx, v)
	default:
		panic(fmt.Errorf("column: unsupported type %T", value))
	}
}

// PutUint64 appends a uint64 value.
func (q *Queue) PutUint64(op UpdateType, idx uint32, value uint64) {
	delta := int(idx) - q.last
	q.last = int(idx)
	if delta == 1 {
		q.buffer = append(q.buffer,
			byte(op)+0x40+0x80,
			byte(value>>56),
			byte(value>>48),
			byte(value>>40),
			byte(value>>32),
			byte(value>>24),
			byte(value>>16),
			byte(value>>8),
			byte(value),
		)
		return
	}

	q.buffer = append(q.buffer,
		byte(op)+0x40,
		byte(value>>56),
		byte(value>>48),
		byte(value>>40),
		byte(value>>32),
		byte(value>>24),
		byte(value>>16),
		byte(value>>8),
		byte(value),
	)
	q.writeOffset(uint32(delta))
}

// PutUint32 appends a uint32 value.
func (q *Queue) PutUint32(op UpdateType, idx uint32, value uint32) {
	delta := int(idx) - q.last
	q.last = int(idx)
	if delta == 1 {
		q.buffer = append(q.buffer,
			byte(op)+0x20+0x80,
			byte(value>>24),
			byte(value>>16),
			byte(value>>8),
			byte(value),
		)
		return
	}

	q.buffer = append(q.buffer,
		byte(op)+0x20,
		byte(value>>24),
		byte(value>>16),
		byte(value>>8),
		byte(value),
	)
	q.writeOffset(uint32(delta))
}

// PutUint16 appends a uint16 value.
func (q *Queue) PutUint16(op UpdateType, idx uint32, value uint16) {
	delta := int(idx) - q.last
	q.last = int(idx)
	if delta == 1 {
		q.buffer = append(q.buffer,
			byte(op)+0x80,
			byte(value>>8),
			byte(value),
		)
		return
	}

	q.buffer = append(q.buffer,
		byte(op),
		byte(value>>8),
		byte(value),
	)
	q.writeOffset(uint32(delta))
}

// writeOffset writes the offset at the current head.
func (q *Queue) writeOffset(delta uint32) {
	for delta >= 0x80 {
		q.buffer = append(q.buffer, byte(delta)|0x80)
		delta >>= 7
	}

	q.buffer = append(q.buffer, byte(delta))
}

// readOffset reads the signed variable-size integer at the current tail. While
// this is a signed integer, it is encoded as a variable-size unsigned integer.
// This would lead to negative values not being packed well, but given the
// rarity of negative values in the data, this is acceptable.
func (q *Queue) readOffset() int32 {
	b := uint32(q.buffer[q.tail])
	if b < 0x80 {
		q.tail++
		return int32(b)
	}

	x := b & 0x7f
	b = uint32(q.buffer[q.tail+1])
	if b < 0x80 {
		q.tail += 2
		return int32(x | (b << 7))
	}

	x |= (b & 0x7f) << 7
	b = uint32(q.buffer[q.tail+2])
	if b < 0x80 {
		q.tail += 3
		return int32(x | (b << 14))
	}

	x |= (b & 0x7f) << 14
	b = uint32(q.buffer[q.tail+3])
	if b < 0x80 {
		q.tail += 4
		return int32(x | (b << 21))
	}

	x |= (b & 0x7f) << 21
	b = uint32(q.buffer[q.tail+4])
	if b < 0x80 {
		q.tail += 5
		return int32(x | (b << 28))
	}

	return 0
}

// readValue reads the operation type and the value at the current position.
func (q *Queue) readValue() (kind UpdateType, data []byte) {
	size := int(2 << ((q.buffer[q.tail] & 0x60) >> 5))
	kind = UpdateType(q.buffer[q.tail] & 0x1f)
	data = q.buffer[q.tail+1 : q.tail+1+size]
	q.tail += size + 1
	return
}
