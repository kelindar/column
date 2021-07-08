package commit

import (
	"encoding/binary"

	"github.com/kelindar/bitmap"
)

// Operation represnts an operation in the queue.
type Operation struct {
	Kind   UpdateType
	Offset int32
	Data   []byte
}

func (op Operation) Uint16() uint16 {
	return binary.BigEndian.Uint16(op.Data)
}

func (op Operation) Uint32() uint32 {
	return binary.BigEndian.Uint32(op.Data)
}

func (op Operation) Uint64() uint64 {
	return binary.BigEndian.Uint64(op.Data)
}

// Queue represents a queue of delta operations.
type Queue struct {
	fill   bitmap.Bitmap // The fill list
	tail   int64         // The tail (read) offset
	last   int32         // The last offset written
	buffer []byte        // The destination buffer
}

// NewQueue creates a new queue to store individual operations.
func NewQueue(capacity int) *Queue {
	return &Queue{
		buffer: make([]byte, 0, capacity),
	}
}

// Reset ressets the queue so it can be re-used.
func (q *Queue) Reset() {
	q.buffer = q.buffer[:0]
	q.tail = 0
	q.last = 0
}

// AppendUint64 appends a uint64 value.
func (q *Queue) AppendUint64(op UpdateType, idx uint32, value uint64) {
	delta := int32(idx) - q.last
	q.last = int32(idx)
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
	q.writeOffset(uint32(idx))
}

// AppendUint32 appends a uint32 value.
func (q *Queue) AppendUint32(op UpdateType, idx uint32, value uint32) {
	delta := int32(idx) - q.last
	q.last = int32(idx)
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
	q.writeOffset(uint32(idx))
}

// AppendUint16 appends a uint16 value.
func (q *Queue) AppendUint16(op UpdateType, idx uint32, value uint16) {
	delta := int32(idx) - q.last
	q.last = int32(idx)
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
	q.writeOffset(uint32(idx))
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

	// Special case, if the offset is one, then the current byte contains the
	// operation type and the value size. Otherwise, we do our varint thing.

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
	size := int64(2 << ((q.buffer[q.tail] & 0x60) >> 5))
	kind = UpdateType(q.buffer[q.tail] & 0x1f)
	data = q.buffer[q.tail+1 : q.tail+1+size]
	q.tail += size + 1
	return
}

// Next reads the current operation
func (q *Queue) Next(dst *Operation) bool {
	if head := int64(len(q.buffer)); q.tail >= head {
		return false // TODO: can just keep the number of elements somewhere to avoid this branch
	}

	// If the first bit is set to one, this means thatthe offset is one and we
	// can skip reading the actual offset. (special case)
	if q.buffer[q.tail] > 0x80 {
		size := int64(2 << ((q.buffer[q.tail] & 0x60) >> 5))
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
