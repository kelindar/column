package commit

import (
	"encoding/binary"
	"fmt"
)

// --------------------------- Reader ----------------------------

// Reader represnts a commit log reader (iterator).
type Reader struct {
	pos    int    // The read position
	buffer []byte // The log slice
	Offset int32
	Value  []byte
	Kind   UpdateType
}

func NewReader(buffer []byte) *Reader {
	return &Reader{
		pos:    0,
		buffer: buffer,
	}
}

// Reset resets the reader so it can be reused.
func (r *Reader) Reset() {
	r.buffer = r.buffer[:0]
	r.pos = 0
	r.Offset = 0
	r.Value = nil
	r.Kind = Put
}

// Uint16 reads a uint16 value.
func (r *Reader) Uint16() uint16 {
	return binary.BigEndian.Uint16(r.Value)
}

// Uint32 reads a uint32 value.
func (r *Reader) Uint32() uint32 {
	return binary.BigEndian.Uint32(r.Value)
}

// Uint64 reads a uint64 value.
func (r *Reader) Uint64() uint64 {
	return binary.BigEndian.Uint64(r.Value)
}

// Next reads the current operation and returns false if there is no more
// operations in the log.
func (r *Reader) Next() bool {
	if r.pos >= len(r.buffer) {
		return false // TODO: can just keep the number of elements somewhere to avoid this branch
	}

	// If the first bit is set, this means that the delta is one and we
	// can skip reading the actual offset. (special case)
	if r.buffer[r.pos] >= 0x80 {
		size := int(2 << ((r.buffer[r.pos] & 0x60) >> 5))
		r.Kind = UpdateType(r.buffer[r.pos] & 0x1f)
		r.Value = r.buffer[r.pos+1 : r.pos+1+size]
		r.Offset++
		r.pos += size + 1
		return true
	}

	r.readValue()
	r.readOffset()
	return true
}

// readOffset reads the signed variable-size integer at the current tail. While
// this is a signed integer, it is encoded as a variable-size unsigned integer.
// This would lead to negative values not being packed well, but given the
// rarity of negative values in the data, this is acceptable.
func (r *Reader) readOffset() {
	b := uint32(r.buffer[r.pos])
	if b < 0x80 {
		r.pos++
		r.Offset += int32(b)
		return
	}

	x := b & 0x7f
	b = uint32(r.buffer[r.pos+1])
	if b < 0x80 {
		r.pos += 2
		r.Offset += int32(x | (b << 7))
		return
	}

	x |= (b & 0x7f) << 7
	b = uint32(r.buffer[r.pos+2])
	if b < 0x80 {
		r.pos += 3
		r.Offset += int32(x | (b << 14))
		return
	}

	x |= (b & 0x7f) << 14
	b = uint32(r.buffer[r.pos+3])
	if b < 0x80 {
		r.pos += 4
		r.Offset += int32(x | (b << 21))
		return
	}

	x |= (b & 0x7f) << 21
	b = uint32(r.buffer[r.pos+4])
	if b < 0x80 {
		r.pos += 5
		r.Offset += int32(x | (b << 28))
		return
	}
}

// readValue reads the operation type and the value at the current position.
func (r *Reader) readValue() {
	size := int(2 << ((r.buffer[r.pos] & 0x60) >> 5))
	r.Kind = UpdateType(r.buffer[r.pos] & 0x1f)
	r.Value = r.buffer[r.pos+1 : r.pos+1+size]
	r.pos += size + 1
}

// --------------------------- Delta log ----------------------------

// Queue represents a queue of delta operations.
type Queue struct {
	last    int32   // The last offset writte
	buffer  []byte  // The destination buffer
	Offsets []int32 // The offsets of chunks
	//Current int16   // The current chunk

	//Column string // The column for the queue
}

// NewQueue creates a new queue to store individual operations.
func NewQueue(capacity int) *Queue {
	return &Queue{
		buffer: make([]byte, 0, capacity),
	}
}

// Reset resets the queue so it can be reused.
func (q *Queue) Reset() {
	//q.Column = ""
	//q.Current = -1
	q.Offsets = q.Offsets[:0]
	q.buffer = q.buffer[:0]
	//q.tail = 0
	q.last = 0
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
	q.writeOffset(uint32(delta))
}

// PutUint32 appends a uint32 value.
func (q *Queue) PutUint32(op UpdateType, idx uint32, value uint32) {
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
	q.writeOffset(uint32(delta))
}

// PutUint16 appends a uint16 value.
func (q *Queue) PutUint16(op UpdateType, idx uint32, value uint16) {
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
