// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	buf := NewBuffer(0)
	buf.Reset("test")
	for i := uint32(0); i < 10; i++ {
		buf.PutUint64(Put, i, 2*uint64(i))
	}

	i := 0
	assert.Equal(t, 91, len(buf.buffer))

	r := NewReader()
	for r.Seek(buf); r.Next(); {
		assert.Equal(t, Put, r.Type)
		assert.Equal(t, i, int(r.Offset))
		assert.Equal(t, int(i*2), int(r.Uint64()))
		i++
	}
}

func TestRandom(t *testing.T) {
	seq := make([]uint32, 1024)
	for i := 0; i < len(seq); i++ {
		seq[i] = uint32(rand.Int31n(10000000))
	}

	buf := NewBuffer(0)
	for i := uint32(0); i < 1000; i++ {
		buf.PutUint32(Put, seq[i], uint32(rand.Int31()))
	}

	i := 0
	r := NewReader()
	for r.Seek(buf); r.Next(); {
		assert.Equal(t, Put, r.Type)
		assert.Equal(t, int(seq[i]), int(r.Offset))
		i++
	}
}

func TestRange(t *testing.T) {
	const count = 10000

	seq := make([]uint32, count)
	for i := 0; i < len(seq); i++ {
		seq[i] = uint32(rand.Int31n(1000000))
	}

	buf := NewBuffer(0)
	for i := uint32(0); i < count; i++ {
		buf.PutUint32(Put, seq[i], uint32(rand.Int31()))
	}

	r := NewReader()
	for i := 0; i < 100; i++ {
		r.Range(buf, uint32(i), func(r *Reader) {
			for r.Next() {
				assert.Equal(t, Put, r.Type)
				assert.Equal(t, i, int(r.Offset>>chunkShift))
			}
		})
	}
}

func TestReadSwap(t *testing.T) {
	buf := NewBuffer(0)
	buf.PutAny(Put, 10, int16(100))
	buf.PutAny(Put, 20, int32(200))
	buf.PutAny(Put, 30, int64(300))
	buf.PutAny(Put, 40, uint16(400))
	buf.PutAny(Put, 50, uint32(500))
	buf.PutAny(Put, 60, uint64(600))
	buf.PutAny(Put, 70, float32(700))
	buf.PutAny(Put, 80, float64(800))
	buf.PutAny(Put, 90, "900")
	buf.PutAny(Put, 100, []byte("binary"))
	buf.PutAny(Put, 110, true)
	buf.PutAny(Put, 120, int8(100))
	buf.PutAny(Put, 130, uint8(100))
	buf.PutAny(Put, 140, int(100))
	buf.PutAny(Put, 150, uint(100))
	buf.PutAny(Put, 160, float64(100))

	// Should only have 1 chunk
	assert.False(t, buf.IsEmpty())
	assert.Equal(t, 1, len(buf.chunks))
	buf.RangeChunks(func(chunk uint32) {
		assert.Equal(t, uint32(0), chunk)
	})

	r := NewReader()
	r.Seek(buf)
	assert.True(t, r.Next())
	assert.Equal(t, int16(100), r.Int16())
	assert.True(t, r.Next())
	assert.Equal(t, int32(200), r.Int32())
	assert.True(t, r.Next())
	assert.Equal(t, int64(300), r.Int64())
	assert.True(t, r.Next())
	assert.Equal(t, uint16(400), r.Uint16())
	assert.True(t, r.Next())
	assert.Equal(t, uint32(500), r.Uint32())
	assert.True(t, r.Next())
	assert.Equal(t, uint64(600), r.Uint64())
	assert.True(t, r.Next())
	assert.Equal(t, float32(700), r.Float32())
	assert.True(t, r.Next())
	assert.Equal(t, float64(800), r.Float64())
	assert.True(t, r.Next())
	assert.Equal(t, "900", r.String())
	assert.True(t, r.Next())
	assert.Equal(t, "binary", string(r.Bytes()))
	assert.True(t, r.Next())
	assert.Equal(t, true, r.Bool())
	assert.True(t, r.Next())
	assert.Equal(t, int16(100), r.Int16())
	assert.True(t, r.Next())
	assert.Equal(t, uint16(100), r.Uint16())
	assert.True(t, r.Next())
	assert.Equal(t, int(100), r.Int())
	assert.True(t, r.Next())
	assert.Equal(t, uint(100), r.Uint())

	// Rewind back and swap values
	r.Rewind()
	assert.True(t, r.Next())
	r.SwapInt16(99)
	assert.Equal(t, int16(99), r.Int16())
	assert.True(t, r.Next())
	r.SwapInt32(199)
	assert.Equal(t, int32(199), r.Int32())
	assert.True(t, r.Next())
	r.SwapInt64(299)
	assert.Equal(t, int64(299), r.Int64())
	assert.True(t, r.Next())
	r.SwapUint16(399)
	assert.Equal(t, uint16(399), r.Uint16())
	assert.True(t, r.Next())
	r.SwapUint32(499)
	assert.Equal(t, uint32(499), r.Uint32())
	assert.True(t, r.Next())
	r.SwapUint64(599)
	assert.Equal(t, uint64(599), r.Uint64())
	assert.True(t, r.Next())
	r.SwapFloat32(699)
	assert.Equal(t, float32(699), r.Float32())
	assert.True(t, r.Next())
	r.SwapFloat64(799)
	assert.Equal(t, float64(799), r.Float64())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	r.SwapBool(true)
	assert.Equal(t, true, r.Bool())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	r.SwapInt(300)
	assert.Equal(t, int(300), r.Int())
	assert.True(t, r.Next())
	r.SwapUint(400)
	assert.Equal(t, uint(400), r.Uint())
	assert.True(t, r.Next())
	r.SwapNumber(float64(800))
	assert.Equal(t, float64(800), r.Float64())
}

func TestWriteUnsupported(t *testing.T) {
	assert.Panics(t, func() {
		buf := NewBuffer(0)
		buf.PutAny(Put, 10, time.Time{})
	})
}

func TestReaderIface(t *testing.T) {
	buf := NewBuffer(0)
	buf.PutFloat64(Put, 777, float64(1))

	r := NewReader()
	r.Seek(buf)
	assert.True(t, r.Next())
	assert.Equal(t, float64(1), r.Float())
	assert.Equal(t, uint32(777), r.Index())
}

func TestReaderMax(t *testing.T) {
	buf := NewBuffer(0)
	buf.Reset("test")
	for i := uint32(0); i < 20000; i++ {
		buf.PutUint64(Put, i, 2*uint64(i))
	}

	r := NewReader()
	assert.Equal(t, 16383, int(r.MaxOffset(buf, 0)))
	assert.Equal(t, 19999, int(r.MaxOffset(buf, 1)))
	assert.Equal(t, 0, int(r.MaxOffset(nil, 0)))
}

func TestRewindAfterMax(t *testing.T) {
	buf := NewBuffer(0)
	buf.Reset("test")
	for i := uint32(0); i < 10; i++ {
		buf.PutUint64(Put, i, 2*uint64(i))
	}

	r := NewReader()
	assert.Equal(t, 9, int(r.MaxOffset(buf, 0)))
	assert.True(t, r.Next())
	assert.Equal(t, 0, int(r.Index()))
}

func TestReadSize(t *testing.T) {
	buf := NewBuffer(0)
	buf.Reset("test")
	buf.PutBool(123, true)

	r := NewReader()
	r.readFixed(buf.buffer[0])
	assert.Equal(t, 0, r.i1-r.i0)
}
