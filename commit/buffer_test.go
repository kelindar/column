// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkQueue/rw-u16-8              	     168	   6981673 ns/op	       3 B/op	       0 allocs/op
BenchmarkQueue/rw-u32-8              	     172	   6878112 ns/op	       3 B/op	       0 allocs/op
BenchmarkQueue/rw-u64-8              	     170	   6974754 ns/op	       3 B/op	       0 allocs/op
BenchmarkQueue/rw-str-8              	      85	  13869299 ns/op	       6 B/op	       0 allocs/op
*/
func BenchmarkQueue(b *testing.B) {
	const count = 1000000
	run("rw-u16", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutUint16(Put, i, uint16(i))
		}
		for r.Seek(buf); r.Next(); {
			_ = r.Uint16()
		}
	})

	run("rw-u32", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutUint32(Put, i, i)
		}
		for r.Seek(buf); r.Next(); {
			_ = r.Uint32()
		}
	})

	run("rw-u64", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutUint64(Put, i, uint64(i))
		}
		for r.Seek(buf); r.Next(); {
			_ = r.Uint64()
		}
	})

	run("rw-str", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutString(Put, i, "hello world")
		}
		for r.Seek(buf); r.Next(); {
			_ = r.String()
		}
	})

}

// Run runs a single benchmark
func run(name string, b *testing.B, count int, fn func(buf *Buffer, r *Reader)) {
	b.Run(name, func(b *testing.B) {
		buf := NewBuffer(count * 20)
		r := NewReader()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			buf.Reset("test")
			fn(buf, r)
		}
	})
}

func TestSizeof(t *testing.T) {
	assert.LessOrEqual(t, int(unsafe.Sizeof(Reader{})), 80)
	assert.LessOrEqual(t, int(unsafe.Sizeof(Buffer{})), 80)
}

func TestReadWrite(t *testing.T) {
	buf := NewBuffer(0)
	buf.PutInt16(Put, 10, 100)
	buf.PutInt16(Put, 11, 100)
	buf.PutInt32(Put, 20, 200)
	buf.PutInt32(Put, 21, 200)
	buf.PutInt64(Put, 30, 300)
	buf.PutInt64(Put, 31, 300)
	buf.PutUint16(Put, 40, 400)
	buf.PutUint16(Put, 41, 400)
	buf.PutUint32(Put, 50, 500)
	buf.PutUint32(Put, 51, 500)
	buf.PutUint64(Put, 60, 600)
	buf.PutUint64(Put, 61, 600)
	buf.PutFloat32(Put, 70, 700)
	buf.PutFloat32(Put, 71, 700)
	buf.PutFloat64(Put, 80, 800)
	buf.PutFloat64(Put, 81, 800)
	buf.PutString(Put, 90, "900")
	buf.PutString(Put, 91, "hello world")
	buf.PutBytes(Put, 100, []byte("binary"))
	buf.PutBool(Put, 110, true)
	buf.PutBool(Put, 111, false)

	// Should only have 1 chunk
	buf.RangeChunks(func(chunk uint32) {
		assert.Equal(t, uint32(0), chunk)
	})

	r := NewReader()
	r.Seek(buf)
	assert.True(t, r.Next())
	assert.Equal(t, int16(100), r.Int16())
	assert.True(t, r.Next())
	assert.Equal(t, int16(100), r.Int16())
	assert.True(t, r.Next())
	assert.Equal(t, int32(200), r.Int32())
	assert.True(t, r.Next())
	assert.Equal(t, int32(200), r.Int32())
	assert.True(t, r.Next())
	assert.Equal(t, int64(300), r.Int64())
	assert.True(t, r.Next())
	assert.Equal(t, int64(300), r.Int64())
	assert.True(t, r.Next())
	assert.Equal(t, uint16(400), r.Uint16())
	assert.True(t, r.Next())
	assert.Equal(t, uint16(400), r.Uint16())
	assert.True(t, r.Next())
	assert.Equal(t, uint32(500), r.Uint32())
	assert.True(t, r.Next())
	assert.Equal(t, uint32(500), r.Uint32())
	assert.True(t, r.Next())
	assert.Equal(t, uint64(600), r.Uint64())
	assert.True(t, r.Next())
	assert.Equal(t, uint64(600), r.Uint64())
	assert.True(t, r.Next())
	assert.Equal(t, float32(700), r.Float32())
	assert.True(t, r.Next())
	assert.Equal(t, float32(700), r.Float32())
	assert.True(t, r.Next())
	assert.Equal(t, float64(800), r.Float64())
	assert.True(t, r.Next())
	assert.Equal(t, float64(800), r.Float64())
	assert.True(t, r.Next())
	assert.Equal(t, "900", r.String())
	assert.True(t, r.Next())
	assert.Equal(t, "hello world", r.String())
	assert.True(t, r.Next())
	assert.Equal(t, "binary", string(r.Bytes()))
	assert.True(t, r.Next())
	assert.Equal(t, true, r.Bool())
	assert.True(t, r.Next())
	assert.Equal(t, false, r.Bool())
	assert.False(t, r.Next())
}
