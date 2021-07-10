package commit

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkQueue/rw-baseline-8         	      97	  11718873 ns/op	 3998997 B/op	  999744 allocs/op
BenchmarkQueue/rw-u16-8              	     168	   6981673 ns/op	       3 B/op	       0 allocs/op
BenchmarkQueue/rw-u32-8              	     172	   6878112 ns/op	       3 B/op	       0 allocs/op
BenchmarkQueue/rw-u64-8              	     170	   6974754 ns/op	       3 B/op	       0 allocs/op
BenchmarkQueue/rw-str-8              	      85	  13869299 ns/op	       6 B/op	       0 allocs/op
*/
func BenchmarkQueue(b *testing.B) {
	const count = 1000000
	b.Run("rw-baseline", func(b *testing.B) {
		q := make([]Update, 0, count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q = q[:0]
			for i := uint32(0); i < count; i++ {
				q = append(q, Update{Type: Put, Index: i, Value: i})
			}

			for _, u := range q {
				_ = u.Value
			}
		}
	})

	run("rw-u16", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutUint16(Put, i, uint16(i))
		}
		for r.Seek(buf); r.Next(); {
			_ = r.AsUint16()
		}
	})

	run("rw-u32", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutUint32(Put, i, i)
		}
		for r.Seek(buf); r.Next(); {
			_ = r.AsUint32()
		}
	})

	run("rw-u64", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutUint64(Put, i, uint64(i))
		}
		for r.Seek(buf); r.Next(); {
			_ = r.AsUint64()
		}
	})

	run("rw-str", b, count, func(buf *Buffer, r *Reader) {
		for i := uint32(0); i < count; i++ {
			buf.PutString(Put, i, "hello world")
		}
		for r.Seek(buf); r.Next(); {
			_ = r.AsString()
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

	r := NewReader()
	r.Seek(buf)
	assert.True(t, r.Next())
	assert.Equal(t, int16(100), r.AsInt16())
	assert.True(t, r.Next())
	assert.Equal(t, int16(100), r.AsInt16())
	assert.True(t, r.Next())
	assert.Equal(t, int32(200), r.AsInt32())
	assert.True(t, r.Next())
	assert.Equal(t, int32(200), r.AsInt32())
	assert.True(t, r.Next())
	assert.Equal(t, int64(300), r.AsInt64())
	assert.True(t, r.Next())
	assert.Equal(t, int64(300), r.AsInt64())
	assert.True(t, r.Next())
	assert.Equal(t, uint16(400), r.AsUint16())
	assert.True(t, r.Next())
	assert.Equal(t, uint16(400), r.AsUint16())
	assert.True(t, r.Next())
	assert.Equal(t, uint32(500), r.AsUint32())
	assert.True(t, r.Next())
	assert.Equal(t, uint32(500), r.AsUint32())
	assert.True(t, r.Next())
	assert.Equal(t, uint64(600), r.AsUint64())
	assert.True(t, r.Next())
	assert.Equal(t, uint64(600), r.AsUint64())
	assert.True(t, r.Next())
	assert.Equal(t, float32(700), r.AsFloat32())
	assert.True(t, r.Next())
	assert.Equal(t, float32(700), r.AsFloat32())
	assert.True(t, r.Next())
	assert.Equal(t, float64(800), r.AsFloat64())
	assert.True(t, r.Next())
	assert.Equal(t, float64(800), r.AsFloat64())
	assert.True(t, r.Next())
	assert.Equal(t, "900", r.AsString())
	assert.True(t, r.Next())
	assert.Equal(t, "hello world", r.AsString())
	assert.True(t, r.Next())
	assert.Equal(t, "binary", string(r.AsBytes()))
	assert.True(t, r.Next())
	assert.Equal(t, true, r.AsBool())
	assert.True(t, r.Next())
	assert.Equal(t, false, r.AsBool())
	assert.False(t, r.Next())
}
