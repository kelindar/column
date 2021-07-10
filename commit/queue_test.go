package commit

import (
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkQueue/rw-baseline-8         	     100	  11909863 ns/op	 3998997 B/op	  999744 allocs/op
BenchmarkQueue/rw-any-8              	      56	  18175307 ns/op	 4445179 B/op	  999744 allocs/op
BenchmarkQueue/rw-u16-8              	     181	   6549557 ns/op	       2 B/op	       0 allocs/op
BenchmarkQueue/rw-u32-8              	     184	   6452655 ns/op	       2 B/op	       0 allocs/op
BenchmarkQueue/rw-u64-8              	     181	   6627228 ns/op	       2 B/op	       0 allocs/op
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

	run("rw-any", b, count, func(q *Queue, r *Reader) {
		for i := uint32(0); i < count; i++ {
			q.Put(Put, i, i)
		}
		for r.Seek(q); r.Next(); {
			_ = r.Uint32()
		}
	})

	run("rw-u16", b, count, func(q *Queue, r *Reader) {
		for i := uint32(0); i < count; i++ {
			q.PutUint16(Put, i, uint16(i))
		}
		for r.Seek(q); r.Next(); {
			_ = r.Uint16()
		}
	})

	run("rw-u32", b, count, func(q *Queue, r *Reader) {
		for i := uint32(0); i < count; i++ {
			q.PutUint32(Put, i, i)
		}
		for r.Seek(q); r.Next(); {
			_ = r.Uint32()
		}
	})

	run("rw-u64", b, count, func(q *Queue, r *Reader) {
		for i := uint32(0); i < count; i++ {
			q.PutUint64(Put, i, uint64(i))
		}
		for r.Seek(q); r.Next(); {
			_ = r.Uint64()
		}
	})

	run("rw-str", b, count, func(q *Queue, r *Reader) {
		for i := uint32(0); i < count; i++ {
			q.PutString(Put, i, "hello world")
		}
		for r.Seek(q); r.Next(); {
			_ = r.String()
		}
	})

}

// Run runs a single benchmark
func run(name string, b *testing.B, count int, fn func(q *Queue, r *Reader)) {
	b.Run(name, func(b *testing.B) {
		q := NewQueue(count * 20)
		r := NewReader()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset("test")
			fn(q, r)
		}
	})
}

func TestQueue(t *testing.T) {
	q := NewQueue(0)
	for i := uint32(0); i < 10; i++ {
		q.PutUint64(Put, i, 2*uint64(i))
	}

	i := 0
	assert.Equal(t, 91, len(q.buffer))

	r := NewReader()
	for r.Seek(q); r.Next(); {
		assert.Equal(t, Put, r.Kind)
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

	q := NewQueue(0)
	for i := uint32(0); i < 1000; i++ {
		q.PutUint32(Put, seq[i], uint32(rand.Int31()))
	}

	i := 0
	r := NewReader()
	for r.Seek(q); r.Next(); {
		assert.Equal(t, Put, r.Kind)
		assert.Equal(t, int(seq[i]), int(r.Offset))
		i++
	}
}

func TestQueueSize(t *testing.T) {
	assert.LessOrEqual(t, int(unsafe.Sizeof(Reader{})), 64)
	assert.LessOrEqual(t, int(unsafe.Sizeof(Queue{})), 80)
}

func TestReadWrite(t *testing.T) {
	q := NewQueue(0)
	q.PutInt16(Put, 10, 100)
	q.PutInt16(Put, 11, 100)
	q.PutInt32(Put, 20, 200)
	q.PutInt32(Put, 21, 200)
	q.PutInt64(Put, 30, 300)
	q.PutInt64(Put, 31, 300)
	q.PutUint16(Put, 40, 400)
	q.PutUint16(Put, 41, 400)
	q.PutUint32(Put, 50, 500)
	q.PutUint32(Put, 51, 500)
	q.PutUint64(Put, 60, 600)
	q.PutUint64(Put, 61, 600)
	q.PutFloat32(Put, 70, 700)
	q.PutFloat32(Put, 71, 700)
	q.PutFloat64(Put, 80, 800)
	q.PutFloat64(Put, 81, 800)
	q.PutString(Put, 90, "900")
	q.PutString(Put, 91, "hello world")
	q.PutBytes(Put, 100, []byte("binary"))
	q.PutBool(Put, 110, true)
	q.PutBool(Put, 111, false)

	r := NewReader()
	r.Seek(q)
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
