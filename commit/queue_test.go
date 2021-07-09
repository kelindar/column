package commit

import (
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkQueue/rw-baseline-8         	     638	   1892620 ns/op	       2 B/op	       0 allocs/op
BenchmarkQueue/rw-u16-8              	     405	   2939654 ns/op	   25486 B/op	       0 allocs/op
BenchmarkQueue/rw-u32-8              	     164	   7215495 ns/op	  152351 B/op	       0 allocs/op
BenchmarkQueue/rw-u64-8              	     160	   7328651 ns/op	  261940 B/op	       0 allocs/op
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

	b.Run("rw-any", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset("test")
			for i := uint32(0); i < count; i++ {
				q.Put(Put, i, i)
			}

			r := NewReader(q.buffer)
			for r.Next() {
				_ = r.Uint32()
			}
		}
	})

	b.Run("rw-u16", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset("test")
			for i := uint32(0); i < count; i++ {
				q.PutUint16(Put, i, uint16(i))
			}

			r := NewReader(q.buffer)
			for r.Next() {
				_ = r.Uint16()
			}
		}
	})

	b.Run("rw-u32", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset("test")
			for i := uint32(0); i < count; i++ {
				q.PutUint32(Put, i, i)
			}

			r := NewReader(q.buffer)
			for r.Next() {
				_ = r.Uint32()
			}
		}
	})

	b.Run("rw-u64", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset("test")
			for i := uint32(0); i < count; i++ {
				q.PutUint64(Put, i, uint64(i))
			}

			r := NewReader(q.buffer)
			for r.Next() {
				_ = r.Uint64()
			}
		}
	})
}

func TestQueue(t *testing.T) {
	q := NewQueue(1024)
	for i := uint32(0); i < 10; i++ {
		q.PutUint64(Put, i, 2*uint64(i))
	}

	i := 0
	assert.Equal(t, 91, len(q.buffer)) // 10 x 10bytes

	r := NewReader(q.buffer)
	for r.Next() {
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

	q := NewQueue(1024)
	for i := uint32(0); i < 1000; i++ {
		q.PutUint32(Put, seq[i], uint32(rand.Int31()))
	}

	i := 0
	r := NewReader(q.buffer)
	for r.Next() {
		assert.Equal(t, Put, r.Kind)
		assert.Equal(t, int(seq[i]), int(r.Offset))
		i++
	}
}

func TestQueueSize(t *testing.T) {
	assert.LessOrEqual(t, int(unsafe.Sizeof(Reader{})), 64)
	assert.LessOrEqual(t, int(unsafe.Sizeof(Queue{})), 80)
}
