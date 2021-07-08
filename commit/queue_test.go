package commit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkQueue/rw-baseline-8         	     676	   1728737 ns/op	       0 B/op	       0 allocs/op
BenchmarkQueue/rw-u16-8              	     408	   2952627 ns/op	   25299 B/op	       0 allocs/op
BenchmarkQueue/rw-u32-8              	     165	   7163215 ns/op	  151427 B/op	       0 allocs/op
BenchmarkQueue/rw-u64-8              	     160	   7405664 ns/op	  261939 B/op	       0 allocs/op
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
				q = append(q, Update{Type: Put, Index: i, Value: 999})
			}

			for _, u := range q {
				_ = u.Value
			}
		}
	})

	b.Run("rw-u16", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset()
			for i := uint32(0); i < count; i++ {
				q.AppendUint16(Put, i, 999)
			}

			var op Operation
			for q.Next(&op) {
				_ = op.Uint16()
			}
		}
	})

	b.Run("rw-u32", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset()
			for i := uint32(0); i < count; i++ {
				q.AppendUint32(Put, i, 999)
			}

			var op Operation
			for q.Next(&op) {
				_ = op.Uint32()
			}
		}
	})

	b.Run("rw-u64", func(b *testing.B) {
		q := NewQueue(count)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			q.Reset()
			for i := uint32(0); i < count; i++ {
				q.AppendUint64(Put, i, 999)
			}

			var op Operation
			for q.Next(&op) {
				_ = op.Uint64()
			}
		}
	})
}

func TestQueue(t *testing.T) {
	q := NewQueue(1024)
	for i := uint32(0); i < 10; i++ {
		q.AppendUint64(Put, i, 2*uint64(i))
	}

	i := 0
	assert.Equal(t, 91, len(q.buffer)) // 10 x 10bytes
	var op Operation
	for q.Next(&op) {
		assert.Equal(t, Put, op.Kind)
		assert.Equal(t, i, int(op.Offset))
		assert.Equal(t, int(i*2), int(op.Uint64()))
		i++
	}
}
