package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkProperty/set-8         	14673010	        68.81 ns/op	     137 B/op	       0 allocs/op
// BenchmarkProperty/get-8         	153183829	         7.869 ns/op	       0 B/op	       0 allocs/op
// BenchmarkProperty/replace-8     	 5477583	       223.7 ns/op	      64 B/op	       4 allocs/op
/*func BenchmarkProperty(b *testing.B) {
	b.Run("set", func(b *testing.B) {
		p := NewProperty()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Set(5, "hello")
		}
	})

	b.Run("get", func(b *testing.B) {
		p := NewProperty()
		p.Set(5, "hello")
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Get(5)
		}
	})

	b.Run("replace", func(b *testing.B) {
		p := NewProperty()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Set(5, "hello")
			p.Remove(5)
		}
	})
}*/

func TestProperty(t *testing.T) {
	p := NewProperty()

	{ // Set the value at index
		p.Set(9, 99.5)
		assert.Equal(t, 1, len(p.data))
	}

	{ // Get the value
		v, ok := p.Get(9)
		assert.Equal(t, 99.5, v)
		assert.True(t, ok)
	}

	{ // Remove the value
		p.Remove(9)
		v, ok := p.Get(9)
		assert.Equal(t, nil, v)
		assert.False(t, ok)
	}

	{ // Set a couple of values, should only take 2 slots
		p.Set(5, "hi")
		p.Set(1000, "roman")
		assert.Equal(t, 2, len(p.data))

		v1, ok := p.Get(5)
		assert.True(t, ok)
		assert.Equal(t, "hi", v1)

		v2, ok := p.Get(1000)
		assert.True(t, ok)
		assert.Equal(t, "roman", v2)
	}

}

func TestPropertyOrder(t *testing.T) {

	// TODO: not sure if it's all correct, what happens if
	// we have 2 properties?

	p := NewProperty()
	for i := uint32(100); i < 200; i++ {
		p.Set(i, i)
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Get(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}

	for i := uint32(150); i < 180; i++ {
		p.Remove(i)
		p.Set(i, i)
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Get(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}
}
