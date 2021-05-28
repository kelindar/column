// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkProperty/set-8         	344183034	         3.498 ns/op	       0 B/op	       0 allocs/op
// BenchmarkProperty/get-8         	1000000000	         1.123 ns/op	       0 B/op	       0 allocs/op
// BenchmarkProperty/replace-8     	291245523	         4.157 ns/op	       0 B/op	       0 allocs/op
func BenchmarkProperty(b *testing.B) {
	b.Run("set", func(b *testing.B) {
		p := newProperty()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Set(5, "hello")
		}
	})

	b.Run("get", func(b *testing.B) {
		p := newProperty()
		p.Set(5, "hello")
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Get(5)
		}
	})

	b.Run("replace", func(b *testing.B) {
		p := newProperty()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Set(5, "hello")
			p.Del(5)
		}
	})
}

func TestProperty(t *testing.T) {
	p := newProperty()

	{ // Set the value at index
		p.Set(9, 99.5)
		assert.Equal(t, 10, len(p.data))
	}

	{ // Get the value
		v, ok := p.Get(9)
		assert.Equal(t, 99.5, v)
		assert.True(t, ok)
	}

	{ // Remove the value
		p.Del(9)
		v, ok := p.Get(9)
		assert.Equal(t, nil, v)
		assert.False(t, ok)
	}

	{ // Set a couple of values, should only take 2 slots
		p.Set(5, "hi")
		p.Set(1000, "roman")
		assert.Equal(t, 1001, len(p.data))

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

	p := newProperty()
	for i := uint32(100); i < 200; i++ {
		p.Set(i, i)
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Get(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}

	for i := uint32(150); i < 180; i++ {
		p.Del(i)
		p.Set(i, i)
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Get(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}
}
