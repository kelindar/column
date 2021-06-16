// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"reflect"
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/stretchr/testify/assert"
)

// BenchmarkColumn/update-8         	51225134	        23.17 ns/op	       0 B/op	       0 allocs/op
// BenchmarkColumn/fetch-8          	100000000	        11.01 ns/op	       0 B/op	       0 allocs/op
// BenchmarkColumn/replace-8        	23745964	        45.09 ns/op	       0 B/op	       0 allocs/op
func BenchmarkColumn(b *testing.B) {
	b.Run("update", func(b *testing.B) {
		p := makeAny()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Update(5, "hello")
		}
	})

	b.Run("fetch", func(b *testing.B) {
		p := makeAny()
		p.Update(5, "hello")
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Value(5)
		}
	})

	b.Run("replace", func(b *testing.B) {
		p := makeAny()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Update(5, "hello")
			p.Delete(5)
		}
	})
}

func TestColumn(t *testing.T) {
	p := makeAny().(*columnAny)

	{ // Set the value at index
		p.Update(9, 99.5)
		assert.Equal(t, 10, len(p.data))
	}

	{ // Get the value
		v, ok := p.Value(9)
		assert.Equal(t, 99.5, v)
		assert.True(t, ok)
	}

	{ // Remove the value
		p.Delete(9)
		v, ok := p.Value(9)
		assert.Equal(t, nil, v)
		assert.False(t, ok)
	}

	{ // Set a couple of values, should only take 2 slots
		p.Update(5, "hi")
		p.Update(1000, "roman")
		assert.Equal(t, 1001, len(p.data))

		v1, ok := p.Value(5)
		assert.True(t, ok)
		assert.Equal(t, "hi", v1)

		v2, ok := p.Value(1000)
		assert.True(t, ok)
		assert.Equal(t, "roman", v2)
	}
}

func TestColumnOrder(t *testing.T) {
	p := makeAny()
	for i := uint32(100); i < 200; i++ {
		p.Update(i, i)
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Value(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}

	for i := uint32(150); i < 180; i++ {
		p.Delete(i)
		p.Update(i, i)
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Value(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}
}

func TestColumns(t *testing.T) {
	cols := []Column{
		makeBools(),
		makeAny(),
	}
	for _, c := range cols {

		{ // Set the value at index
			c.Update(9, true)
			assert.True(t, c.Contains(9))
		}

		{ // Get the values
			v, ok := c.Value(9)
			assert.Equal(t, true, v)
			assert.True(t, ok)
		}

		{
			other := bitmap.Bitmap{0xffffffffffffffff}
			c.Intersect(&other)
			assert.Equal(t, uint64(0b1000000000), other[0])
		}

		{
			other := bitmap.Bitmap{0xffffffffffffffff}
			c.Difference(&other)
			assert.Equal(t, uint64(0xfffffffffffffdff), other[0])
		}

		{
			other := bitmap.Bitmap{0xffffffffffffffff}
			c.Union(&other)
			assert.Equal(t, uint64(0xffffffffffffffff), other[0])
		}

		{ // Remove the value
			c.Delete(9)
			c.DeleteMany(&bitmap.Bitmap{0xffffffffffffffff})

			_, ok := c.Value(9)
			assert.False(t, ok)
		}

		{ // Update several items at once
			c.UpdateMany([]Update{{Index: 1, Value: true}, {Index: 2, Value: false}})
			assert.True(t, c.Contains(1))
			assert.True(t, c.Contains(2))
		}
	}
}

func TestFromKind(t *testing.T) {
	for i := 0; i < 26; i++ {
		column := ForKind(reflect.Kind(i))
		_, ok := column.Value(100)
		assert.False(t, ok)
	}
}
