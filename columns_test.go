// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"reflect"
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

// BenchmarkColumn/fetch-8          	100000000	        11.01 ns/op	       0 B/op	       0 allocs/op
func BenchmarkColumn(b *testing.B) {
	b.Run("fetch", func(b *testing.B) {
		p := makeAny()
		p.Grow(10)
		p.Update([]commit.Update{{
			Type:  commit.Put,
			Index: 5,
			Value: "hello",
		}})
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			p.Value(5)
		}
	})
}

func TestColumn(t *testing.T) {
	p := makeAny().(*columnAny)
	p.Grow(1000)

	{ // Set the value at index
		p.Update([]commit.Update{{Type: commit.Put, Index: 9, Value: 99.5}})
		assert.Equal(t, 1001, len(p.data))
	}

	{ // Get the value
		v, ok := p.Value(9)
		assert.Equal(t, 99.5, v)
		assert.True(t, ok)
	}

	{ // Remove the value
		p.Delete(&bitmap.Bitmap{0b1000000000})
		v, ok := p.Value(9)
		assert.Equal(t, nil, v)
		assert.False(t, ok)
	}

	{ // Set a couple of values, should only take 2 slots
		p.Update([]commit.Update{
			{Type: commit.Put, Index: 5, Value: "hi"},
			{Type: commit.Put, Index: 1000, Value: "roman"},
		})

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
	p.Grow(199)
	for i := uint32(100); i < 200; i++ {
		p.Update([]commit.Update{{
			Type:  commit.Put,
			Index: i,
			Value: i,
		}})
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Value(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}

	for i := uint32(150); i < 180; i++ {
		var deletes bitmap.Bitmap
		deletes.Set(i)
		p.Delete(&deletes)
		p.Update([]commit.Update{{
			Type:  commit.Put,
			Index: i,
			Value: i,
		}})
	}

	for i := uint32(100); i < 200; i++ {
		x, ok := p.Value(i)
		assert.True(t, ok)
		assert.Equal(t, i, x)
	}
}

func TestColumns(t *testing.T) {
	cols := []Column{
		ForBool(),
		ForAny(),
	}
	for _, c := range cols {

		{ // Set the value at index
			c.Grow(9)
			c.Update([]commit.Update{{
				Type:  commit.Put,
				Index: 9,
				Value: true,
			}})
			assert.True(t, c.Contains(9))
		}

		{ // Get the values
			v, ok := c.Value(9)
			assert.Equal(t, true, v)
			assert.True(t, ok)
		}

		{ // Remove the value
			c.Delete(&bitmap.Bitmap{0xffffffffffffffff})

			_, ok := c.Value(9)
			assert.False(t, ok)
		}

		{ // Update several items at once
			c.Update([]commit.Update{{Index: 1, Value: true}, {Index: 2, Value: false}})
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
