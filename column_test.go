// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
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

func TestColumns(t *testing.T) {
	tests := []struct {
		column Column
		value  interface{}
	}{
		{column: ForEnum(), value: "mage"},
		{column: ForBool(), value: true},
		{column: ForAny(), value: "test"},
		{column: ForInt(), value: int(99)},
		{column: ForInt16(), value: int16(99)},
		{column: ForInt32(), value: int32(99)},
		{column: ForInt64(), value: int64(99)},
		{column: ForUint(), value: uint(99)},
		{column: ForUint16(), value: uint16(99)},
		{column: ForUint32(), value: uint32(99)},
		{column: ForUint64(), value: uint64(99)},
		{column: ForFloat32(), value: float32(99.5)},
		{column: ForFloat64(), value: float64(99.5)},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%T", tc.column), func(t *testing.T) {
			testColumn(t, tc.column, tc.value)
		})
	}
}

// Tests an individual column implementation
func testColumn(t *testing.T, column Column, value interface{}) {
	for i := 0; i < 2000; i += 50 {
		column.Grow(uint32(i))
	}

	column.Grow(9)
	column.Update([]commit.Update{{
		Type:  commit.Put,
		Index: 9,
		Value: value,
	}})

	// Assert the value
	v, ok := column.Value(9)
	assert.Equal(t, 1, column.Index().Count())
	assert.True(t, column.Contains(9))
	assert.Equal(t, value, v)
	assert.True(t, ok)

	// Delete the value and update again
	column.Delete(0, bitmap.Bitmap{0xffffffffffffffff})
	_, ok = column.Value(9)
	assert.False(t, ok)
	column.Update([]commit.Update{{
		Type:  commit.Put,
		Index: 9,
		Value: value,
	}})

	// Assert Numeric
	if column, ok := column.(Numeric); ok {

		// LoadFloat64
		f64, ok := column.LoadFloat64(9)
		assert.EqualValues(t, value, f64)
		assert.True(t, ok)

		// FilterFloat64
		index := bitmap.Bitmap{0xffff}
		column.FilterFloat64(&index, func(v float64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// LoadInt64
		i64, ok := column.LoadInt64(9)
		assert.EqualValues(t, value, i64)
		assert.True(t, ok)

		// FilterInt64
		index = bitmap.Bitmap{0xffff}
		column.FilterInt64(&index, func(v int64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// LoadUint64
		u64, ok := column.LoadUint64(9)
		assert.EqualValues(t, value, u64)
		assert.True(t, ok)

		// FilterUint64
		index = bitmap.Bitmap{0xffff}
		column.FilterUint64(&index, func(v uint64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// Atomic Add
		column.Update([]commit.Update{
			{Type: commit.Put, Index: 1, Value: value},
			{Type: commit.Put, Index: 2, Value: value},
			{Type: commit.Add, Index: 1, Value: value},
		})
		assert.True(t, column.Contains(1))
		assert.True(t, column.Contains(2))
		//v, _ := column.LoadInt64(1)
	}

	// Assert Textual
	if column, ok := column.(Textual); ok {

		// LoadString
		str, ok := column.LoadString(9)
		assert.EqualValues(t, value, str)
		assert.True(t, ok)

		// FilterFloat64
		index := bitmap.Bitmap{0xffff}
		column.FilterString(&index, func(v string) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())
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
		p.Delete(0, deletes)
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

func TestFromKind(t *testing.T) {
	for i := 0; i < 26; i++ {
		column := ForKind(reflect.Kind(i))
		_, ok := column.Value(100)
		assert.False(t, ok)
	}
}
