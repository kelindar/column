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

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkColumn/chunkOf-8         	 8466814	       136.2 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkColumn(b *testing.B) {
	b.Run("chunkOf", func(b *testing.B) {
		var temp bitmap.Bitmap
		temp.Grow(2 * chunkSize)

		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for i := 0; i < 100; i++ {
				chunkOf(temp, 1)
			}
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
		{column: ForString(), value: "test"},
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

		t.Run(fmt.Sprintf("%T-cursor", tc.column), func(t *testing.T) {
			testColumnCursor(t, tc.column, tc.value)
		})

		t.Run(fmt.Sprintf("%T-put-delete", tc.column), func(t *testing.T) {
			testPutDelete(t, tc.column, tc.value)
		})

		t.Run(fmt.Sprintf("%T-snapshot", tc.column), func(t *testing.T) {
			testSnapshot(t, tc.column, tc.value)
		})
	}
}

// Tests an individual column implementation
func testColumn(t *testing.T, column Column, value interface{}) {
	for i := 0; i < 2000; i += 50 {
		column.Grow(uint32(i))
	}

	// Add a value
	applyChanges(column, Update{commit.Put, 9, value})

	// Assert the value
	v, ok := column.Value(9)
	assert.Equal(t, 1, column.Index().Count())
	assert.True(t, column.Contains(9))
	assert.Equal(t, value, v)
	assert.True(t, ok)

	// Apply updates
	applyChanges(column, Update{commit.Put, 9, value})

	// Assert Numeric
	if column, ok := column.(Numeric); ok {

		// LoadFloat64
		f64, ok := column.LoadFloat64(9)
		assert.EqualValues(t, value, f64)
		assert.True(t, ok)

		// FilterFloat64
		index := bitmap.Bitmap{0xffff}
		column.FilterFloat64(0, index, func(v float64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// LoadInt64
		i64, ok := column.LoadInt64(9)
		assert.EqualValues(t, value, i64)
		assert.True(t, ok)

		// FilterInt64
		index = bitmap.Bitmap{0xffff}
		column.FilterInt64(0, index, func(v int64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// LoadUint64
		u64, ok := column.LoadUint64(9)
		assert.EqualValues(t, value, u64)
		assert.True(t, ok)

		// FilterUint64
		index = bitmap.Bitmap{0xffff}
		column.FilterUint64(0, index, func(v uint64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// Atomic Add
		applyChanges(column,
			Update{Type: commit.Put, Index: 1, Value: value},
			Update{Type: commit.Put, Index: 2, Value: value},
			Update{Type: commit.Add, Index: 1, Value: value},
		)

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
		column.FilterString(0, index, func(v string) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())
	}

}

// Tests an individual column cursor
func testColumnCursor(t *testing.T, column Column, value interface{}) {
	col := NewCollection()
	col.CreateColumn("test", column)
	col.InsertObject(map[string]interface{}{
		"test": value,
	})

	assert.NotPanics(t, func() {
		col.Query(func(txn *Txn) error {
			return txn.Range("test", func(cur Cursor) {
				setAny(&cur, "test", value)
				if _, ok := column.(Numeric); ok {
					addAny(&cur, "test", value)
				}
			})
		})
	})
}

// testPutDelete test a put and a delete
func testPutDelete(t *testing.T, column Column, value interface{}) {
	applyChanges(column,
		Update{commit.Put, 0, value},
		Update{commit.Delete, 0, nil},
	)

	// Should be deleted
	_, ok := column.Value(0)
	assert.False(t, ok)
}

// testSnapshot test a snapshot of a column
func testSnapshot(t *testing.T, column Column, value interface{}) {
	buf := commit.NewBuffer(8)
	column.Snapshot(0, buf)
	assert.False(t, buf.IsEmpty())
}

func TestFromKind(t *testing.T) {
	for _, v := range []reflect.Kind{
		reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Bool, reflect.String,
		reflect.Float32, reflect.Float64,
	} {
		column := ForKind(v)
		_, ok := column.Value(100)
		assert.False(t, ok)
	}
}

func applyChanges(column Column, updates ...Update) {
	buf := commit.NewBuffer(10)
	for _, u := range updates {
		buf.PutAny(u.Type, u.Index, u.Value)
	}

	r := new(commit.Reader)
	r.Seek(buf)
	column.Apply(r)
}

type Update struct {
	Type  commit.OpType
	Index uint32
	Value interface{}
}

// setAny used for testing
func setAny(cur *Cursor, column string, value interface{}) {
	switch v := value.(type) {
	case uint:
		cur.SetUint(v)
		cur.SetUintAt(column, v)
	case uint64:
		cur.SetUint64(v)
		cur.SetUint64At(column, v)
	case uint32:
		cur.SetUint32(v)
		cur.SetUint32At(column, v)
	case uint16:
		cur.SetUint16(v)
		cur.SetUint16At(column, v)
	case int:
		cur.SetInt(v)
		cur.SetIntAt(column, v)
	case int64:
		cur.SetInt64(v)
		cur.SetInt64At(column, v)
	case int32:
		cur.SetInt32(v)
		cur.SetInt32At(column, v)
	case int16:
		cur.SetInt16(v)
		cur.SetInt16At(column, v)
	case float64:
		cur.SetFloat64(v)
		cur.SetFloat64At(column, v)
	case float32:
		cur.SetFloat32(v)
		cur.SetFloat32At(column, v)
	case bool:
		cur.SetBool(v)
		cur.SetBoolAt(column, v)
	case string:
		cur.SetString(v)
		cur.SetStringAt(column, v)
	default:
		panic(fmt.Errorf("column: unsupported type (%T)", value))
	}
}

// addAny used for testing
func addAny(cur *Cursor, column string, value interface{}) {
	switch v := value.(type) {
	case uint:
		cur.AddUint(v)
		cur.AddUintAt(column, v)
	case uint64:
		cur.AddUint64(v)
		cur.AddUint64At(column, v)
	case uint32:
		cur.AddUint32(v)
		cur.AddUint32At(column, v)
	case uint16:
		cur.AddUint16(v)
		cur.AddUint16At(column, v)
	case int:
		cur.AddInt(v)
		cur.AddIntAt(column, v)
	case int64:
		cur.AddInt64(v)
		cur.AddInt64At(column, v)
	case int32:
		cur.AddInt32(v)
		cur.AddInt32At(column, v)
	case int16:
		cur.AddInt16(v)
		cur.AddInt16At(column, v)
	case float64:
		cur.AddFloat64(v)
		cur.AddFloat64At(column, v)
	case float32:
		cur.AddFloat32(v)
		cur.AddFloat32At(column, v)
	default:
		panic(fmt.Errorf("column: unsupported type (%T)", value))
	}
}

func TestForString(t *testing.T) {
	coll := NewCollection()
	coll.CreateColumn("id", ForInt64())
	coll.CreateColumn("data", ForString())
	coll.CreateIndex("one", "id", func(r Reader) bool {
		return r.Int() == 1
	})

	data := []string{"a", "b", "c", "d"}
	for i, d := range data {
		coll.InsertObject(map[string]interface{}{"id": i, "data": d})
	}

	coll.Query(func(tx *Txn) error {
		tx.With("one").Select(func(v Selector) {
			assert.Equal(t, "b", v.StringAt("data"))
		})
		return nil
	})
}

func TestForKind(t *testing.T) {
	assert.NotNil(t, ForKind(reflect.String))
	assert.Panics(t, func() {
		ForKind(reflect.Invalid)
	})
}

func TestAtKey(t *testing.T) {
	const serial = "c68a66f6-7b90-4b3a-8105-cfde490df780"

	// Update a name
	players := loadPlayers(500)
	players.UpdateAtKey(serial, "name", func(v Cursor) error {
		v.SetString("Roman")
		return nil
	})

	// Read back and assert
	assertion := func(v Selector) {
		assert.Equal(t, "Roman", v.StringAt("name"))
		assert.Equal(t, "elf", v.StringAt("race"))
	}

	assert.True(t, players.SelectAtKey(serial, assertion))
	assert.NoError(t, players.Query(func(txn *Txn) error {
		assert.True(t, txn.SelectAtKey(serial, assertion))
		return nil
	}))
}

func TestUpdateAtKeyWithoutPK(t *testing.T) {
	col := NewCollection()
	assert.Error(t, col.UpdateAtKey("test", "name", func(v Cursor) error {
		return nil
	}))
}

func TestSelectAtKeyWithoutPK(t *testing.T) {
	col := NewCollection()
	assert.False(t, col.SelectAtKey("test", func(v Selector) {}))
}

func TestSnapshotBool(t *testing.T) {
	input := ForBool()
	input.Grow(8)
	applyChanges(input,
		Update{commit.Put, 2, true},
		Update{commit.Put, 5, true},
	)

	// Snapshot into a new buffer
	buf := commit.NewBuffer(8)
	input.Snapshot(0, buf)

	// Create a new reader and read the column
	rdr := commit.NewReader()
	rdr.Seek(buf)
	output := ForBool()
	output.Grow(8)
	output.Apply(rdr)
	assert.Equal(t, input, output)
}

func TestSnapshotIndex(t *testing.T) {
	predicateFn := func(Reader) bool {
		return true
	}
	input := newIndex("test", "a", predicateFn)
	input.Grow(8)
	applyChanges(input,
		Update{commit.Put, 2, true},
		Update{commit.Put, 5, true},
	)

	// Snapshot into a new buffer
	buf := commit.NewBuffer(8)
	input.Snapshot(0, buf)

	// Create a new reader and read the column
	rdr := commit.NewReader()
	rdr.Seek(buf)
	output := newIndex("test", "a", predicateFn)
	output.Grow(8)
	output.Apply(rdr)
	assert.Equal(t, input.Column.(*columnIndex).fill, output.Column.(*columnIndex).fill)
}
