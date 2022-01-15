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
	column.Grow(1)
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

		// FilterFloat64
		index := bitmap.Bitmap{0xffff}
		column.FilterFloat64(0, index, func(v float64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

		// FilterInt64
		index = bitmap.Bitmap{0xffff}
		column.FilterInt64(0, index, func(v int64) bool {
			return false
		})
		assert.Equal(t, 0, index.Count())

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
	}
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
		column, err := ForKind(v)
		assert.NoError(t, err)
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

func TestForKindInvalid(t *testing.T) {
	c, err := ForKind(reflect.Invalid)
	assert.Nil(t, c)
	assert.Error(t, err)
}

func TestAtKey(t *testing.T) {
	const serial = "c68a66f6-7b90-4b3a-8105-cfde490df780"

	// Update a name
	players := loadPlayers(500)
	players.UpdateAtKey(serial, func(txn *Txn, index uint32) error {
		name := txn.Enum("name")
		name.Set(index, "Roman")
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
	assert.Error(t, col.UpdateAtKey("test", func(txn *Txn, index uint32) error {
		name := txn.Enum("name")
		name.Set(index, "Roman")
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
	applyChanges(input.Column,
		Update{commit.Put, 2, true},
		Update{commit.Put, 5, true},
	)

	// Snapshot into a new buffer
	buf := commit.NewBuffer(8)
	input.Column.Snapshot(0, buf)

	// Create a new reader and read the column
	rdr := commit.NewReader()
	rdr.Seek(buf)
	output := newIndex("test", "a", predicateFn)
	output.Grow(8)
	output.Apply(rdr)
	assert.Equal(t, input.Column.(*columnIndex).fill, output.Column.(*columnIndex).fill)
}

func TestResize(t *testing.T) {
	assert.Equal(t, 1, resize(100, 0))
	assert.Equal(t, 2, resize(100, 1))
	assert.Equal(t, 4, resize(100, 2))
	assert.Equal(t, 16, resize(100, 11))
	assert.Equal(t, 256, resize(100, 255))
	assert.Equal(t, 1232, resize(100, 1000))
	assert.Equal(t, 1232, resize(200, 1000))
	assert.Equal(t, 1232, resize(512, 1000))
	assert.Equal(t, 1213, resize(500, 1000)) // Inconsistent
	assert.Equal(t, 22504, resize(512, 20000))
	assert.Equal(t, 28322, resize(22504, 22600))
}

func TestAccessors(t *testing.T) {
	tests := []struct {
		column Column
		value  interface{}
		access func(*Txn, string) interface{}
	}{
		{column: ForEnum(), value: "mage", access: func(txn *Txn, n string) interface{} { return txn.Enum(n) }},
		{column: ForString(), value: "test", access: func(txn *Txn, n string) interface{} { return txn.String(n) }},
		{column: ForInt(), value: int(99), access: func(txn *Txn, n string) interface{} { return txn.Int(n) }},
		{column: ForInt16(), value: int16(99), access: func(txn *Txn, n string) interface{} { return txn.Int16(n) }},
		{column: ForInt32(), value: int32(99), access: func(txn *Txn, n string) interface{} { return txn.Int32(n) }},
		{column: ForInt64(), value: int64(99), access: func(txn *Txn, n string) interface{} { return txn.Int64(n) }},
		{column: ForUint(), value: uint(99), access: func(txn *Txn, n string) interface{} { return txn.Uint(n) }},
		{column: ForUint16(), value: uint16(99), access: func(txn *Txn, n string) interface{} { return txn.Uint16(n) }},
		{column: ForUint32(), value: uint32(99), access: func(txn *Txn, n string) interface{} { return txn.Uint32(n) }},
		{column: ForUint64(), value: uint64(99), access: func(txn *Txn, n string) interface{} { return txn.Uint64(n) }},
		{column: ForFloat32(), value: float32(99.5), access: func(txn *Txn, n string) interface{} { return txn.Float32(n) }},
		{column: ForFloat64(), value: float64(99.5), access: func(txn *Txn, n string) interface{} { return txn.Float64(n) }},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%T", tc.column), func(t *testing.T) {
			col := NewCollection()
			assert.NoError(t, col.CreateColumn("pk", ForKey()))
			assert.NoError(t, col.CreateColumn("column", tc.column))

			// Invoke 'Set' method of the accessor
			assert.NoError(t, col.Query(func(txn *Txn) error {
				column := tc.access(txn, "column")
				assert.Len(t, invoke(column, "Set", uint32(0), tc.value), 0)
				return nil
			}))

			// Invoke 'Get' method of the accessor
			assert.NoError(t, col.Query(func(txn *Txn) error {
				column := tc.access(txn, "column")
				assert.GreaterOrEqual(t, len(invoke(column, "Get", uint32(0))), 1)
				return nil
			}))

			// If it has 'Add' method, try to invoke it
			assert.NoError(t, col.Query(func(txn *Txn) error {
				column := tc.access(txn, "column")
				if m := reflect.ValueOf(column).MethodByName("Add"); m.IsValid() {
					assert.Len(t, invoke(column, "Add", uint32(0), tc.value), 0)
				}
				return nil
			}))

			// Invalid column  name should panic
			assert.Panics(t, func() {
				col.Query(func(txn *Txn) error {
					tc.access(txn, "invalid")
					return nil
				})
			})

			// Invalid column type should panic
			assert.Panics(t, func() {
				col.Query(func(txn *Txn) error {
					tc.access(txn, "pk")
					return nil
				})
			})
		})
	}
}

func TestBooleanAccessor(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("active", ForBool()))
	assert.NoError(t, col.CreateColumn("name", ForString()))

	// Insert a boolean value
	_, err := col.Insert(func(txn *Txn, index uint32) error {
		txn.Bool("active").Set(index, true)
		txn.String("name").Set(index, "Roman")
		return nil
	})
	assert.NoError(t, err)

	// Boolean should also work for name
	col.Query(func(txn *Txn) error {
		active := txn.Bool("active")
		hasName := txn.Bool("name")

		assert.True(t, active.Get(0))
		assert.True(t, hasName.Get(0))
		return nil
	})
}

func TestInvalidPKAccessor(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("pk", ForString()))
	assert.Panics(t, func() {
		col.Query(func(txn *Txn) error {
			txn.Key()
			return nil
		})
	})
}

func invoke(any interface{}, name string, args ...interface{}) []reflect.Value {
	inputs := make([]reflect.Value, len(args))
	for i, _ := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}

	return reflect.ValueOf(any).MethodByName(name).Call(inputs)
}
