// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

// Row represents a cursor at a particular row offest in the transaction.
type Row struct {
	txn *Txn
}

// --------------------------- Numbers ----------------------------

// Int loads a int value at a particular column
func (r Row) Int(columnName string) (v int, ok bool) {
	return readNumber[int](r.txn, columnName)
}

// SetInt stores a int value at a particular column
func (r Row) SetInt(columnName string, value int) {
	r.txn.Int(columnName).Set(value)
}

// AddInt adds delta to a int value at a particular column
func (r Row) AddInt(columnName string, value int) {
	r.txn.Int(columnName).Add(value)
}

// Int16 loads a int16 value at a particular column
func (r Row) Int16(columnName string) (v int16, ok bool) {
	return readNumber[int16](r.txn, columnName)
}

// SetInt16 stores a int16 value at a particular column
func (r Row) SetInt16(columnName string, value int16) {
	r.txn.Int16(columnName).Set(value)
}

// AddInt16 adds delta to a int16 value at a particular column
func (r Row) AddInt16(columnName string, value int16) {
	r.txn.Int16(columnName).Add(value)
}

// Int32 loads a int32 value at a particular column
func (r Row) Int32(columnName string) (v int32, ok bool) {
	return readNumber[int32](r.txn, columnName)
}

// SetInt32 stores a int32 value at a particular column
func (r Row) SetInt32(columnName string, value int32) {
	r.txn.Int32(columnName).Set(value)
}

// AddInt32 adds delta to a int32 value at a particular column
func (r Row) AddInt32(columnName string, value int32) {
	r.txn.Int32(columnName).Add(value)
}

// Int64 loads a int64 value at a particular column
func (r Row) Int64(columnName string) (v int64, ok bool) {
	return readNumber[int64](r.txn, columnName)
}

// SetInt64 stores a int64 value at a particular column
func (r Row) SetInt64(columnName string, value int64) {
	r.txn.Int64(columnName).Set(value)
}

// AddInt64 adds delta to a int64 value at a particular column
func (r Row) AddInt64(columnName string, value int64) {
	r.txn.Int64(columnName).Add(value)
}

// Uint loads a uint value at a particular column
func (r Row) Uint(columnName string) (v uint, ok bool) {
	return readNumber[uint](r.txn, columnName)
}

// SetUint stores a uint value at a particular column
func (r Row) SetUint(columnName string, value uint) {
	r.txn.Uint(columnName).Set(value)
}

// AddUint adds delta to a uint value at a particular column
func (r Row) AddUint(columnName string, value uint) {
	r.txn.Uint(columnName).Add(value)
}

// Uint16 loads a uint16 value at a particular column
func (r Row) Uint16(columnName string) (v uint16, ok bool) {
	return readNumber[uint16](r.txn, columnName)
}

// SetUint16 stores a uint16 value at a particular column
func (r Row) SetUint16(columnName string, value uint16) {
	r.txn.Uint16(columnName).Set(value)
}

// AddUint16 adds delta to a uint16 value at a particular column
func (r Row) AddUint16(columnName string, value uint16) {
	r.txn.Uint16(columnName).Add(value)
}

// Uint32 loads a uint32 value at a particular column
func (r Row) Uint32(columnName string) (v uint32, ok bool) {
	return readNumber[uint32](r.txn, columnName)
}

// SetUint32 stores a uint32 value at a particular column
func (r Row) SetUint32(columnName string, value uint32) {
	r.txn.Uint32(columnName).Set(value)
}

// AddUint32 adds delta to a uint32 value at a particular column
func (r Row) AddUint32(columnName string, value uint32) {
	r.txn.Uint32(columnName).Add(value)
}

// Uint64 loads a uint64 value at a particular column
func (r Row) Uint64(columnName string) (v uint64, ok bool) {
	return readNumber[uint64](r.txn, columnName)
}

// SetUint64 stores a uint64 value at a particular column
func (r Row) SetUint64(columnName string, value uint64) {
	r.txn.Uint64(columnName).Set(value)
}

// AddUint64 adds delta to a uint64 value at a particular column
func (r Row) AddUint64(columnName string, value uint64) {
	r.txn.Uint64(columnName).Add(value)
}

// Float32 loads a float32 value at a particular column
func (r Row) Float32(columnName string) (v float32, ok bool) {
	return readNumber[float32](r.txn, columnName)
}

// SetFloat32 stores a float32 value at a particular column
func (r Row) SetFloat32(columnName string, value float32) {
	r.txn.Float32(columnName).Set(value)
}

// AddFloat32 adds delta to a float32 value at a particular column
func (r Row) AddFloat32(columnName string, value float32) {
	r.txn.Float32(columnName).Add(value)
}

// Float64 loads a float64 value at a particular column
func (r Row) Float64(columnName string) (float64, bool) {
	return readNumber[float64](r.txn, columnName)
}

// SetFloat64 stores a float64 value at a particular column
func (r Row) SetFloat64(columnName string, value float64) {
	r.txn.Float64(columnName).Set(value)
}

// AddFloat64 adds delta to a float64 value at a particular column
func (r Row) AddFloat64(columnName string, value float64) {
	r.txn.Float64(columnName).Add(value)
}

// --------------------------- Strings ----------------------------

// Key loads a primary key value at a particular column
func (r Row) Key() (v string, ok bool) {
	if pk := r.txn.owner.pk; pk != nil {
		v, ok = pk.LoadString(r.txn.cursor)
	}
	return
}

// SetKey stores a primary key value at a particular column
func (r Row) SetKey(key string) {
	r.txn.Key().Set(key)
}

// String loads a string value at a particular column
func (r Row) String(columnName string) (v string, ok bool) {
	return textReaderFor[*columnString](r.txn, columnName).Get()
}

// SetString stores a string value at a particular column
func (r Row) SetString(columnName string, value string) {
	r.txn.String(columnName).Set(value)
}

// MergeString merges a string value at a particular column
func (r Row) MergeString(columnName string, value string) {
	r.txn.String(columnName).Merge(value)
}

// Enum loads a string value at a particular column
func (r Row) Enum(columnName string) (v string, ok bool) {
	return textReaderFor[*columnEnum](r.txn, columnName).Get()
}

// SetEnum stores a string value at a particular column
func (r Row) SetEnum(columnName string, value string) {
	r.txn.Enum(columnName).Set(value)
}

// --------------------------- Others ----------------------------

// Bool loads a bool value at a particular column
func (r Row) Bool(columnName string) bool {
	return boolReaderFor(r.txn, columnName).Get()
}

// SetBool stores a bool value at a particular column
func (r Row) SetBool(columnName string, value bool) {
	r.txn.Bool(columnName).Set(value)
}

// Any loads a bool value at a particular column
func (r Row) Any(columnName string) (any, bool) {
	return anyReaderFor(r.txn, columnName).Get()
}

// SetAny stores a bool value at a particular column
func (r Row) SetAny(columnName string, value interface{}) {
	r.txn.Any(columnName).Set(value)
}
