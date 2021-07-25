// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"encoding/binary"
	"math"
	"reflect"
	"sync"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Enum ----------------------------

var _ Textual = new(columnEnum)

// columnEnum represents a enumerable string column
type columnEnum struct {
	lock  sync.RWMutex
	fill  bitmap.Bitmap     // The fill-list
	locs  []uint32          // The list of locations
	data  []byte            // The actual values
	cache map[string]uint32 // Cache for string locations (no need to persist)
}

// makeEnum creates a new column
func makeEnum() Column {
	return &columnEnum{
		fill:  make(bitmap.Bitmap, 0, 4),
		locs:  make([]uint32, 0, 64),
		data:  make([]byte, 0, 16*32),
		cache: make(map[string]uint32, 16),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnEnum) Grow(idx uint32) {
	if idx < uint32(len(c.locs)) {
		return
	}

	if idx < uint32(cap(c.locs)) {
		c.fill.Grow(idx)
		c.locs = c.locs[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint32, idx+1, capacityFor(idx+1))
	copy(clone, c.locs)
	c.locs = clone
}

// Apply applies a set of operations to the column.
func (c *columnEnum) Apply(r *commit.Reader) {
	for r.Next() {
		if r.Type == commit.Put {

			// Attempt to find if we already have the location of this value from the
			// cache, and if we don't, find it and set the offset for faster lookup.
			value := r.String()

			c.lock.RLock()
			offset, cached := c.cache[value]
			c.lock.RUnlock()

			if !cached {
				c.lock.Lock()
				offset = c.findOrAdd(value)
				c.cache[value] = offset
				c.lock.Unlock()
			}

			// Set the value at the index
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.locs[r.Offset] = offset
		}
	}
}

// Search for the string or adds it and returns the offset
func (c *columnEnum) findOrAdd(v string) uint32 {
	value := toBytes(v)
	target := hash32(value)
	for i := 0; i < len(c.data); {
		hash := binary.BigEndian.Uint32(c.data[i : i+4])
		size := int(c.data[i+4])
		if hash == target {
			return uint32(i + 4)
		}

		i += 5 + size
	}

	// Not found, add
	var head [5]byte
	binary.BigEndian.PutUint32(head[0:4], target)
	head[4] = byte(len(value)) // Max 255 chars
	addedAt := len(c.data)
	c.data = append(c.data, head[:]...)
	c.data = append(c.data, value...)
	return uint32(addedAt + 4)
}

// readAt reads a string at a location
func (c *columnEnum) readAt(at uint32) string {
	size := uint32(c.data[at])
	data := c.data[at+1 : at+1+size]
	return toString(&data)
}

// Delete deletes a set of items from the column.
func (c *columnEnum) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)

	// TODO: remove unused strings, need some reference counting for that
	// and can proably be done during vacuum() instead
}

// Value retrieves a value at a specified index
func (c *columnEnum) Value(idx uint32) (v interface{}, ok bool) {
	return c.LoadString(idx)
}

// LoadString retrieves a value at a specified index
func (c *columnEnum) LoadString(idx uint32) (v string, ok bool) {
	if idx < uint32(len(c.locs)) && c.fill.Contains(idx) {
		v, ok = c.readAt(c.locs[idx]), true
	}
	return
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnEnum) FilterString(offset uint32, index bitmap.Bitmap, predicate func(v string) bool) {
	cache := struct {
		index uint32 // Last seen offset
		value bool   // Last evaluated predicate
	}{}
	cache.index = math.MaxUint32

	// Do a quick ellimination of elements which are NOT contained in this column, this
	// allows us not to check contains during the filter itself
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])

	// Filters down the strings, if strings repeat we avoid reading every time by
	// caching the last seen index/value combination.
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		if idx < uint32(len(c.locs)) {
			if at := c.locs[idx]; at != cache.index {
				cache.index = at
				cache.value = predicate(c.readAt(at))
				return cache.value
			}

			// The value is cached, avoid evaluating it
			return cache.value
		}
		return false
	})
}

// Contains checks whether the column has a value at a specified index.
func (c *columnEnum) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnEnum) Index() *bitmap.Bitmap {
	return &c.fill
}

// --------------------------- String ----------------------------

var _ Textual = new(columnString)

// columnString represents a string column
type columnString struct {
	fill bitmap.Bitmap // The fill-list
	data []string      // The actual values
}

// makeString creates a new string column
func makeStrings() Column {
	return &columnString{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]string, 0, 64),
	}
}

// Grow grows the size of the column until we have enough to store
func (c *columnString) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]string, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Apply applies a set of operations to the column.
func (c *columnString) Apply(r *commit.Reader) {

	// Update the values of the column, for this one we can only process stores
	for r.Next() {
		if r.Type == commit.Put {
			c.fill[r.Offset>>6] |= 1 << (r.Offset & 0x3f)
			c.data[r.Offset] = string(r.Bytes())
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnString) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Value retrieves a value at a specified index
func (c *columnString) Value(idx uint32) (v interface{}, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// Contains checks whether the column has a value at a specified index.
func (c *columnString) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnString) Index() *bitmap.Bitmap {
	return &c.fill
}

// LoadString retrieves a value at a specified index
func (c *columnString) LoadString(idx uint32) (string, bool) {
	v, has := c.Value(idx)
	s, ok := v.(string)
	return s, has && ok
}

// FilterString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (c *columnString) FilterString(offset uint32, index bitmap.Bitmap, predicate func(v string) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(c.data[idx])
	})
}

// --------------------------- Hash ----------------------------

const (
	offset32 = uint32(2166136261)
	prime32  = uint32(16777619)
	init32   = offset32
)

// hash32 returns the hash of b.
func hash32(b []byte) uint32 {
	return hashBytes32(init32, b)
}

// hashBytes32 adds the hash of b to the precomputed hash value h.
func hashBytes32(h uint32, b []byte) uint32 {
	for len(b) >= 8 {
		h = (h ^ uint32(b[0])) * prime32
		h = (h ^ uint32(b[1])) * prime32
		h = (h ^ uint32(b[2])) * prime32
		h = (h ^ uint32(b[3])) * prime32
		h = (h ^ uint32(b[4])) * prime32
		h = (h ^ uint32(b[5])) * prime32
		h = (h ^ uint32(b[6])) * prime32
		h = (h ^ uint32(b[7])) * prime32
		b = b[8:]
	}

	if len(b) >= 4 {
		h = (h ^ uint32(b[0])) * prime32
		h = (h ^ uint32(b[1])) * prime32
		h = (h ^ uint32(b[2])) * prime32
		h = (h ^ uint32(b[3])) * prime32
		b = b[4:]
	}

	if len(b) >= 2 {
		h = (h ^ uint32(b[0])) * prime32
		h = (h ^ uint32(b[1])) * prime32
		b = b[2:]
	}

	if len(b) > 0 {
		h = (h ^ uint32(b[0])) * prime32
	}

	return h
}

// --------------------------- Convert ----------------------------

// toBytes converts a string to a byte slice without allocating.
func toBytes(v string) (b []byte) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&v))
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = strHeader.Data

	l := len(v)
	byteHeader.Len = l
	byteHeader.Cap = l
	return
}

// toString converts a strign to a byte slice without allocating.
func toString(b *[]byte) string {
	return *(*string)(unsafe.Pointer(b))
}
