// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Enum ----------------------------

// columnEnum represents a enumerable string column
type columnEnum struct {
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
		c.locs = c.locs[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]uint32, idx+1, capacityFor(idx+1))
	copy(clone, c.locs)
	c.locs = clone
}

// Update performs a series of updates at once
func (c *columnEnum) Update(updates []commit.Update) {
	for _, u := range updates {
		if u.Type == commit.Put {

			// Attempt to find if we already have the location of this value from the
			// cache, and if we don't, find it and set the offset for faster lookup.
			offset, cached := c.cache[u.Value.(string)]
			if !cached {
				offset = c.findOrAdd(u.Value.(string))
				c.cache[u.Value.(string)] = offset
			}

			// Set the value at the index
			c.fill.Set(u.Index)
			c.locs[u.Index] = offset
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
func (c *columnEnum) Delete(items *bitmap.Bitmap) {
	c.fill.AndNot(*items)

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
func (c *columnEnum) FilterString(index *bitmap.Bitmap, predicate func(v string) bool) {
	cache := struct {
		index uint32 // Last seen offset
		value string // Last seen value
	}{}

	// Filters down the strings, if strings repeat we avoid reading every time by
	// caching the last seen index/value combination.
	index.Filter(func(idx uint32) (match bool) {
		if idx < uint32(len(c.locs)) && c.fill.Contains(idx) {
			if at := c.locs[idx]; at != cache.index {
				v := c.readAt(at)
				cache.index = at
				cache.value = v
				return predicate(v)
			}

			// The value is cached, avoid reading it
			return predicate(cache.value)
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
