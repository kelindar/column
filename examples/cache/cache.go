// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"fmt"
	"hash/crc32"
	"strconv"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column"
)

const buckets = 32

// Cache represents a key-value store
type Cache struct {
	bloom bitmap.Bitmap // Our silly bloom filter
	store *column.Collection
}

// New creates a new key-value cache
func New() *Cache {
	db := column.NewCollection()
	db.CreateColumn("key", column.ForString())
	db.CreateColumn("val", column.ForString())
	db.CreateColumn("crc", column.ForUint64())

	// Create a bunch of buckets for faster retrieval
	for i := 0; i < buckets; i++ {
		db.CreateColumn(strconv.Itoa(i), column.ForBool())
	}

	return &Cache{
		bloom: make(bitmap.Bitmap, 1<<16),
		store: db,
	}
}

// Get attempts to retrieve a value for a key
func (c *Cache) Get(key string) (value string, found bool) {
	value, idx := c.search([]byte(key))
	return value, idx >= 0
}

// Set updates or inserts a new value
func (c *Cache) Set(key, value string) {
	k := []byte(key)

	// First check if the value already exists, and update it if found.
	if c.checkFilter(value) {
		if _, idx := c.search([]byte(key)); idx >= 0 {
			c.store.UpdateAt(uint32(idx), "val", func(v column.Cursor) error {
				v.SetString(value)
				return nil
			})
			return
		}
	}

	// If not found, insert a new row
	c.addToFilter(value)

	h1 := fmt.Sprintf("%d", crc32.Checksum(k, hash1)%buckets)
	h2 := fmt.Sprintf("%d", crc32.Checksum(k, hash2)%buckets)
	h3 := fmt.Sprintf("%d", crc32.Checksum(k, hash3)%buckets)

	c.store.Insert(map[string]interface{}{
		"key": key,
		"val": value,
		"crc": uint64(crc32.ChecksumIEEE(k)),
		h1:    true,
		h2:    true,
		h3:    true,
	})
}

// search attempts to retrieve a value for a key. If the value is found, it returns
// the actual value and its index in the collection. Otherwise, it returns -1.
func (c *Cache) search(key []byte) (value string, index int) {
	index = -1
	h1 := fmt.Sprintf("%d", crc32.Checksum(key, hash1)%buckets)
	h2 := fmt.Sprintf("%d", crc32.Checksum(key, hash2)%buckets)
	h3 := fmt.Sprintf("%d", crc32.Checksum(key, hash3)%buckets)

	hash := crc32.ChecksumIEEE([]byte(key))
	c.store.Query(func(txn *column.Txn) error {
		return txn.
			With(h1, h2, h3).
			WithUint("crc", func(v uint64) bool {
				return v == uint64(hash)
			}).Range("val", func(v column.Cursor) {
			value = v.String()
			index = int(v.Index())
		})
	})
	return
}

func (c *Cache) addToFilter(value string) {
	b1, b2, b3 := bloomBits([]byte(value), len(c.bloom))
	c.bloom.Set(b1)
	c.bloom.Set(b2)
	c.bloom.Set(b3)
}

func (c *Cache) checkFilter(value string) bool {
	b1, b2, b3 := bloomBits([]byte(value), len(c.bloom))
	return c.bloom.Contains(b1) && c.bloom.Contains(b2) && c.bloom.Contains(b3)
}

// Couple of hash functions for the bloom filter
var hash1 = crc32.MakeTable(crc32.Koopman)
var hash2 = crc32.MakeTable(crc32.Castagnoli)
var hash3 = crc32.MakeTable(crc32.IEEE)

func bloomBits(v []byte, length int) (uint32, uint32, uint32) {
	size := uint32(length * 64)
	return crc32.Checksum(v, hash1) % size,
		crc32.Checksum(v, hash2) % size,
		crc32.Checksum(v, hash3) % size
}
