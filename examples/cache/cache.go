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

const buckets = 100

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
		bucket := uint(i) // copy the value
		db.CreateIndex(strconv.Itoa(i), "crc", func(r column.Reader) bool {
			return r.Uint()%uint(buckets) == bucket
		})
	}

	return &Cache{
		bloom: make(bitmap.Bitmap, 8192),
		store: db,
	}
}

// Get attempts to retrieve a value for a key
func (c *Cache) Get(key string) (value string, found bool) {
	hash := crc32.ChecksumIEEE([]byte(key))
	value, idx := c.search(hash)
	return value, idx >= 0
}

// Set updates or inserts a new value
func (c *Cache) Set(key, value string) {
	hash := crc32.ChecksumIEEE([]byte(key))

	// First check if the value already exists, and update it if found.
	if c.checkFilter(hash) {
		if _, idx := c.search(hash); idx >= 0 {
			c.store.UpdateAt(uint32(idx), "val", func(v column.Cursor) error {
				v.SetString(value)
				return nil
			})
			return
		}
	}

	// If not found, insert a new row
	c.addToFilter(hash)
	c.store.Insert(map[string]interface{}{
		"key": key,
		"val": value,
		"crc": uint64(hash),
	})
}

// search attempts to retrieve a value for a key. If the value is found, it returns
// the actual value and its index in the collection. Otherwise, it returns -1.
func (c *Cache) search(hash uint32) (value string, index int) {
	index = -1

	c.store.Query(func(txn *column.Txn) error {
		bucketName := fmt.Sprintf("%d", hash%uint32(buckets))
		return txn.
			With(bucketName).
			WithUint("crc", func(v uint64) bool {
				return v == uint64(hash)
			}).Range("val", func(v column.Cursor) {
			value = v.String()
			index = int(v.Index())
		})
	})
	return
}

func (c *Cache) addToFilter(hash uint32) {
	position := hash % uint32(len(c.bloom)*64)
	c.bloom.Set(position)
}

func (c *Cache) checkFilter(hash uint32) bool {
	position := hash % uint32(len(c.bloom)*64)
	return c.bloom.Contains(position)
}
