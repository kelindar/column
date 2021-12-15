// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

const (
	bitmapShift = chunkShift - 6
	bitmapSize  = 1 << bitmapShift
	chunkShift  = 14 // 16K
	chunkSize   = 1 << chunkShift
)

// --------------------------- Locked Range ---------------------------

// rangeRead iterates over index, chunk by chunk and ensures that each
// chunk is protected by an appropriate read lock.
func (txn *Txn) rangeRead(f func(offset uint32, index bitmap.Bitmap)) {
	limit := commit.Chunk(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := commit.Chunk(0); chunk <= limit; chunk++ {
		lock.RLock(uint(chunk))
		f(chunk.Min(), chunk.OfBitmap(txn.index))
		lock.RUnlock(uint(chunk))
	}
}

// rangeReadPair iterates over the index and another bitmap, chunk by chunk and
// ensures that each chunk is protected by an appropriate read lock.
func (txn *Txn) rangeReadPair(other bitmap.Bitmap, f func(a, b bitmap.Bitmap)) {
	limit := commit.Chunk(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := commit.Chunk(0); chunk <= limit; chunk++ {
		lock.RLock(uint(chunk))
		f(chunk.OfBitmap(txn.index), chunk.OfBitmap(other))
		lock.RUnlock(uint(chunk))
	}
}

// rangeWrite ranges over the dirty chunks and acquires exclusive latches along
// the way. This is used to commit a transaction.
func (txn *Txn) rangeWrite(fn func(chunk commit.Chunk, fill bitmap.Bitmap) error) {
	txn.dirty.Range(func(chunk uint32) {
		txn.owner.writeAtChunk(commit.Chunk(chunk), fn)
	})
}

// writeAtChunk acquires appropriate locks for a chunk and executes a
// write callback
func (c *Collection) writeAtChunk(chunk commit.Chunk, fn func(chunk commit.Chunk, fill bitmap.Bitmap) error) (err error) {
	lock := c.slock
	lock.Lock(uint(chunk))

	// Compute the fill
	c.lock.Lock()
	fill := chunk.OfBitmap(c.fill)
	c.lock.Unlock()

	// Call the delegate
	err = fn(chunk, fill)
	lock.Unlock(uint(chunk))
	return
}
