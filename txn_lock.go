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

// chunkOf returns a part of a bitmap for the corresponding chunk
func chunkOf(v bitmap.Bitmap, chunk uint32) bitmap.Bitmap {
	const shift = chunkShift - 6
	x1 := min(int32(chunk+1)<<shift, int32(len(v)))
	x0 := min(int32(chunk)<<shift, x1)
	return v[x0:x1]
}

// chunkBounds resolves a chunk window bounds
func chunkBounds(chunk, maxSize uint32) (uint32, uint32) {
	x1 := min(int32(chunk+1)<<chunkShift, int32(maxSize))
	x0 := min(int32(chunk)<<chunkShift, x1)
	return uint32(x0), uint32(x1)
}

// min returns a minimum of two numbers without branches.
func min(v1, v2 int32) int32 {
	return v2 + ((v1 - v2) & ((v1 - v2) >> 31))
}

// --------------------------- Locked Range ---------------------------

// rangeRead iterates over index, chunk by chunk and ensures that each
// chunk is protected by an appropriate read lock.
func (txn *Txn) rangeRead(f func(offset uint32, index bitmap.Bitmap)) {
	limit := uint32(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := uint32(0); chunk <= limit; chunk++ {
		lock.RLock(uint(chunk))
		f(chunk<<chunkShift, chunkOf(txn.index, chunk))
		lock.RUnlock(uint(chunk))
	}
}

// rangeReadPair iterates over the index and another bitmap, chunk by chunk and
// ensures that each chunk is protected by an appropriate read lock.
func (txn *Txn) rangeReadPair(other bitmap.Bitmap, f func(a, b bitmap.Bitmap)) {
	limit := uint32(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := uint32(0); chunk <= limit; chunk++ {
		lock.RLock(uint(chunk))
		f(chunkOf(txn.index, chunk), chunkOf(other, chunk))
		lock.RUnlock(uint(chunk))
	}
}

// rangeWrite ranges over the dirty chunks and acquires exclusive latches along
// the way. This is used to commit a transaction.
func (txn *Txn) rangeWrite(fn func(chunk commit.Chunk, fill bitmap.Bitmap)) {
	txn.dirty.Range(func(chunk uint32) {
		txn.owner.writeAtChunk(commit.Chunk(chunk), fn)
	})
}

// writeAtChunk acquires appropriate locks for a chunk and executes a
// write callback
func (c *Collection) writeAtChunk(chunk commit.Chunk, fn func(chunk commit.Chunk, fill bitmap.Bitmap)) {
	lock := c.slock
	lock.Lock(uint(chunk))

	// Compute the fill
	c.lock.Lock()
	fill := chunk.OfBitmap(c.fill)
	c.lock.Unlock()

	// Call the delegate
	fn(chunk, fill)
	lock.Unlock(uint(chunk))
}
