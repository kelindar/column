// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"github.com/kelindar/bitmap"
)

const (
	bitmapShift = chunkShift - 6
	bitmapSize  = 1 << bitmapShift
	chunkShift  = 14 // 16K
	chunkSize   = 1 << chunkShift
)

// --------------------------- Locked Range ---------------------------

// rlockEach iterates over index, chunk by chunk and ensures that each
// chunk is protected by an appropriate read lock.
func (txn *Txn) rlockEach(f func(offset uint32, index bitmap.Bitmap)) {
	limit := uint32(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := uint32(0); chunk <= limit; chunk++ {
		at := chunk << bitmapShift
		fill := txn.index[at:]
		if len(fill) > bitmapSize {
			fill = txn.index[at : at+bitmapSize]
		}

		lock.RLock(uint(chunk))
		f(chunk<<chunkShift, fill)
		lock.RUnlock(uint(chunk))
	}
}

// rlockEachPair iterates over the index and another bitmap, chunk by chunk and
// ensures that each chunk is protected by an appropriate read lock.
func (txn *Txn) rlockEachPair(other bitmap.Bitmap, f func(a, b bitmap.Bitmap)) {
	limit := uint32(len(txn.index) >> bitmapShift)
	lock := txn.owner.slock

	for chunk := uint32(0); chunk <= limit; chunk++ {
		at := chunk << bitmapShift
		dst, src := txn.index[at:], other[at:]
		if len(dst) > bitmapSize {
			dst = txn.index[at : at+bitmapSize]
			src = other[at : at+bitmapSize]
		}

		lock.RLock(uint(chunk))
		f(dst, src)
		lock.RUnlock(uint(chunk))
	}
}

func (txn *Txn) commitEach(f func(chunk uint32, fill bitmap.Bitmap)) {
	lock := txn.owner.slock
	fill := txn.owner.fill

	txn.dirty.Range(func(chunk uint32) {
		start, end := chunk<<chunkShift, (chunk+1)<<chunkShift
		if capacity := uint32(len(fill)) << 6; capacity < end {
			end = capacity
		}

		lock.Lock(uint(chunk))
		f(chunk, fill[start>>6:end>>6])
		lock.Unlock(uint(chunk))
	})
}
