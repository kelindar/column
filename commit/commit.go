// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import "github.com/kelindar/bitmap"

// --------------------------- Chunk ----------------------------

const (
	bitmapShift = chunkShift - 6
	bitmapSize  = 1 << bitmapShift
	chunkShift  = 14 // 16K
	chunkSize   = 1 << chunkShift
)

// Chunk represents a chunk number
type Chunk uint32

// ChunkAt returns the chunk number at a given index
func ChunkAt(index uint32) Chunk {
	return Chunk(index) / chunkSize
}

// OfBitmap computes a chunk for a given bitmap
func (c Chunk) OfBitmap(v bitmap.Bitmap) bitmap.Bitmap {
	const shift = chunkShift - 6
	x1 := min(int32(c+1)<<shift, int32(len(v)))
	x0 := min(int32(c)<<shift, x1)
	return v[x0:x1]
}

// Offset returns the offset at which the chunk should be positioned
func (c Chunk) Offset() uint32 {
	return uint32(int32(c) << chunkShift)
}

// Range iterates over a chunk given a bitmap
func (c Chunk) Range(v bitmap.Bitmap, fn func(idx uint32)) {
	offset := c.Offset()
	output := c.OfBitmap(v)
	output.Range(func(idx uint32) {
		fn(offset + idx)
	})
}

// min returns a minimum of two numbers without branches.
func min(v1, v2 int32) int32 {
	return v2 + ((v1 - v2) & ((v1 - v2) >> 31))
}

// --------------------------- Commit ----------------------------

// Writer represents a contract that a commit writer must implement
type Writer interface {
	Write(commit Commit) error
}

// Commit represents an individual transaction commit. If multiple chunks are committed
// in the same transaction, it would result in multiple commits per transaction.
type Commit struct {
	Chunk   Chunk     // The chunk number
	Updates []*Buffer // The update buffers
}

// Clone clones a commit into a new one
func (c *Commit) Clone() (clone Commit) {
	clone.Chunk = c.Chunk
	for _, u := range c.Updates {
		if len(u.buffer) > 0 {
			clone.Updates = append(clone.Updates, u.Clone())
		}
	}
	return
}

// --------------------------- Channel ----------------------------

var _ Writer = new(Channel)

// Channel represents an impementation of a commit writer that simply sends each commit
// into the channel.
type Channel chan Commit

// Write clones the commit and writes it into the writer
func (w *Channel) Write(commit Commit) error {
	*w <- commit.Clone()
	return nil
}
