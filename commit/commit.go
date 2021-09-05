// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"github.com/kelindar/bitmap"
)

// --------------------------- Commit ----------------------------

// Commit represents an individual transaction commit. If multiple columns are committed
// in the same transaction, it would result in multiple commits per transaction.
type Commit struct {
	Chunk   uint32        // The chunk number
	Updates []*Buffer     // The update buffers
	Dirty   bitmap.Bitmap // The dirty bitmap (TODO: rebuild instead?)
}

// Clone clones a commit into a new one
func (c *Commit) Clone() (clone Commit) {
	clone.Chunk = c.Chunk
	c.Dirty.Clone(&clone.Dirty)

	for _, u := range c.Updates {
		if len(u.buffer) > 0 {
			clone.Updates = append(clone.Updates, u.Clone())
		}
	}
	return
}
