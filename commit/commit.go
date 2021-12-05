// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

// Writer represents a contract that a commit writer must implement
type Writer interface {
	Write(commit Commit) error
}

// --------------------------- Commit ----------------------------

// Commit represents an individual transaction commit. If multiple chunks are committed
// in the same transaction, it would result in multiple commits per transaction.
type Commit struct {
	Chunk   uint32    // The chunk number
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
