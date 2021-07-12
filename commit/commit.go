// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"github.com/kelindar/bitmap"
)

// --------------------------- Commit Type ----------------------------

// Type represents a type of a commit operation.
type Type uint8

// Various commit types
const (
	Store  = Type(1 << 0) // Store stores (updates or inserts) a set of values
	Insert = Type(1 << 1) // Insert inserts elements into the collection
	Delete = Type(1 << 2) // Delete deletes a set of entries in the collection
)

// String returns the string representation of the type
func (t Type) String() (op string) {
	switch t {
	case Store | Insert | Delete:
		return "store,insert,delete"
	case Store | Insert:
		return "store,insert"
	case Store | Delete:
		return "store,delete"
	case Insert | Delete:
		return "insert,delete"
	case Store:
		return "store"
	case Insert:
		return "insert"
	case Delete:
		return "delete"
	default:
		return "invalid"
	}
}

// --------------------------- Commit ----------------------------

// Commit represents an individual transaction commit. If multiple columns are committed
// in the same transaction, it would result in multiple commits per transaction.
type Commit struct {
	Type    Type          // The type of the commit
	Chunk   uint32        // The chunk number
	Updates []*Buffer     // The update buffers
	Dirty   bitmap.Bitmap // The dirty bitmap (TODO: rebuild instead?)
	Deletes bitmap.Bitmap // The delete list
	Inserts bitmap.Bitmap // The insert list
}

// Is returns whether a commit is of a specified type
func (c *Commit) Is(t Type) bool {
	return (c.Type & t) == t
}

// Clone clones a commit into a new one
func (c *Commit) Clone() (clone Commit) {
	clone.Type = c.Type
	clone.Chunk = c.Chunk

	c.Deletes.Clone(&clone.Deletes)
	c.Inserts.Clone(&clone.Inserts)
	c.Dirty.Clone(&clone.Dirty)

	for _, u := range c.Updates {
		if len(u.buffer) > 0 {
			buffer := make([]byte, len(u.buffer))
			copy(buffer, u.buffer)
			chunks := make([]header, 0, len(u.chunks))
			chunks = append(chunks, u.chunks...)
			clone.Updates = append(clone.Updates, &Buffer{
				Column: u.Column,
				buffer: buffer,
				chunks: chunks,
				last:   u.last,
				chunk:  u.chunk,
			})
		}
	}
	return
}
