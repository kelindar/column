// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"github.com/kelindar/bitmap"
)

// --------------------------- Update Type ----------------------------

// Update represents an update operation
type Update struct {
	Type  OpType      // The type of an update operation
	Index uint32      // The index to update/delete
	Value interface{} // The value to update to
}

// --------------------------- Update Type ----------------------------

// Updates represents a list of updates for a column column.
type Updates struct {
	Column  string   // The column name
	Update  []Update // The update queue
	Offsets []int32  // The offsets of chunks
	Current int      // The current chunk
}

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
	Updates []Updates     // The update list
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
	clone.Deletes = append(clone.Deletes, c.Deletes...)
	clone.Inserts = append(clone.Inserts, c.Inserts...)
	clone.Dirty = append(clone.Dirty, c.Dirty...)
	for _, u := range c.Updates {
		if len(u.Update) > 0 {
			updates := make([]Update, 0, len(u.Update))
			updates = append(updates, u.Update...)
			offsets := make([]int32, 0, len(u.Offsets))
			offsets = append(offsets, u.Offsets...)
			clone.Updates = append(clone.Updates, Updates{
				Column:  u.Column,
				Offsets: offsets,
				Update:  updates,
			})
		}
	}
	return
}
