// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"sync"

	"github.com/kelindar/bitmap"
)

// --------------------------- Pool of Commits ----------------------------

// commits represents a pool of commits
var commits = &sync.Pool{
	New: func() interface{} {
		return &Commit{
			Deletes: make(bitmap.Bitmap, 0, 4),
			Updates: make([]Update, 0, 256),
		}
	},
}

// acquire acquires a commit and reinitializes it
func acquire(kind Type) *Commit {
	commit := commits.Get().(*Commit)
	commit.Type = kind
	commit.Column = ""
	commit.Deletes = commit.Deletes[:0]
	commit.Updates = commit.Updates[:0]
	return commit
}

// --------------------------- Update Type ----------------------------

// UpdateType represents a type of an update operation.
type UpdateType uint8

// Various update operations supported.
const (
	Put UpdateType = iota // Put stores a value regardless of a previous value
	Add                   // Add increments the current stored value by the amount
)

// Update represents an update operation
type Update struct {
	Type  UpdateType  // The type of an update operation
	Index uint32      // The index to update/delete
	Value interface{} // The value to update to
}

// --------------------------- Commit Type ----------------------------

// Type represents a type of a commit operation.
type Type uint8

// Various commit types
const (
	_          Type = iota // Invalid
	TypeStore              // Store stores (updates or inserts) a set of values
	TypeDelete             // Delete deletes a set of entries in the collection
)

// --------------------------- Commit ----------------------------

// Commit represents an individual transaction commit. If multiple columns are committed
// in the same transaction, it would result in multiple commits per transaction.
type Commit struct {
	Type    Type          // The type of the commit
	Column  string        // The column name
	Updates []Update      // The update list
	Deletes bitmap.Bitmap // The delete list
}

// ForStore allocates a commit for a set of updates.
func ForStore(columnName string, updates []Update) *Commit {
	c := acquire(TypeStore)
	c.Column = columnName
	c.Updates = updates
	return c
}

// ForDelete allocates a commit for a set of deletes.
func ForDelete(deletes bitmap.Bitmap) *Commit {
	c := acquire(TypeDelete)
	c.Deletes = deletes
	return c
}

// Clone clones a commit into a new one
func (c *Commit) Clone() *Commit {
	clone := acquire(c.Type)
	switch c.Type {
	case TypeStore:
		clone.Column = c.Column
		for _, v := range c.Updates {
			clone.Updates = append(clone.Updates, v)
		}
	case TypeDelete:
		for _, v := range c.Deletes {
			clone.Deletes = append(clone.Deletes, v)
		}
	}
	return clone
}

// Close releases a commit back to the pool so it can be reused without adding
// to the GC pressure.
func (c *Commit) Close() error {
	commits.Put(c)
	return nil
}
