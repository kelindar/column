// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"github.com/kelindar/bitmap"
)

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
	TypeInsert             // Insert inserts elements into the collection
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
	Inserts bitmap.Bitmap // The insert list
}

// Clone clones a commit into a new one
func (c *Commit) Clone() (clone Commit) {
	switch c.Type {
	case TypeStore:
		clone.Type = TypeStore
		clone.Column = c.Column
		clone.Updates = clone.Updates[:0]
		clone.Updates = append(clone.Updates, c.Updates...)
	case TypeDelete:
		clone.Type = TypeDelete
		clone.Deletes = clone.Deletes[:0]
		clone.Deletes = append(clone.Deletes, c.Deletes...)
	case TypeInsert:
		clone.Type = TypeInsert
		clone.Inserts = clone.Inserts[:0]
		clone.Inserts = append(clone.Inserts, c.Inserts...)
	}
	return
}
