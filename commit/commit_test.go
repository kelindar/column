// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"testing"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/stretchr/testify/assert"
)

func TestCommits(t *testing.T) {
	commit1 := Commit{
		Type:    Delete,
		Deletes: bitmap.Bitmap{0xff},
	}
	commit2 := Commit{
		Type: Store,
		Updates: []Updates{{
			Column: "test",
			Update: []Update{{Type: Put, Index: 5, Value: "hi"}},
		}},
	}
	commit3 := Commit{
		Type:    Insert,
		Inserts: bitmap.Bitmap{0xaa},
	}

	// Assert types
	assert.Equal(t, Delete, commit1.Type)
	assert.Equal(t, Store, commit2.Type)
	assert.Equal(t, Insert, commit3.Type)

	// Clone and assert
	clone1 := commit1.Clone()
	clone2 := commit2.Clone()
	clone3 := commit3.Clone()
	assert.Equal(t, commit1, clone1, "clone1")
	assert.Equal(t, commit2, clone2, "clone2")
	assert.Equal(t, commit3, clone3, "clone3")
	assert.NotEqual(t, unsafe.Pointer(&commit1.Deletes), unsafe.Pointer(&clone1.Deletes))
	assert.NotEqual(t, unsafe.Pointer(&commit2.Updates), unsafe.Pointer(&clone2.Updates))
	assert.NotEqual(t, unsafe.Pointer(&commit3.Inserts), unsafe.Pointer(&clone3.Inserts))
}

func TestType(t *testing.T) {
	assert.Equal(t, "store", Type(1).String())
	assert.Equal(t, "insert", Type(2).String())
	assert.Equal(t, "store,insert", Type(3).String())
	assert.Equal(t, "delete", Type(4).String())
	assert.Equal(t, "store,delete", Type(5).String())
	assert.Equal(t, "insert,delete", Type(6).String())
	assert.Equal(t, "store,insert,delete", Type(7).String())
	assert.Equal(t, "invalid", Type(8).String())

	c := Commit{
		Type: Type(6),
	}
	assert.True(t, c.Is(Delete))
}
