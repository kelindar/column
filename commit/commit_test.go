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
	commit1 := ForDelete(bitmap.Bitmap{0xff})
	commit2 := ForStore("test", []Update{
		{Type: Put, Index: 5, Value: "hi"},
	})

	// Assert types
	assert.Equal(t, TypeDelete, commit1.Type)
	assert.Equal(t, TypeStore, commit2.Type)

	// Clone and assert
	clone1 := commit1.Clone()
	clone2 := commit2.Clone()
	assert.Equal(t, commit1, clone1, "clone1")
	assert.Equal(t, commit2, clone2, "clone2")
	assert.NotEqual(t, unsafe.Pointer(commit1), unsafe.Pointer(clone1))
	assert.NotEqual(t, unsafe.Pointer(commit2), unsafe.Pointer(clone2))
}
