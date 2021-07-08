// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/stretchr/testify/assert"
)

func TestWriterChannel(t *testing.T) {
	w := make(Channel, 1)
	w.Write(Commit{
		Type:    Delete,
		Deletes: bitmap.Bitmap{0xff},
	})

	out := <-w
	assert.Equal(t, Delete, out.Type)
	assert.Equal(t, bitmap.Bitmap{0xff}, out.Deletes)
}

func TestNegate(t *testing.T) {
	for i := -10000; i < 10000; i += 1000 {
		assert.Equal(t, negateBaseline(int32(i)), negateBranchless(int32(i)))
	}
}

func negateBranchless(v int32) uint32 {
	ux := uint32(v) << 1
	if v < 0 {
		ux = ^ux
	}
	return ux
}

func negateBaseline(v int32) uint32 {
	ux := uint32(v) << 1
	if v < 0 {
		ux = ^ux
	}
	return ux
}
