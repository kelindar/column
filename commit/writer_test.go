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
		Dirty: bitmap.Bitmap{0xff},
	})

	out := <-w
	assert.Equal(t, bitmap.Bitmap{0xff}, out.Dirty)
}
