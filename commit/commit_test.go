// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommitClone(t *testing.T) {
	commit := Commit{
		Updates: []*Buffer{{
			buffer: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
			chunks: []header{{
				Chunk: 0,
			}},
		}},
	}

	clone := commit.Clone()
	assert.EqualValues(t, commit, clone)
}
