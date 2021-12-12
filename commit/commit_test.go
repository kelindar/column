// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"io"
	"sync/atomic"
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

func TestWriterChannel(t *testing.T) {
	w := make(Channel, 1)
	w.Write(Commit{
		Chunk: 123,
	})

	out := <-w
	assert.Equal(t, 123, int(out.Chunk))
}

type limitWriter struct {
	value uint32
	Limit int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if n := atomic.AddUint32(&w.value, uint32(len(p))); int(n) > w.Limit {
		return 0, io.ErrShortBuffer
	}
	return len(p), nil
}
