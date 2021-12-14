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

func TestChunkOffset(t *testing.T) {
	tests := []struct {
		chunk  Chunk
		offset uint32
	}{
		{chunk: 0, offset: 0},
		{chunk: 1, offset: chunkSize},
		{chunk: 2, offset: 2 * chunkSize},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.offset, tc.chunk.Offset())
	}
}

func TestChunkAt(t *testing.T) {
	tests := []struct {
		index uint32
		chunk Chunk
	}{
		{index: 0, chunk: 0},
		{index: chunkSize - 1, chunk: 0},
		{index: chunkSize, chunk: 1},
		{index: chunkSize + 1, chunk: 1},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.chunk, ChunkAt(tc.index))
	}
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
