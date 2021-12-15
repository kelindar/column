// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkColumn/chunkOf-8         	 8466814	       136.2 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkColumn(b *testing.B) {
	b.Run("chunkOf", func(b *testing.B) {
		var temp bitmap.Bitmap
		temp.Grow(2 * chunkSize)

		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for i := 0; i < 100; i++ {
				Chunk(1).OfBitmap(temp)
			}
		}
	})
}

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

func TestChunkMinMax(t *testing.T) {
	tests := []struct {
		chunk    Chunk
		min, max uint32
	}{
		{chunk: 0, min: 0, max: chunkSize - 1},
		{chunk: 1, min: chunkSize, max: 2*chunkSize - 1},
		{chunk: 2, min: 2 * chunkSize, max: 3*chunkSize - 1},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.min, tc.chunk.Min())
		assert.Equal(t, tc.max, tc.chunk.Max())
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

func TestChunkOf(t *testing.T) {
	tests := []struct {
		size   uint32
		chunk  Chunk
		expect int
	}{
		{size: 3 * chunkSize, expect: chunkSize, chunk: 0},
		{size: 3 * chunkSize, expect: chunkSize, chunk: 1},
		{size: 3 * chunkSize, expect: chunkSize, chunk: 2},
		{size: 3 * chunkSize, expect: 0, chunk: 3},
		{size: 2*chunkSize - 70, expect: chunkSize, chunk: 0},
		{size: 2*chunkSize - 70, expect: 16320, chunk: 1},
		{size: 2*chunkSize - 70, expect: 0, chunk: 2},
		{size: 2*chunkSize - 10, expect: chunkSize, chunk: 0},
		{size: 2*chunkSize - 10, expect: chunkSize, chunk: 1},
		{size: 2*chunkSize - 10, expect: 0, chunk: 2},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v-%v", tc.chunk, tc.size), func(t *testing.T) {
			var tmp bitmap.Bitmap
			tmp.Grow(tc.size - 1)
			assert.Equal(t, tc.expect, len(tc.chunk.OfBitmap(tmp))*64)
		})
	}
}

func TestMin(t *testing.T) {
	tests := []struct {
		v1, v2 int32
		expect int32
	}{
		{v1: 0, v2: 0, expect: 0},
		{v1: 10, v2: 0, expect: 0},
		{v1: 0, v2: 10, expect: 0},
		{v1: 10, v2: 20, expect: 10},
		{v1: 20, v2: 10, expect: 10},
		{v1: 20, v2: 20, expect: 20},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v,%v", tc.v1, tc.v2), func(t *testing.T) {
			assert.Equal(t, int(tc.expect), int(min(tc.v1, tc.v2)))
		})
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
