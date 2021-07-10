// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	buf := NewBuffer(0)
	buf.Reset("test")
	for i := uint32(0); i < 10; i++ {
		buf.PutUint64(Put, i, 2*uint64(i))
	}

	i := 0
	assert.Equal(t, 91, len(buf.buffer))

	r := NewReader()
	for r.Seek(buf); r.Next(); {
		assert.Equal(t, Put, r.Type)
		assert.Equal(t, i, int(r.Offset))
		assert.Equal(t, int(i*2), int(r.AsUint64()))
		i++
	}
}

func TestRandom(t *testing.T) {
	seq := make([]uint32, 1024)
	for i := 0; i < len(seq); i++ {
		seq[i] = uint32(rand.Int31n(10000000))
	}

	buf := NewBuffer(0)
	for i := uint32(0); i < 1000; i++ {
		buf.PutUint32(Put, seq[i], uint32(rand.Int31()))
	}

	i := 0
	r := NewReader()
	for r.Seek(buf); r.Next(); {
		assert.Equal(t, Put, r.Type)
		assert.Equal(t, int(seq[i]), int(r.Offset))
		i++
	}
}

func TestRange(t *testing.T) {
	const count = 10000

	seq := make([]uint32, count)
	for i := 0; i < len(seq); i++ {
		seq[i] = uint32(rand.Int31n(1000000))
	}

	buf := NewBuffer(0)
	for i := uint32(0); i < count; i++ {
		buf.PutUint32(Put, seq[i], uint32(rand.Int31()))
	}

	r := NewReader()
	for i := 0; i < 100; i++ {
		r.Range(buf, uint32(i), func(r *Reader) {
			for r.Next() {
				assert.Equal(t, Put, r.Type)
				assert.Equal(t, i, int(r.Offset>>chunkShift))
			}
		})
	}
}
