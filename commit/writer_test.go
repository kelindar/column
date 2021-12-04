// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
