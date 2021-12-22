// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// --------------------------- Commit Encoding ----------------------------

func TestCommitWriteToFailures(t *testing.T) {
	for size := 0; size < 30; size++ {
		output := &limitWriter{Limit: size}
		commit := newCommit(1)
		_, err := commit.WriteTo(output)
		assert.Error(t, err)
	}
}

func TestCommitReadFromFailures(t *testing.T) {
	commit := newCommit(1)
	buffer := bytes.NewBuffer(nil)
	n, err := commit.WriteTo(buffer)
	assert.NoError(t, err)

	for size := 0; size < int(n)-1; size++ {
		output := new(Commit)
		_, err := output.ReadFrom(bytes.NewReader(buffer.Bytes()[:size]))
		assert.Error(t, err)
	}
}

func newCommit(id int) Commit {
	return Commit{
		ID:    uint64(id),
		Chunk: 0,
		Updates: []*Buffer{
			newInterleaved("a"),
			newInterleaved("b"),
		},
	}
}

// --------------------------- Log Operations ----------------------------

func TestLogAppendRange(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	logger := Open(buffer)

	assert.NoError(t, logger.Append(newCommit(1)))
	assert.NoError(t, logger.Append(newCommit(2)))

	var arr []uint64
	assert.NoError(t, logger.Range(func(commit Commit) error {
		arr = append(arr, commit.ID)
		return nil
	}))

	assert.Equal(t, []uint64{1, 2}, arr)
}

func TestLogRangeFailures(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	logger := Open(buffer)
	assert.NoError(t, logger.Append(newCommit(1)))
	assert.NoError(t, logger.Append(newCommit(2)))

	n := buffer.Len()
	for size := 0; size < int(n)-1; size++ {
		tmp := bytes.NewBuffer(buffer.Bytes()[:size])
		out := Open(tmp)

		count := 0
		out.Range(func(commit Commit) error {
			count++
			return nil
		})
		assert.Less(t, count, 2, fmt.Sprintf("size=%v", size))
	}
}

func TestLogRangeStopOnError(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	logger := Open(buffer)
	assert.NoError(t, logger.Append(newCommit(1)))
	assert.NoError(t, logger.Append(newCommit(2)))

	assert.Error(t, logger.Range(func(commit Commit) error {
		return io.ErrClosedPipe
	}))
}

func TestLogOpenFile(t *testing.T) {
	name := "commit.log"
	logger, err := OpenFile(name)
	defer os.Remove(name)
	defer logger.Close()

	assert.NoError(t, err)
	assert.NotNil(t, logger)
}

func TestLogOpenFileInvalid(t *testing.T) {
	logger, err := OpenFile("")
	assert.Error(t, err)
	assert.Nil(t, logger)
}
