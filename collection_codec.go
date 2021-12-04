// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

// --------------------------- Commit Replay ---------------------------

// Replay replays a commit on a collection, applying the changes.
func (c *Collection) Replay(change commit.Commit) error {
	return c.Query(func(txn *Txn) error {
		txn.dirty.Set(change.Chunk)
		for i := range change.Updates {
			if !change.Updates[i].IsEmpty() {
				txn.updates = append(txn.updates, change.Updates[i])
			}
		}
		return nil
	})
}

// --------------------------- Snapshotting ---------------------------

// WriteTo writes collection encoded into binary format into the destination writer until
// there's no more data to write or when an error occurs. The return value n is the number
// of bytes written. Any error encountered during the write is also returned.
func (c *Collection) WriteTo(w io.Writer) (int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Check if there's any data to write
	if c.fill.Count() == 0 {
		return 0, fmt.Errorf("column: unable to write an empty collection")
	}

	// Write the fill bitmap
	n, err := c.fill.WriteTo(w)
	if err != nil {
		return 0, err
	}

	// Snapshot each column and write the buffer
	tmp := commit.NewBuffer(8192)
	err = c.cols.Range(func(column *column) error {
		m, err := column.WriteTo(w, tmp)
		if err != nil {
			return err
		}

		n += m
		return nil
	})
	return n, err
}

// ReadFrom reads a collection from the provided reader source until EOF or error. The
// return value n is the number of bytes read. Any error except EOF encountered during
// the read is also returned.
func (c *Collection) ReadFrom(r io.Reader) (int64, error) {
	fill, err := bitmap.ReadFrom(r)
	if err != nil {
		return 0, err
	}

	c.lock.Lock()
	c.fill = fill
	atomic.StoreUint64(&c.count, uint64(c.fill.Count()))
	c.lock.Unlock()

	max, _ := c.fill.Max()
	tmp := commit.NewBuffer(8192)
	var n int64
	for {
		m, err := tmp.ReadFrom(r)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}

		n += m
		c.Query(func(txn *Txn) error {
			txn.commitCapacity(max)
			txn.updates = append(txn.updates, tmp)
			tmp.RangeChunks(func(chunk uint32) {
				txn.dirty.Set(chunk)
			})
			return nil
		})
	}
}
