// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kelindar/column/commit"
	"github.com/klauspost/compress/s2"
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

	// Write the number of columns
	encoder := c.codec.EncoderFor(w)
	if _, err := writeUintTo(encoder, c.cols.Count()+1); err != nil {
		encoder.Close()
		return 0, err
	}

	// Acquire a buffer which is re-used for each column
	buffer := c.txns.acquirePage(rowColumn)
	defer c.txns.releasePage(buffer)

	// Write the inserts column
	buffer.PutBitmap(commit.Insert, c.fill)
	n, err := buffer.WriteTo(encoder)
	if err != nil {
		encoder.Close()
		return 0, err
	}

	// Snapshot each column and write the buffer
	if err := c.cols.Range(func(column *column) error {
		buffer.Reset(column.name)
		m, err := column.WriteTo(encoder, buffer)
		if err != nil {
			return err
		}

		n += m
		return nil
	}); err != nil {
		encoder.Close()
		return n, err
	}

	return n + 4, encoder.Close()
}

// ReadFrom reads a collection from the provided reader source until EOF or error. The
// return value n is the number of bytes read. Any error except EOF encountered during
// the read is also returned.
func (c *Collection) ReadFrom(r io.Reader) (int64, error) {
	r = c.codec.DecoderFor(r)
	count, n, err := readUintFrom(r)
	if err != nil {
		return n, err
	}

	return n, c.Query(func(txn *Txn) error {
		for i := 0; i < count; i++ {
			buffer := txn.owner.txns.acquirePage("")
			m, err := buffer.ReadFrom(r)
			switch {
			case err == io.EOF && i < count:
				return fmt.Errorf("unexpected EOF")
			case err != nil:
				return err
			default:
				txn.updates = append(txn.updates, buffer)
				buffer.RangeChunks(func(chunk uint32) {
					txn.dirty.Set(chunk)
				})
				n += m
			}
		}
		return nil
	})
}

// --------------------------- Compression Codec ----------------------------

// newCodec creates a new compressor for the destination writer
func newCodec(options *Options) codec {
	return &s2codec{
		w: s2.NewWriter(nil),
		r: s2.NewReader(nil),
	}
}

type codec interface {
	DecoderFor(reader io.Reader) io.Reader
	EncoderFor(writer io.Writer) io.WriteCloser
}

type s2codec struct {
	w *s2.Writer
	r *s2.Reader
}

func (c *s2codec) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *s2codec) Write(p []byte) (int, error) {
	return c.w.Write(p)
}

func (c *s2codec) DecoderFor(reader io.Reader) io.Reader {
	c.r.Reset(reader)
	return c
}

func (c *s2codec) EncoderFor(writer io.Writer) io.WriteCloser {
	c.w.Reset(writer)
	return c
}

func (c *s2codec) Close() error {
	return c.w.Close()
}

// --------------------------- Read/Write ----------------------------

// writeUintTo writes the length of something into the destination writer
func writeUintTo(w io.Writer, v int) (n int, err error) {
	var temp [4]byte
	binary.BigEndian.PutUint32(temp[:], uint32(v))
	return w.Write(temp[:])
}

// readUintFrom reads the unsigned integer from the reader
func readUintFrom(r io.Reader) (int, int64, error) {
	var temp [4]byte
	n, err := io.ReadFull(r, temp[:])
	v := int(binary.BigEndian.Uint32(temp[:]))
	return v, int64(n), err
}
