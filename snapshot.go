// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"fmt"
	"io"

	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
	"github.com/kelindar/iostream"
	"github.com/klauspost/compress/s2"
)

var (
	errUnexpectedEOF = errors.New("column: unable to restore, unexpected EOF")
)

// --------------------------- Commit Replay ---------------------------

// Replay replays a commit on a collection, applying the changes.
func (c *Collection) Replay(change commit.Commit) error {
	return c.Query(func(txn *Txn) error {
		txn.dirty.Set(uint32(change.Chunk))
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
func (c *Collection) WriteTo(dst io.Writer) (int64, error) {

	// Create a writer, encoder and a reusable buffer
	encoder := c.codec.EncoderFor(dst)
	writer := iostream.NewWriter(c.codec.EncoderFor(dst))
	buffer := c.txns.acquirePage(rowColumn)
	defer c.txns.releasePage(buffer)

	// Write the schema version
	if err := writer.WriteUvarint(0x1); err != nil {
		return writer.Offset(), err
	}

	// Load the number of columns and the max index
	c.lock.Lock()
	max, _ := c.fill.Max()
	chunks := commit.ChunkAt(max) + 1
	columns := uint64(c.cols.Count()) + 1 // extra 'insert' column
	c.lock.Unlock()

	// Write the number of columns
	if err := writer.WriteUvarint(columns); err != nil {
		return writer.Offset(), err
	}

	// Write the number of chunks
	if err := writer.WriteUvarint(uint64(chunks)); err != nil {
		return writer.Offset(), err
	}

	// Write all chunks, one at a time
	for i := commit.Chunk(0); i < chunks; i++ {
		offset := i.Min()
		if err := c.writeAtChunk(i, func(chunk commit.Chunk, fill bitmap.Bitmap) error {

			// Write the inserts column
			buffer.Reset(rowColumn)
			fill.Range(func(idx uint32) {
				buffer.PutOperation(commit.Insert, offset+idx)
			})
			if err := writer.WriteSelf(buffer); err != nil {
				return err
			}

			// Snapshot each column and write the buffer
			return c.cols.RangeUntil(func(column *column) error {
				buffer.Reset(column.name)
				column.Snapshot(chunk, buffer)
				return writer.WriteSelf(buffer)
			})
		}); err != nil {
			return writer.Offset(), err
		}
	}

	return writer.Offset(), encoder.Close()
}

// ReadFrom reads a collection from the provided reader source until EOF or error. The
// return value n is the number of bytes read. Any error except EOF encountered during
// the read is also returned.
func (c *Collection) ReadFrom(src io.Reader) (int64, error) {
	r := iostream.NewReader(c.codec.DecoderFor(src))

	// Read the version and make sure it matches
	version, err := r.ReadUvarint()
	if err != nil || version != 0x1 {
		return r.Offset(), fmt.Errorf("column: unable to restore (version %d) %v", version, err)
	}

	// Read the number of columns
	columns, err := r.ReadUvarint()
	if err != nil {
		return r.Offset(), err
	}

	// Read the number of chunks
	chunks, err := r.ReadUvarint()
	if err != nil {
		return r.Offset(), err
	}

	// Read each chunk/column
	for chunk := 0; chunk < int(chunks); chunk++ {
		if err := c.Query(func(txn *Txn) error {
			txn.dirty.Set(uint32(chunk))
			for i := uint64(0); i < columns; i++ {
				buffer := txn.owner.txns.acquirePage("")
				_, err := buffer.ReadFrom(r)
				switch {
				case err == io.EOF && i < columns:
					return errUnexpectedEOF
				case err != nil:
					return err
				default:
					txn.updates = append(txn.updates, buffer)
				}
			}

			return nil
		}); err != nil {
			return r.Offset(), err
		}
	}

	return r.Offset(), nil
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
