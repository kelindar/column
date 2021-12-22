// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"unsafe"

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

// Restore restores the collection from the underlying snapshot reader. This operation
// should be called before any of transactions, right after initialization.
func (c *Collection) Restore(snapshot io.ReadWriter) error {
	commits, err := c.readState(snapshot)
	if err != nil {
		return err
	}

	// Reconcile the pending commit log
	return commit.Open(snapshot).Range(func(commit commit.Commit) error {
		lastCommit := commits[commit.Chunk]
		if commit.ID > lastCommit {
			return c.Replay(commit)
		}
		return nil
	})
}

// Snapshot writes a collection snapshot into the underlying writer.
func (c *Collection) Snapshot(dst io.Writer) error {
	recorder, err := c.recorderOpen()
	if err != nil {
		return err
	}

	// Take a snapshot of the current state
	if _, err := c.writeState(dst); err != nil {
		return err
	}

	// Close the recorder in order to write the footer
	if err := recorder.Close(); err != nil {
		return err
	}

	// Reopen the log file in read-only mode
	footer, err := os.Open(recorder.Name())
	if err != nil {
		return err
	}

	// Write the commits back into the destination stream
	if _, err := io.Copy(dst, footer); err != nil {
		return err
	}

	// Close the read-only log now
	if err := footer.Close(); err != nil {
		return err
	}

	// Swap the recorder pointer
	return c.recorderClose()
}

func (c *Collection) recorderOpen() (*commit.Log, error) {
	recorder, err := commit.OpenTemp()
	if err != nil {
		return nil, err
	}

	dst := (*unsafe.Pointer)(unsafe.Pointer(&c.record))
	ptr := unsafe.Pointer(recorder)
	if !atomic.CompareAndSwapPointer(dst, nil, ptr) {
		return nil, fmt.Errorf("column: unable to snapshot, another one might be in progress")
	}
	return recorder, nil
}

func (c *Collection) recorderClose() error {
	recorder, ok := c.isSnapshotting()
	if !ok {
		return fmt.Errorf("column: unable to close snapshot, no recorder found")
	}

	dst := (*unsafe.Pointer)(unsafe.Pointer(&c.record))
	atomic.StorePointer(dst, nil)

	return os.Remove(recorder.Name())
}

// isSnapshotting loads a currently used commit log for a pending snapshot
func (c *Collection) isSnapshotting() (*commit.Log, bool) {
	dst := (*unsafe.Pointer)(unsafe.Pointer(&c.record))
	ptr := atomic.LoadPointer(dst)
	if ptr == nil {
		return nil, false
	}

	return (*commit.Log)(ptr), true
}

// --------------------------- Collection Encoding ---------------------------

// writeState writes collection state into the specified writer.
func (c *Collection) writeState(dst io.Writer) (int64, error) {

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

	// Write each chunk
	if err := writer.WriteRange(int(chunks), func(i int, w *iostream.Writer) error {
		chunk := commit.Chunk(i)
		return c.writeAtChunk(chunk, func(chunk commit.Chunk, fill bitmap.Bitmap) error {
			offset := chunk.Min()

			// Write the last written commit for this chunk
			if err := writer.WriteUvarint(c.commits[chunk]); err != nil {
				return err
			}

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
				if column.IsIndex() {
					return nil // Skip indexes
				}

				buffer.Reset(column.name)
				column.Snapshot(chunk, buffer)
				return writer.WriteSelf(buffer)
			})
		})
	}); err != nil {
		return writer.Offset(), err
	}

	return writer.Offset(), encoder.Close()
}

// readState reads a collection snapshotted state from the underlying reader. It
// returns the last commit IDs for each chunk.
func (c *Collection) readState(src io.Reader) ([]uint64, error) {
	r := iostream.NewReader(c.codec.DecoderFor(src))
	commits := make([]uint64, 128)

	// Read the version and make sure it matches
	version, err := r.ReadUvarint()
	if err != nil || version != 0x1 {
		return nil, fmt.Errorf("column: unable to restore (version %d) %v", version, err)
	}

	// Read the number of columns
	columns, err := r.ReadUvarint()
	if err != nil {
		return nil, err
	}

	// Read each chunk
	return commits, r.ReadRange(func(chunk int, r *iostream.Reader) error {
		return c.Query(func(txn *Txn) error {
			txn.dirty.Set(uint32(chunk))

			// Read the last written commit ID for the chunk
			if commits[chunk], err = r.ReadUvarint(); err != nil {
				return err
			}

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
		})
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
