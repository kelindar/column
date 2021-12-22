// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

import (
	"io"
	"os"
	"sync"

	"github.com/kelindar/iostream"
	"github.com/klauspost/compress/s2"
)

// Logger represents a contract that a commit logger must implement
type Logger interface {
	Append(commit Commit) error
}

var _ Logger = new(Channel)
var _ Logger = new(Log)

// --------------------------- Channel ----------------------------

// Channel represents an impementation of a commit writer that simply sends each commit
// into the channel.
type Channel chan Commit

// Append clones the commit and writes it into the logger
func (w *Channel) Append(commit Commit) error {
	*w <- commit.Clone()
	return nil
}

// --------------------------- Log ----------------------------

// Log represents a commit log that can be used to write the changes to the collection
// during a snapshot. It also supports reading a commit log back.
type Log struct {
	lock   sync.Mutex
	source io.ReadWriter
	writer *iostream.Writer
	reader *iostream.Reader
}

// Open opens a commit log stream for both read and write.
func Open(source io.ReadWriter) *Log {
	return &Log{
		source: source,
		writer: iostream.NewWriter(s2.NewWriter(source)),
		reader: iostream.NewReader(s2.NewReader(source)),
	}
}

// OpenFile opens a specified commit log file in a read/write mode. If
// the file does not exist, it will create it.
func OpenFile(filename string) (*Log, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return Open(file), nil
}

// Append writes the commit into the log destination
func (l *Log) Append(commit Commit) (err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	// Write the commit into the stream
	if _, err = commit.WriteTo(l.writer); err == nil {
		err = l.writer.Flush()
	}
	return
}

// Range iterates over all the commits in the log and calls the provided
// callback function on each of them. If the callback returns an error, the
// iteration will stop.
func (l *Log) Range(fn func(Commit) error) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	for {
		var commit Commit
		_, err := commit.ReadFrom(l.reader)
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		}

		// Read the commit
		if err := fn(commit); err != nil {
			return err
		}
	}
}

// Close closes the source log file
func (l *Log) Close() (err error) {
	if closer, ok := l.source.(io.Closer); ok {
		err = closer.Close()
	}
	return
}
