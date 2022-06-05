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
func (w Channel) Append(commit Commit) error {
	w <- commit.Clone()
	return nil
}

// --------------------------- Log ----------------------------

// Log represents a commit log that can be used to write the changes to the collection
// during a snapshot. It also supports reading a commit log back.
type Log struct {
	lock   sync.Mutex
	source io.Reader
	writer *iostream.Writer
	reader *iostream.Reader
}

// Open opens a commit log stream for both read and write.
func Open(source io.Reader) *Log {
	log := &Log{
		source: source,
		reader: iostream.NewReader(s2.NewReader(source)),
	}

	if rw, ok := source.(io.Writer); ok {
		log.writer = iostream.NewWriter(s2.NewWriter(rw))
	}
	return log
}

// OpenFile opens a specified commit log file in a read/write mode. If
// the file does not exist, it will create it.
func OpenFile(filename string) (*Log, error) {
	return openFile(os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm))
}

// OpenTemp opens a temporary commit log file with read/write permissions
func OpenTemp() (*Log, error) {
	return openFile(os.CreateTemp("", "column_*.log"))
}

// openFile opens a file or returns the error provided
func openFile(file *os.File, err error) (*Log, error) {
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

// Name calls the corresponding Name() method on the underlying source
func (l *Log) Name() (name string) {
	if file, ok := l.source.(interface {
		Name() string
	}); ok {
		name = file.Name()
	}
	return
}

// Copy copies the contents of the log into the destination writer.
func (l *Log) Copy(dst io.Writer) error {
	l.lock.Lock()
	defer l.lock.Unlock()

	// Rewind to the beginning of the file, the underlying source must
	// implement io.Seeker for this to work.
	if seeker, ok := l.source.(io.Seeker); ok {
		if _, err := seeker.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}

	// Append the pending commits to the destination
	_, err := io.Copy(dst, l.source)
	return err
}

// Close closes the source log file.
func (l *Log) Close() (err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if closer, ok := l.source.(io.Closer); ok {
		err = closer.Close()
	}
	return
}
