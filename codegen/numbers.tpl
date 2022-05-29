// This code was generated, DO NOT EDIT.
// Any changes will be lost if this file is regenerated.

package column

import (
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
)

{{ range . }}
// --------------------------- {{.Name}} ----------------------------

// make{{.Name}}s creates a new vector for {{.Type}}s
func make{{.Name}}s() Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value {{.Type}}) {
			buffer.Put{{.Name}}(idx, value)
		},
		func(r *commit.Reader, fill bitmap.Bitmap, data []{{.Type}}) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.{{.Name}}()
				case commit.Add:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.AddTo{{.Name}}(data[offset])
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		},
	)
}

// {{.Type}}Writer represents a read-write accessor for {{.Type}}
type {{.Type}}Writer struct {
	numericReader[{{.Type}}]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s {{.Type}}Writer) Set(value {{.Type}}) {
	s.writer.Put{{.Name}}(s.txn.cursor, value)
}

// Add atomically adds a delta to the value at the current transaction cursor
func (s {{.Type}}Writer) Add(delta {{.Type}}) {
	s.writer.Add{{.Name}}(s.txn.cursor, delta)
}

// {{.Name}} returns a read-write accessor for {{.Type}} column
func (txn *Txn) {{.Name}}(columnName string) {{.Type}}Writer {
	return {{.Type}}Writer{
		numericReader: numericReaderFor[{{.Type}}](txn, columnName),
		writer:        txn.bufferFor(columnName),
	}
}

{{ end }}