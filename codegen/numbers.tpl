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
func make{{.Name}}s(opts ...func(*option[{{.Type}}])) Column {
	return makeNumeric(
		func(buffer *commit.Buffer, idx uint32, value {{.Type}}) { buffer.Put{{.Name}}(commit.Put, idx, value) },
		func(r *commit.Reader, fill bitmap.Bitmap, data []{{.Type}}, opts option[{{.Type}}]) {
			for r.Next() {
				offset := r.IndexAtChunk()
				switch r.Type {
				case commit.Put:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.{{.Name}}()
				case commit.Merge:
					fill[offset>>6] |= 1 << (offset & 0x3f)
					data[offset] = r.Swap{{.Name}}(opts.Merge(data[offset], r.{{.Name}}()))
				case commit.Delete:
					fill.Remove(offset)
				}
			}
		}, opts,
	)
}

// rw{{.Name}} represents a read-write cursor for {{.Type}}
type rw{{.Name}} struct {
	rdNumber[{{.Type}}]
	writer *commit.Buffer
}

// Set sets the value at the current transaction cursor
func (s rw{{.Name}}) Set(value {{.Type}}) {
	s.writer.Put{{.Name}}(commit.Put, s.txn.cursor, value)
}

// Merge atomically merges a delta to the value at the current transaction cursor
func (s rw{{.Name}}) Merge(delta {{.Type}}) {
	s.writer.Put{{.Name}}(commit.Merge, s.txn.cursor, delta)
}

// {{.Name}} returns a read-write accessor for {{.Type}} column
func (txn *Txn) {{.Name}}(columnName string) rw{{.Name}} {
	return rw{{.Name}}{
		rdNumber: readNumberOf[{{.Type}}](txn, columnName),
		writer:   txn.bufferFor(columnName),
	}
}

{{ end }}