// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"sync"

	"github.com/kelindar/bitmap"
)

// Bitmaps represents a pool of bitmaps
var bitmaps = &sync.Pool{
	New: func() interface{} {
		return &bitmap.Bitmap{}
	},
}

func aquireBitmap() *bitmap.Bitmap {
	return bitmaps.Get().(*bitmap.Bitmap)
}

func releaseBitmap(b *bitmap.Bitmap) {
	bitmaps.Put(b)
}

// --------------------------- Query ----------------------------

// Query represents a query for a collection
type Query struct {
	owner *Collection
	index *bitmap.Bitmap
}

// With applies a logical AND operation to the current query and the specified index.
func (q Query) With(index string, extra ...string) Query {
	if idx, ok := q.owner.props[index]; ok {
		q.index.And(idx.Bitmap())
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := q.owner.props[e]; ok {
			q.index.And(idx.Bitmap())
		}
	}
	return q
}

// Without applies a logical AND NOT operation to the current query and the specified index.
func (q Query) Without(index string, extra ...string) Query {
	if idx, ok := q.owner.props[index]; ok {
		q.index.AndNot(idx.Bitmap())
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := q.owner.props[e]; ok {
			q.index.AndNot(idx.Bitmap())
		}
	}
	return q
}

// Union computes a union between the current query and the specified index.
func (q Query) Union(index string, extra ...string) Query {
	if idx, ok := q.owner.props[index]; ok {
		q.index.Or(idx.Bitmap())
	}

	// go through extra indexes
	for _, e := range extra {
		if idx, ok := q.owner.props[e]; ok {
			q.index.Or(idx.Bitmap())
		}
	}
	return q
}

// WithValue applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (q Query) WithValue(property string, predicate func(v interface{}) bool) Query {
	if p, ok := q.owner.props[property]; ok {
		q.index.Filter(func(x uint32) bool {
			if v, ok := p.Value(x); ok {
				return predicate(v)
			}
			return false
		})
	}
	return q
}

// WithFloat64 filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to float64.
func (q Query) WithFloat64(property string, predicate func(v float64) bool) Query {
	if p, ok := q.owner.props[property]; ok {
		if n, ok := p.(numerical); ok {
			q.index.Filter(func(x uint32) bool {
				if v, ok := n.Float64(x); ok {
					return predicate(v)
				}
				return false
			})
		}
	}
	return q
}

// WithInt64 filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to int64.
func (q Query) WithInt64(property string, predicate func(v int64) bool) Query {
	if p, ok := q.owner.props[property]; ok {
		if n, ok := p.(numerical); ok {
			q.index.Filter(func(x uint32) bool {
				if v, ok := n.Int64(x); ok {
					return predicate(v)
				}
				return false
			})
		}
	}
	return q
}

// WithUint64 filters down the values based on the specified predicate. The column for
// this filter must be numerical and convertible to uint64.
func (q Query) WithUint64(property string, predicate func(v uint64) bool) Query {
	if p, ok := q.owner.props[property]; ok {
		if n, ok := p.(numerical); ok {
			q.index.Filter(func(x uint32) bool {
				if v, ok := n.Uint64(x); ok {
					return predicate(v)
				}
				return false
			})
		}
	}
	return q
}

// WithString filters down the values based on the specified predicate. The column for
// this filter must be a string.
func (q Query) WithString(property string, predicate func(v string) bool) Query {
	return q.WithValue(property, func(v interface{}) bool {
		return predicate(v.(string))
	})
}

// count returns the number of objects matching the query
func (q *Query) count() int {
	return int(q.index.Count())
}

// iterate iterates over the objects with the given properties, but does not perform any
// locking.
func (q Query) iterate(fn func(Selector) bool) {
	q.index.Range(func(x uint32) bool {
		return fn(Selector{
			index: x,
			owner: q.owner,
		})
	})
}

// --------------------------- Selector ----------------------------

// Selector represents a column selector which can retrieve a value by a column name.
type Selector struct {
	index uint32
	owner *Collection
}

// Value reads a value for a current row at a given column.
func (s *Selector) Value(column string) interface{} {
	if c, ok := s.owner.props[column]; ok {
		v, _ := c.Value(s.index)
		return v
	}
	return nil
}

// String reads a string value for a current row at a given column.
func (s *Selector) String(column string) string {
	if c, ok := s.owner.props[column]; ok {
		if v, ok := c.Value(s.index); ok {
			return v.(string)
		}
	}
	return ""
}

// Float64 reads a float64 value for a current row at a given column.
func (s *Selector) Float64(column string) float64 {
	if c, ok := s.owner.props[column]; ok {
		if n, ok := c.(numerical); ok {
			v, _ := n.Float64(s.index)
			return v
		}
	}
	return 0
}

// Int64 reads an int64 value for a current row at a given column.
func (s *Selector) Int64(column string) int64 {
	if c, ok := s.owner.props[column]; ok {
		if n, ok := c.(numerical); ok {
			v, _ := n.Int64(s.index)
			return v
		}
	}
	return 0
}

// Uint64 reads a uint64 value for a current row at a given column.
func (s *Selector) Uint64(column string) uint64 {
	if c, ok := s.owner.props[column]; ok {
		if n, ok := c.(numerical); ok {
			v, _ := n.Uint64(s.index)
			return v
		}
	}
	return 0
}

// Bool reads a boolean value for a current row at a given column.
func (s *Selector) Bool(column string) bool {
	if c, ok := s.owner.props[column]; ok {
		return c.Contains(s.index)
	}
	return false
}
