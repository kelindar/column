package columnar

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

// Query represents a query for a collection
type Query struct {
	owner *Collection
	index *bitmap.Bitmap
}

// Value applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (q Query) Value(property string, predicate func(v interface{}) bool) Query {
	if p, ok := q.owner.props[property]; ok {
		q.index.Filter(func(x uint32) bool {
			if v, ok := p.Get(x); ok {
				return predicate(v)
			}
			return false
		})
	}
	return q
}

// String ...
func (q Query) String(property string, value string) Query {
	if p, ok := q.owner.props[property]; ok {
		q.index.Filter(func(x uint32) bool {
			if v, ok := p.Get(x); ok {
				return v == value
			}
			return false
		})
	}
	return q
}

// count returns the number of objects matching the query
func (q *Query) count() int {
	return int(q.index.Count())
}

// iterate iterates over the objects with the given properties, but does not perform any
// locking.
func (q Query) iterate(fn func(Object) bool, props []string) {
	obj := make(Object, len(props))

	// Range over the entries in the index, since we're selecting row by row
	q.index.Range(func(x uint32) bool {
		for _, name := range props {
			if prop, ok := q.owner.props[name]; ok {
				if v, ok := prop.Get(x); ok {
					obj[name] = v
					fn(obj)
				}
			}
		}
		return true
	})
}
