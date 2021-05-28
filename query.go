package columnar

import (
	"github.com/kelindar/bitmap"
)

// Bitmaps represents a pool of bitmaps
/*var bitmaps = &sync.Pool{
	New: func() interface{} {
		return roaring.NewBitmap()
	},
}

func aquireBitmap() *roaring.Bitmap {
	return bitmaps.Get().(*roaring.Bitmap)
}

func releaseBitmap(b *roaring.Bitmap) {
	bitmaps.Put(b)
}*/

// Query represents a query for a collection
type Query struct {
	owner *Collection
	index bitmap.Bitmap
}

// Count returns the number of objects matching the query
func (q *Query) Count() int {
	return int(q.index.Count())
}

// And applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (q *Query) And(property string, predicate func(v interface{}) bool) *Query {
	q.owner.lock.RLock()
	defer q.owner.lock.RUnlock()

	// Range over the values of the property and apply a filter
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

// AndValue filters down the result set where the value of a property matches the
// specified value. This in turn, calls normal filter.
func (q *Query) AndValue(property string, value interface{}) *Query {
	return q.And(property, func(v interface{}) bool {
		return v == value
	})
}
