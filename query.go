package columnar

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Bitmaps represents a pool of bitmaps
var bitmaps = &sync.Pool{
	New: func() interface{} {
		return roaring.NewBitmap()
	},
}

func aquireBitmap() *roaring.Bitmap {
	return bitmaps.Get().(*roaring.Bitmap)
}

func releaseBitmap(b *roaring.Bitmap) {
	b.Clear()
	bitmaps.Put(b)
}

// Query represents a query for a collection
type Query struct {
	owner *Collection
	index *roaring.Bitmap
}

// Count returns the number of objects matching the query
func (q Query) Count() int {
	if q.index == nil {
		panic("query has completed")
	}

	count := int(q.index.GetCardinality())
	releaseBitmap(q.index)
	q.index = nil
	return count
}

// Where applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (q Query) Where(property string, predicate func(v interface{}) bool) Query {
	q.owner.lock.RLock()
	defer q.owner.lock.RUnlock()

	// Range over the values of the property and apply a filter
	if p, ok := q.owner.props[property]; ok {
		p.Filter(q.index, predicate)
	}
	return q
}

// Range iterates through the results, calling the given callback with each
// value. If the callback returns false, the iteration is halted.
func (q Query) Range(f func(*Object) bool) {
	obj := make(Object, len(q.owner.props))
	q.index.Iterate(func(x uint32) bool {
		if q.owner.FetchTo(uint32(x), &obj) {
			return f(&obj)
		}
		return true
	})
}
