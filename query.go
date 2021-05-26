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

// Query represents a query for a collection
type Query struct {
	owner *Collection
	index *roaring.Bitmap
	props []string
}

// Count returns the number of objects matching the query
func (q *Query) Count() int {
	return int(q.index.GetCardinality())
}

// Select selects the specific properties from a query
func (q *Query) Select(props ...string) *Query {
	q.props = props
	return q
}

// WithMany does a logical AND between the current query and the specified
// properties, combining them together.
func (q *Query) WithMany(props []string) *Query {
	for _, extra := range props {
		if p, ok := q.owner.props[extra]; ok {
			q.index.And(&p.fill)
		}
	}
	return q
}

// Where applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (q *Query) Where(predicate func(v interface{}) bool, property string) *Query {
	filter := bitmaps.Get().(*roaring.Bitmap)
	defer bitmaps.Put(filter)
	defer filter.Clear()

	// Range over the values of the property and apply a filter
	q.owner.rangeProperty(func(id uint32, v interface{}) bool {
		if predicate(v) {
			filter.Add(id)
		}
		return true
	}, property)

	// Update the current index
	q.index.And(filter)
	return q
}

// Range iterates through the results, calling the given callback with each
// value. If the callback returns false, the iteration is halted.
func (q *Query) Range(f func(*Object) bool) {
	obj := make(Object, len(q.owner.props))
	q.index.Iterate(func(x uint32) bool {
		if q.owner.FetchTo(uint32(x), &obj) {
			return f(&obj)
		}
		return true
	})
}
