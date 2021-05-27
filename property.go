package columnar

import (
	"github.com/RoaringBitmap/roaring"
)

// Property represents a generic column
type Property struct {
	free freelist       // The fill/free-list
	data []keyValuePair // The actual values
}

// keyValuePair represents a key/value pair
type keyValuePair struct {
	key uint32
	val interface{}
}

// NewProperty creates a new property column
func NewProperty() *Property {
	return &Property{
		data: make([]keyValuePair, 0, 64),
	}
}

// Index returns the bitmap index for the entries in this property. This
// bitmap contains the IDs which are present.
func (p *Property) Index() *roaring.Bitmap {
	return &p.free.fill
}

// Set sets a value at a specified index
func (p *Property) Set(id uint32, value interface{}) {
	idx, replace := p.free.Add(id)
	if replace {
		p.data[idx] = keyValuePair{
			key: id,
			val: value,
		}
		return
	}

	// Append the new element
	p.data = append(p.data, keyValuePair{
		key: id,
		val: value,
	})
}

// Get retrieves a value at a specified index
func (p *Property) Get(id uint32) (interface{}, bool) {
	if idx, ok := p.free.Get(id); ok {
		return p.data[idx].val, true
	}
	return nil, false
}

// Remove removes a value at a specified index
func (p *Property) Remove(id uint32) {
	if idx, ok := p.free.Remove(id); ok {
		p.data[idx].val = nil
	}
}

// Range iterates over the property values. If the callback returns
// false, the iteration is halted.
func (p *Property) Range(f func(uint32, interface{}) bool) {
	for _, v := range p.data {
		if !f(v.key, v.val) {
			return
		}
	}
}

// ------------------------------------------------------------------------------------

// freelist represents a bitmap-backed free list
type freelist struct {
	fill roaring.Bitmap // The index of filled entries
	free roaring.Bitmap // The index of free (reclaimable) entries
}

// Get retrieves a location where the value for the specified ID is located.
func (l *freelist) Get(id uint32) (int, bool) {

	// This only works because the "id" is controlled by the Collection, and same id cannot
	// be replaced (ie: monotonically increasing), hence our data is ALWAYS sorted.
	if idx := l.fill.Rank(id); idx > 0 {
		return int(idx - 1), true
	}
	return 0, false
}

// Add adds the id into the free list and returns a location at which the
// element should be stored and whether the value needs to be replace or not.
func (l *freelist) Add(id uint32) (int, bool) {
	if l.free.IsEmpty() {
		idx := l.fill.GetCardinality()
		l.fill.Add(id)
		return int(idx), false
	}

	// Replace an existing one
	idx := l.free.Minimum()
	l.free.Remove(idx)
	l.fill.Add(id)
	return int(idx), true
}

// Remove removes an item from a free list and returns an index at which the data
// should be removed from the underlying slice.
func (l *freelist) Remove(id uint32) (int, bool) {
	if idx := l.fill.Rank(id); idx > 0 {
		l.free.Add(uint32(idx - 1))
		l.fill.Remove(id)
		return int(idx - 1), true
	}

	return 0, false
}
