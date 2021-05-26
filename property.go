package columnar

import (
	"github.com/RoaringBitmap/roaring"
)

// Property represents a generic column
type Property struct {
	fill roaring.Bitmap // The index of filled entries
	free roaring.Bitmap // The index of free (reclaimable) entries
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

// Set sets a value at a specified index
func (p *Property) Set(id uint32, value interface{}) {
	if p.free.IsEmpty() {
		p.data = append(p.data, keyValuePair{
			key: id,
			val: value,
		})
		p.fill.Add(id)
		return
	}

	// Replace an existing one
	idx := p.free.Minimum()
	p.free.Remove(idx)
	p.fill.Add(id)
	p.data[idx] = keyValuePair{
		key: id,
		val: value,
	}
	return
}

// Get retrieves a value at a specified index
func (p *Property) Get(id uint32) (interface{}, bool) {

	// This only works because the "id" is controlled by the Collection, and same id cannot
	// be replaced (ie: monotonically increasing), hence our data is ALWAYS sorted.
	if idx := p.fill.Rank(id); idx > 0 {
		return p.data[idx-1].val, true
	}
	return nil, false
}

// Remove removes a value at a specified index
func (p *Property) Remove(id uint32) {
	if !p.fill.Contains(id) {
		return
	}

	idx := uint32(p.fill.Rank(id) - 1)
	p.free.Add(idx)
	p.fill.Remove(id)
	p.data[idx].val = nil
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
