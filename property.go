package columnar

import (
	"github.com/kelindar/bitmap"
)

// Property represents a generic column
type Property struct {
	free bitmap.Bitmap // The free-list
	data []interface{} // The actual values
}

// NewProperty creates a new property column
func NewProperty() *Property {
	return &Property{
		free: make(bitmap.Bitmap, 0, 4),
		data: make([]interface{}, 0, 64),
	}
}

// Set sets a value at a specified index
func (p *Property) Set(idx uint32, value interface{}) {
	size := uint32(len(p.data))
	for i := size; i <= idx; i++ {
		p.free.Set(i)
		p.data = append(p.data, nil)
	}

	// If this is a replacement, remove
	if p.free.Contains(idx) {
		p.free.Remove(idx)
	}

	// Set the data at index
	p.data[idx] = value
}

// Get retrieves a value at a specified index
func (p *Property) Get(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(p.data)) || p.free.Contains(idx) {
		return nil, false
	}

	return p.data[idx], true
}

// Remove removes a value at a specified index
func (p *Property) Remove(idx uint32) {
	p.free.Set(idx)
}
