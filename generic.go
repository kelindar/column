package column

import (
	"github.com/cheekybits/genny/generic"
	"github.com/kelindar/bitmap"
)

type number generic.Number

// --------------------------- numbers ----------------------------

// columnnumber represents a generic column
type columnnumber struct {
	fill bitmap.Bitmap // The fill-list
	data []number      // The actual values
}

// makenumbers creates a new vector or numbers
func makenumbers() Column {
	return &columnnumber{
		fill: make(bitmap.Bitmap, 0, 4),
		data: make([]number, 0, 64),
	}
}

// Set sets a value at a specified index
func (p *columnnumber) Set(idx uint32, value interface{}) {
	size := uint32(len(p.data))
	for i := size; i <= idx; i++ {
		p.data = append(p.data, 0)
	}

	// Set the data at index
	p.fill.Set(idx)
	p.data[idx] = value.(number)
}

// Value retrieves a value at a specified index
func (p *columnnumber) Value(idx uint32) (interface{}, bool) {
	if idx >= uint32(len(p.data)) || !p.fill.Contains(idx) {
		return number(0), false
	}

	return p.data[idx], true
}

// Float64 retrieves a float64 value at a specified index
func (p *columnnumber) Float64(idx uint32) (float64, bool) {
	if idx >= uint32(len(p.data)) || !p.fill.Contains(idx) {
		return 0, false
	}

	return float64(p.data[idx]), true
}

// Int64 retrieves an int64 value at a specified index
func (p *columnnumber) Int64(idx uint32) (int64, bool) {
	if idx >= uint32(len(p.data)) || !p.fill.Contains(idx) {
		return 0, false
	}

	return int64(p.data[idx]), true
}

// Uint64 retrieves an uint64 value at a specified index
func (p *columnnumber) Uint64(idx uint32) (uint64, bool) {
	if idx >= uint32(len(p.data)) || !p.fill.Contains(idx) {
		return 0, false
	}

	return uint64(p.data[idx]), true
}

// Del removes a value at a specified index
func (p *columnnumber) Del(idx uint32) {
	p.fill.Remove(idx)
	p.data[idx] = 0
}

// Bitmap returns the associated index bitmap.
func (p *columnnumber) Bitmap() bitmap.Bitmap {
	return p.fill
}
