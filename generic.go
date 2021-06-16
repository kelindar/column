// +build ignore

package column

import (
	"github.com/cheekybits/genny/generic"
	"github.com/kelindar/bitmap"
)

type number generic.Number

// --------------------------- numbers ----------------------------

// columnnumber represents a generic column
type columnnumber struct {
	column
	data []number // The actual values
}

// makenumbers creates a new vector or numbers
func makenumbers() Column {
	return &columnnumber{
		data: make([]number, 0, 64),
		column: column{
			fill: make(bitmap.Bitmap, 0, 4),
		},
	}
}

// Update sets a value at a specified index
func (c *columnnumber) Update(idx uint32, value interface{}) {
	c.Lock()
	defer c.Unlock()

	size := uint32(len(c.data))
	for i := size; i <= idx; i++ {
		c.data = append(c.data, 0)
	}

	// Set the data at index
	c.fill.Set(idx)
	c.data[idx] = value.(number)
}

// UpdateMany performs a series of updates at once
func (c *columnnumber) UpdateMany(updates []Update) {
	c.Lock()
	defer c.Unlock()

	for _, u := range updates {
		c.fill.Set(u.Index)
		c.data[u.Index] = u.Value.(number)
	}
}

// Value retrieves a value at a specified index
func (c *columnnumber) Value(idx uint32) (v interface{}, ok bool) {
	v = number(0)
	c.RLock()
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	c.RUnlock()
	return
}

// Float64 retrieves a float64 value at a specified index
func (c *columnnumber) Float64(idx uint32) (v float64, ok bool) {
	c.RLock()
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	c.RUnlock()
	return
}

// Int64 retrieves an int64 value at a specified index
func (c *columnnumber) Int64(idx uint32) (v int64, ok bool) {
	c.RLock()
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	c.RUnlock()
	return
}

// Uint64 retrieves an uint64 value at a specified index
func (c *columnnumber) Uint64(idx uint32) (v uint64, ok bool) {
	c.RLock()
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	c.RUnlock()
	return
}
