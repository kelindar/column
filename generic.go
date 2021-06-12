package column

import (
	"sync"

	"github.com/cheekybits/genny/generic"
	"github.com/kelindar/bitmap"
)

type number generic.Number

// --------------------------- numbers ----------------------------

// columnnumber represents a generic column
type columnnumber struct {
	sync.RWMutex
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

// Delete removes a value at a specified index
func (c *columnnumber) Delete(idx uint32) {
	c.Lock()
	c.fill.Remove(idx)
	c.Unlock()
}

// DeleteMany deletes a set of items from the column.
func (c *columnnumber) DeleteMany(items *bitmap.Bitmap) {
	c.Lock()
	c.fill.AndNot(*items)
	c.Unlock()
}

// Contains checks whether the column has a value at a specified index.
func (c *columnnumber) Contains(idx uint32) bool {
	c.RLock()
	defer c.RUnlock()
	return c.fill.Contains(idx)
}

// And performs a logical and operation and updates the destination bitmap.
func (c *columnnumber) And(dst *bitmap.Bitmap) {
	c.RLock()
	dst.And(c.fill)
	c.RUnlock()
}

// And performs a logical and not operation and updates the destination bitmap.
func (c *columnnumber) AndNot(dst *bitmap.Bitmap) {
	c.RLock()
	dst.AndNot(c.fill)
	c.RUnlock()
}

// Or performs a logical or operation and updates the destination bitmap.
func (c *columnnumber) Or(dst *bitmap.Bitmap) {
	c.RLock()
	dst.Or(c.fill)
	c.RUnlock()
}
