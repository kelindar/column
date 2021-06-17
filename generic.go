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

// Grow grows the size of the column until we have enough to store
func (c *columnnumber) Grow(idx uint32) {
	c.Lock()
	// TODO: also grow the bitmap
	size := uint32(len(c.data))
	for i := size; i <= idx; i++ {
		c.data = append(c.data, 0)
	}
	c.Unlock()
}

// UpdateMany performs a series of updates at once
func (c *columnnumber) UpdateMany(updates []Update) {
	c.Lock()
	defer c.Unlock()

	// Range over all of the updates, and depending on the operation perform the action
	for i, u := range updates {
		c.fill.Set(u.Index)
		switch u.Kind {
		case UpdatePut:
			c.data[u.Index] = u.Value.(number)

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case UpdateAdd:
			value := c.data[u.Index] + u.Value.(number)
			c.data[u.Index] = value
			updates[i] = Update{
				Kind:  UpdatePut,
				Index: u.Index,
				Value: value,
			}
		}
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
