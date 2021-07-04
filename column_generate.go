package column

import (
	"github.com/cheekybits/genny/generic"
	"github.com/kelindar/bitmap"
	"github.com/kelindar/column/commit"
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

// Grow grows the size of the column until we have enough to store
func (c *columnnumber) Grow(idx uint32) {
	if idx < uint32(len(c.data)) {
		return
	}

	if idx < uint32(cap(c.data)) {
		c.fill.Grow(idx)
		c.data = c.data[:idx+1]
		return
	}

	c.fill.Grow(idx)
	clone := make([]number, idx+1, capacityFor(idx+1))
	copy(clone, c.data)
	c.data = clone
}

// Update performs a series of updates at once
func (c *columnnumber) Update(updates []commit.Update) {

	// Range over all of the updates, and depending on the operation perform the action
	for i, u := range updates {
		c.fill[u.Index>>6] |= 1 << (u.Index & 0x3f) // Set the bit without grow
		switch u.Type {
		case commit.Put:
			c.data[u.Index] = u.Value.(number)

		// If this is an atomic increment/decrement, we need to change the operation to
		// the final value, since after this update an index needs to be recalculated.
		case commit.Add:
			value := c.data[u.Index] + u.Value.(number)
			c.data[u.Index] = value
			updates[i] = commit.Update{
				Type:  commit.Put,
				Index: u.Index,
				Value: value,
			}
		}
	}
}

// Delete deletes a set of items from the column.
func (c *columnnumber) Delete(offset int, items bitmap.Bitmap) {
	fill := c.fill[offset:]
	fill.AndNot(items)
}

// Contains checks whether the column has a value at a specified index.
func (c *columnnumber) Contains(idx uint32) bool {
	return c.fill.Contains(idx)
}

// Index returns the fill list for the column
func (c *columnnumber) Index() *bitmap.Bitmap {
	return &c.fill
}

// Value retrieves a value at a specified index
func (c *columnnumber) Value(idx uint32) (v interface{}, ok bool) {
	v = number(0)
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = c.data[idx], true
	}
	return
}

// LoadFloat64 retrieves a float64 value at a specified index
func (c *columnnumber) LoadFloat64(idx uint32) (v float64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = float64(c.data[idx]), true
	}
	return
}

// LoadInt64 retrieves an int64 value at a specified index
func (c *columnnumber) LoadInt64(idx uint32) (v int64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = int64(c.data[idx]), true
	}
	return
}

// LoadUint64 retrieves an uint64 value at a specified index
func (c *columnnumber) LoadUint64(idx uint32) (v uint64, ok bool) {
	if idx < uint32(len(c.data)) && c.fill.Contains(idx) {
		v, ok = uint64(c.data[idx]), true
	}
	return
}

// FilterFloat64 filters down the values based on the specified predicate.
func (c *columnnumber) FilterFloat64(offset uint32, index bitmap.Bitmap, predicate func(v float64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) bool {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(float64(c.data[idx]))
	})
}

// FilterInt64 filters down the values based on the specified predicate.
func (c *columnnumber) FilterInt64(offset uint32, index bitmap.Bitmap, predicate func(v int64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(int64(c.data[idx]))
	})
}

// FilterUint64 filters down the values based on the specified predicate.
func (c *columnnumber) FilterUint64(offset uint32, index bitmap.Bitmap, predicate func(v uint64) bool) {
	index.And(c.fill[offset>>6 : int(offset>>6)+len(index)])
	index.Filter(func(idx uint32) (match bool) {
		idx = offset + idx
		return idx < uint32(len(c.data)) && predicate(uint64(c.data[idx]))
	})
}
