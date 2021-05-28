// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"sync"

	"github.com/kelindar/bitmap"
)

// Object represents a single object
type Object map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock  sync.RWMutex       // The collection lock
	size  uint32             // The current size
	fill  bitmap.Bitmap      // The fill-list
	props map[string]Mutator // The map of properties
	index map[string]*index  // The set of indexes

}

// New creates a new columnar collection.
func New() *Collection {
	return &Collection{
		props: make(map[string]Mutator, 8),
		index: make(map[string]*index, 8),
		fill:  make(bitmap.Bitmap, 0, 4),
	}
}

// Fetch retrieves an object by its handle
func (c *Collection) Fetch(index uint32) (Object, bool) {
	obj := make(Object, 8)
	if ok := c.FetchTo(index, &obj); !ok {
		return nil, false
	}
	return obj, true
}

// FetchTo retrieves an object by its handle into a existing object
func (c *Collection) FetchTo(idx uint32, dest *Object) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// If it's empty or over the sequence, not found
	if idx >= c.size || !c.fill.Contains(idx) {
		return false
	}

	// Reassemble the object from its properties
	obj := *dest
	for name, prop := range c.props {
		if v, ok := prop.Get(idx); ok {
			obj[name] = v
		}
	}
	return true
}

// Add adds an object to a collection and returns the allocated index
func (c *Collection) Add(obj Object) uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Find the index for the add
	idx, ok := c.fill.FirstZero()
	if !ok {
		idx = c.size
		c.size++
	}

	c.fill.Set(idx)

	// Add to all of the properties
	for k, v := range obj {
		if _, ok := c.props[k]; !ok {
			c.props[k] = newProperty()
		}

		// Set the value for this property
		c.props[k].Set(idx, v)

		// If there's an index for this property, keep it up-to-date
		for _, i := range c.index {
			if i.Target() == k {
				i.Set(idx, v)
			}
		}
	}

	return idx
}

// Remove removes the object
func (c *Collection) Remove(idx uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove from global index
	c.fill.Remove(idx)

	// Remove the data for this element
	for _, p := range c.props {
		p.Del(idx)
	}

	// Update the index for this element
	for _, i := range c.index {
		i.Del(idx)
	}
}

// Index creates an index on a specified property
func (c *Collection) Index(name, property string, fn IndexFunc) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if fn != nil {
		c.index[name] = newIndex(property, fn)
	} else { // Remove the index
		delete(c.index, name)
	}
}

// Count counts the number of elements which match the specified filter function. If
// there is no specified filter function, it returns the total count of elements in
// the collection.
func (c *Collection) Count(where func(where Query)) int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// If there's no filter specified, simply count everything
	if where == nil {
		return c.fill.Count()
	}

	q := c.query(where)
	defer releaseBitmap(q.index)
	return q.count()
}

// Find ...
func (c *Collection) Find(where func(where Query), fn func(Object) bool, props ...string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	q := c.query(where)
	defer releaseBitmap(q.index)
	q.iterate(fn, props)
}

func (c *Collection) query(where func(Query)) Query {
	r := aquireBitmap()
	c.fill.Clone(r)
	q := Query{
		owner: c,
		index: r,
	}
	where(q)
	return q
}
