package columnar

import (
	"sync"

	"github.com/kelindar/bitmap"
)

// Object represents a single object
type Object map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock  sync.RWMutex         // The collection lock
	size  uint32               // The current size
	fill  bitmap.Bitmap        // The fill-list
	props map[string]*Property // The map of properties
}

// New creates a new columnar collection.
func New() *Collection {
	return &Collection{
		props: make(map[string]*Property, 8),
		fill:  make(bitmap.Bitmap, 0, 4),
	}
}

// Count returns the total count of elements in the collection
func (c *Collection) Count() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.fill.Count()
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
			c.props[k] = NewProperty()
		}
		c.props[k].Set(idx, v)
	}

	return idx
}

// Remove removes the object
func (c *Collection) Remove(idx uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove from the index and from each property
	c.fill.Remove(idx)
	for _, p := range c.props {
		p.Remove(idx)
	}
}

// Where applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (c *Collection) Where(property string, predicate func(v interface{}) bool) *Query {
	return c.query().And(property, predicate)
}

// query creates a new query
func (c *Collection) query() *Query {
	return &Query{
		owner: c,
		index: c.fill.Clone(nil),
	}
}
