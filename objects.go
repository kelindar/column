package columnar

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Object represents a single object
type Object map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	lock  sync.RWMutex         // The collection lock
	next  uint32               // The next index sequence
	free  freelist             // The fill/free-list
	props map[string]*Property // The map of properties
}

// New creates a new columnar collection.
func New() *Collection {
	return &Collection{
		props: make(map[string]*Property, 8),
	}
}

// Count returns the total count of elements in the collection
func (c *Collection) Count() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.free.Count()
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
func (c *Collection) FetchTo(id uint32, dest *Object) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// Check if the element is present
	if _, ok := c.free.Get(id); !ok {
		return false
	}

	// Reassemble the object from its properties
	obj := *dest
	for name, prop := range c.props {
		if v, ok := prop.Get(id); ok {
			obj[name] = v
		}
	}
	return true
}

// Add adds an object to a collection and returns the allocated index
func (c *Collection) Add(obj Object) uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Increment the sequence
	id := c.next
	c.next++
	c.free.Add(id)

	// Add to all of the properties
	for k, v := range obj {
		if _, ok := c.props[k]; !ok {
			c.props[k] = NewProperty()
		}
		c.props[k].Set(id, v)
	}

	return id
}

// Remove removes the object
func (c *Collection) Remove(id uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Remove from its own free list and from every property
	if _, ok := c.free.Remove(id); ok {
		for _, p := range c.props {
			p.Remove(id)
		}
	}
}

// Where applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (c *Collection) Where(property string, predicate func(v interface{}) bool) Query {
	return c.query().Where(property, predicate)
}

// query creates a new query
func (c *Collection) query() Query {
	fill := roaring.NewBitmap()
	fill.Or(&c.free.fill)

	return Query{
		owner: c,
		index: fill,
	}
}

// rangeProperty iterates over the property key/value pairs. If the callback returns
// false, the iteration is halted.
func (c *Collection) rangeProperty(f func(uint32, interface{}), name string) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if p, ok := c.props[name]; ok {
		p.Range(f)
	}
}
