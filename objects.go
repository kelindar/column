package columnar

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Object represents a single object
type Object map[string]interface{}

// Collection represents a collection of objects in a columnar format
type Collection struct {
	csize uint32               // The count of max size
	cfree int32                // The count of free items
	lock  sync.RWMutex         // The collection lock
	free  roaring.Bitmap       // The index of free (reclaimable) entries
	props map[string]*Property // The map of properties
}

func New() *Collection {
	return &Collection{
		props: make(map[string]*Property, 8),
	}
}

// Count returns the total count of elements in the collection
func (c *Collection) Count() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return int(c.csize) - int(c.cfree)
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
func (c *Collection) FetchTo(index uint32, dest *Object) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// If the index is out of bounds or points to a free spot
	if index >= uint32(c.csize) || c.free.Contains(index) || dest == nil {
		return false
	}

	// Reassemble the object from its properties
	obj := *dest
	for name, prop := range c.props {
		if v, ok := prop.Get(index); ok {
			obj[name] = v
		}
	}
	return true
}

// Add adds an object to a collection and returns the allocated index
func (c *Collection) Add(obj Object) uint32 {
	handle := c.next()
	for k, v := range obj {
		if _, ok := c.props[k]; !ok {
			c.props[k] = NewProperty()
		}
		c.props[k].Set(handle, v)
	}

	return handle
}

// Remove removes the object
func (c *Collection) Remove(index uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Simply add the index to the free list, no need to actually
	// delete the underlying data.
	if index < uint32(c.csize) || !c.free.Contains(index) {
		c.cfree++
		c.free.Add(uint32(index))
	}
}

// Where applies a filter predicate over values for a specific properties. It filters
// down the items in the query.
func (c *Collection) Where(predicate func(v interface{}) bool, property string) *Query {
	return c.query().Where(predicate, property)
}

// query creates a new query
func (c *Collection) query() *Query {
	fill := roaring.NewBitmap()
	fill.AddRange(uint64(0), uint64(c.csize))
	fill.Xor(&c.free)

	return &Query{
		owner: c,
		index: fill,
	}
}

// next allocates a next available handle
func (c *Collection) next() uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.free.IsEmpty() {
		idx := uint32(c.csize)
		c.csize++
		return idx
	}

	// Find the next available slot and remove it
	c.cfree--
	idx := c.free.Iterator().Next()
	c.free.Remove(idx)
	return uint32(idx)
}

// rangeProperty iterates over the property key/value pairs. If the callback returns
// false, the iteration is halted.
func (c *Collection) rangeProperty(f func(uint32, interface{}) bool, name string) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if p, ok := c.props[name]; ok {
		p.Range(f)
	}
}
