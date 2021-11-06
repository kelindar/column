# Example: Key/Value Cache

This example demonstrates a `Key` column type that allows you to perform `O(1)` lookups over that. This can be used in the case where you do not have a specific offset for the entry.

```go
// Cache represents a key-value store
type Cache struct {
	store *column.Collection
}

// New creates a new key-value cache
func New() *Cache {
	db := column.NewCollection()
	db.CreateColumn("key", column.ForKey())
	db.CreateColumn("val", column.ForString())

	return &Cache{
		store: db,
	}
}

// Get attempts to retrieve a value for a key
func (c *Cache) Get(key string) (value string, found bool) {
	c.store.SelectAtKey(key, func(v column.Selector) {
		value = v.StringAt("val")
		found = true
	})
	return
}

// Set updates or inserts a new value
func (c *Cache) Set(key, value string) {
	if err := c.store.UpdateAtKey(key, "val", func(v column.Cursor) error {
		v.SetString(value)
		return nil
	}); err != nil {
		panic(err)
	}
}
```

## Some Results

```
running insert of 50000 rows...
-> inserted 10000 rows
-> inserted 20000 rows
-> inserted 30000 rows
-> inserted 40000 rows
-> inserted 50000 rows
-> insert took 80.2478ms

running query of user_11255...
Hi, User 11255 true
-> query took 1.271µs
```
