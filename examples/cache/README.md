# Example: Key/Value Cache

Here's an example of how one might go about building a key-value cache using columnar storage. This code is kept simple on purpose and hence is not the most efficient one, but do note that it doesn't actually have any hash maps and uses exclusively bitmap indexing to retrieve keys.  

## Implementation

Three columns are created on the collection `key` for the key string itself, `val` for the string value to be cached and `crc` for the checksum/hash value of the key.

```go
db := column.NewCollection()
db.CreateColumn("key", column.ForString())
db.CreateColumn("val", column.ForString())
db.CreateColumn("crc", column.ForUint64())
```

On top of `crc` column we create a number of bucket indices, in this example `100` buckets are created. Basically it functions as a transposed hash table when you think about it, but in this implementation the bucket size will vary - more items will result in larger buckets and larger search for the key. It's not ideal, but interesting for the sake of the exercice.

```go
for i := 0; i < buckets; i++ {
    bucket := uint(i) // copy the value
    db.CreateIndex(strconv.Itoa(i), "crc", func(r column.Reader) bool {
        return r.Uint()%uint(buckets) == bucket
    })
}
```

Next, we also create a bitmap to act as a bloom filter. This allows us to quikcly check if a single key is present in the collection or not, and quickly skip search if it does not exist. It's helps to make the insertion not too slow for large numbers of elements stored in the cache.

```go
func (c *Cache) addToFilter(hash uint32) {
	position := hash % uint32(len(c.bloom)*64)
	c.bloom.Set(position)
}

func (c *Cache) checkFilter(hash uint32) bool {
	position := hash % uint32(len(c.bloom)*64)
	return c.bloom.Contains(position)
}

```

The search function is expressed with a single transaction. It computes the bucket, narrows down the search to the bucket using the bitmap index with the same name, then does a linear search within it to compare the hash. Once found, it selects the value and the index.

```go
// Get attempts to retrieve a value for a key
func (c *Cache) Get(key string) (value string, found bool) {
	hash := crc32.ChecksumIEEE([]byte(key))
	value, idx := c.search(hash)
	return value, idx >= 0
}

// search attempts to retrieve a value for a key. If the value is found, it returns
// the actual value and its index in the collection. Otherwise, it returns -1.
func (c *Cache) search(hash uint32) (value string, index int) {
	index = -1

	c.store.Query(func(txn *column.Txn) error {
		bucketName := fmt.Sprintf("%d", hash%uint32(buckets))
		return txn.
			With(bucketName).
			WithUint("crc", func(v uint64) bool {
				return v == uint64(hash)
			}).Range("val", func(v column.Cursor) {
			value = v.String()
			index = int(v.Index())
		})
	})
	return
}
```

On the flip side, when you want to insert the key we simply check the bloom filter, and if it exists we perform a search and update the value if found. Otherwise, we add to the bloom filter and insert a new row.

```go
// Set updates or inserts a new value
func (c *Cache) Set(key, value string) {
	hash := crc32.ChecksumIEEE([]byte(key))

	// First check if the value already exists, and update it if found.
	if c.checkFilter(hash) {
		if _, idx := c.search(hash); idx >= 0 {
			c.store.UpdateAt(uint32(idx), "val", func(v column.Cursor) error {
				v.SetString(value)
				return nil
			})
			return
		}
	}

	// If not found, insert a new row
	c.addToFilter(hash)
	c.store.Insert(map[string]interface{}{
		"key": key,
		"val": value,
		"crc": uint64(hash),
	})
}
```

## Some Results

Below are some results for the cache, it first populates 50,000 items, which is kind of slow. Then, does 10 random searches and measures it. Here, since we have 100 buckets and 50,000 items the search would produce around 500 elements per retrieval, after the binary AND operation.

```
running insert of 50000 rows...
-> inserted 10000 rows
-> inserted 20000 rows
-> inserted 30000 rows
-> inserted 40000 rows
-> inserted 50000 rows
-> insert took 214.1031ms

running query of user_10156...
Hi, User 10156 true
-> query took 3.683µs

running query of user_26245...
Hi, User 26245 true
-> query took 3.862µs

running query of user_4187...
Hi, User 4187 true
-> query took 3.427µs

running query of user_10333...
Hi, User 10333 true
-> query took 3.529µs

running query of user_22579...
Hi, User 22579 true
-> query took 3.508µs

running query of user_14530...
Hi, User 14530 true
-> query took 3.352µs

running query of user_11922...
Hi, User 11922 true
-> query took 3.508µs

running query of user_24969...
Hi, User 24969 true
-> query took 3.542µs

running query of user_44266...
Hi, User 44266 true
-> query took 7.024µs

running query of user_482...
Hi, User 482 true
-> query took 3.503µs
```