<p align="center">
<img width="330" height="110" src=".github/logo.png" border="0" alt="kelindar/column">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/column" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/column"><img src="https://pkg.go.dev/badge/github.com/kelindar/column" alt="PkgGoDev"></a>
<a href="https://goreportcard.com/report/github.com/kelindar/column"><img src="https://goreportcard.com/badge/github.com/kelindar/column" alt="Go Report Card"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
<a href="https://coveralls.io/github/kelindar/column"><img src="https://coveralls.io/repos/github/kelindar/column/badge.svg" alt="Coverage"></a>
</p>

## Columnar In-Memory Store with Bitmap Indexing

This package contains a **high-performance, columnar, in-memory storage engine** that supports fast querying, update and iteration with zero-allocations and bitmap indexing.

## Features

- Optimized, cache-friendly **columnar data layout** that minimizes cache-misses.
- Optimized for **zero heap allocation** during querying (see benchmarks below).
- Optimized **batch updates/deletes**, an update during a transaction takes around `12ns`.
- Support for **SIMD-enabled filtering** (i.e. "where" clause) by leveraging [bitmap indexing](https://github.com/kelindar/bitmap).
- Support for **columnar projection** (i.e. "select" clause) for fast retrieval.
- Support for **computed indexes** that are dynamically calculated based on provided predicate.
- Support for **concurrent updates** using sharded latches to keep things fast.
- Support for **transaction isolation**, allowing you to create transactions and commit/rollback.
- Support for **expiration** of rows based on time-to-live or expiration column.
- Support for **atomic increment/decrement** of numerical values, transactionally.
- Support for **change data stream** that streams all commits consistently.
- Support for **concurrent snapshotting** allowing to store the entire collection into a file.

## Documentation

The general idea is to leverage cache-friendly ways of organizing data in [structures of arrays (SoA)](https://en.wikipedia.org/wiki/AoS_and_SoA) otherwise known "columnar" storage in database design. This, in turn allows us to iterate and filter over columns very efficiently. On top of that, this package also adds [bitmap indexing](https://en.wikipedia.org/wiki/Bitmap_index) to the columnar storage, allowing to build filter queries using binary `and`, `and not`, `or` and `xor` (see [kelindar/bitmap](https://github.com/kelindar/bitmap) with SIMD support).

- [Collection and Columns](#collection-and-columns)
- [Querying and Indexing](#querying-and-indexing)
- [Iterating over Results](#iterating-over-results)
- [Updating Values](#updating-values)
- [Expiring Values](#expiring-values)
- [Transaction Commit and Rollback](#transaction-commit-and-rollback)
- [Streaming Changes](#streaming-changes)
- [Snapshot and Restore](#snapshot-and-restore)
- [Complete Example](#complete-example)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)

## Collection and Columns

In order to get data into the store, you'll need to first create a `Collection` by calling `NewCollection()` method. Each collection requires a schema, which can be either specified manually by calling `CreateColumn()` multiple times or automatically inferred from an object by calling `CreateColumnsOf()` function.

In the example below we're loading some `JSON` data by using `json.Unmarshal()` and auto-creating colums based on the first element on the loaded slice. After this is done, we can then load our data by inserting the objects one by one into the collection. This is accomplished by calling `InsertObject()` method on the collection itself repeatedly.

```go
data := loadFromJson("players.json")

// Create a new columnar collection
players := column.NewCollection()
players.CreateColumnsOf(data[0])

// Insert every item from our loaded data
for _, v := range data {
	players.InsertObject(v)
}
```

Now, let's say we only want specific columns to be added. We can do this by calling `CreateColumn()` method on the collection manually to create the required columns.

```go
// Create a new columnar collection with pre-defined columns
players := column.NewCollection()
players.CreateColumn("name", column.ForString())
players.CreateColumn("class", column.ForString())
players.CreateColumn("balance", column.ForFloat64())
players.CreateColumn("age", column.ForInt16())

// Insert every item from our loaded data
for _, v := range loadFromJson("players.json") {
	players.InsertObject(v)
}
```

While the previous example demonstrated how to insert many objects, it was doing it one by one and is rather inefficient. This is due to the fact that each `InsertObject()` call directly on the collection initiates a separate transacion and there's a small performance cost associated with it. If you want to do a bulk insert and insert many values, faster, that can be done by calling `Insert()` on a transaction, as demonstrated in the example below. Note that the only difference is instantiating a transaction by calling the `Query()` method and calling the `txn.Insert()` method on the transaction instead the one on the collection.

```go
players.Query(func(txn *Txn) error {
	for _, v := range loadFromJson("players.json") {
		txn.InsertObject(v)
	}
	return nil // Commit
})
```

## Querying and Indexing

The store allows you to query the data based on a presence of certain attributes or their values. In the example below we are querying our collection and applying a _filtering_ operation bu using `WithValue()` method on the transaction. This method scans the values and checks whether a certain predicate evaluates to `true`. In this case, we're scanning through all of the players and looking up their `class`, if their class is equal to "rogue", we'll take it. At the end, we're calling `Count()` method that simply counts the result set.

```go
// This query performs a full scan of "class" column
players.Query(func(txn *column.Txn) error {
	count := txn.WithValue("class", func(v interface{}) bool {
		return v == "rogue"
	}).Count()
	return nil
})
```

Now, what if we'll need to do this query very often? It is possible to simply _create an index_ with the same predicate and have this computation being applied every time (a) an object is inserted into the collection and (b) an value of the dependent column is updated. Let's look at the example below, we're fist creating a `rogue` index which depends on "class" column. This index applies the same predicate which only returns `true` if a class is "rogue". We then can query this by simply calling `With()` method and providing the index name.

An index is essentially akin to a boolean column, so you could technically also select it's value when querying it. Now, in this example the query would be around `10-100x` faster to execute as behind the scenes it uses [bitmap indexing](https://github.com/kelindar/bitmap) for the "rogue" index and performs a simple logical `AND` operation on two bitmaps when querying. This avoid the entire scanning and applying of a predicate during the `Query`.

```go
// Create the index "rogue" in advance
out.CreateIndex("rogue", "class", func(v interface{}) bool {
	return v == "rogue"
})

// This returns the same result as the query before, but much faster
players.Query(func(txn *column.Txn) error {
	count := txn.With("rogue").Count()
	return nil
})
```

The query can be further expanded as it allows indexed `intersection`, `difference` and `union` operations. This allows you to ask more complex questions of a collection. In the examples below let's assume we have a bunch of indexes on the `class` column and we want to ask different questions.

First, let's try to merge two queries by applying a `Union()` operation with the method named the same. Here, we first select only rogues but then merge them together with mages, resulting in selection containing both rogues and mages.

```go
// How many rogues and mages?
players.Query(func(txn *Txn) error {
	txn.With("rogue").Union("mage").Count()
	return nil
})
```

Next, let's count everyone who isn't a rogue, for that we can use a `Without()` method which performs a difference (i.e. binary `AND NOT` operation) on the collection. This will result in a count of all players in the collection except the rogues.

```go
// How many rogues and mages?
players.Query(func(txn *Txn) error {
	txn.Without("rogue").Count()
	return nil
})
```

Now, you can combine all of the methods and keep building more complex queries. When querying indexed and non-indexed fields together it is important to know that as every scan will apply to only the selection, speeding up the query. So if you have a filter on a specific index that selects 50% of players and then you perform a scan on that (e.g. `WithValue()`), it will only scan 50% of users and hence will be 2x faster.

```go
// How many rogues that are over 30 years old?
players.Query(func(txn *Txn) error {
	txn.With("rogue").WithFloat("age", func(v float64) bool {
		return v >= 30
	}).Count()
	return nil
})
```

## Iterating over Results

In all of the previous examples, we've only been doing `Count()` operation which counts the number of elements in the result set. In this section we'll look how we can iterate over the result set.

As before, a transaction needs to be started using the `Query()` method on the collection. After which, we can call the `txn.Range()` method which allows us to iterate over the result set in the transaction. Note that it can be chained right after `With..()` methods, as expected.

In order to access the results of the iteration, prior to calling `Range()` method, we need to **first load column reader(s)** we are going to need, using methods such as `txn.String()`, `txn.Float64()`, etc. These prepare read/write buffers necessary to perform efficient lookups while iterating.

In the example below we select all of the rogues from our collection and print out their name by using the `Range()` method and accessing the "name" column using a column reader which is created by calling `txn.String("name")` method.

```go
players.Query(func(txn *Txn) error {
	names := txn.String("name") // Create a column reader

	return txn.With("rogue").Range(func(i uint32) {
		name, _ := names.Get()
		println("rogue name", name)
	})
})
```

Similarly, if you need to access more columns, you can simply create the appropriate column reader(s) and use them as shown in the example before.

```go
players.Query(func(txn *Txn) error {
	names := txn.String("name")
	ages  := txn.Int64("age")

	return txn.With("rogue").Range(func(i uint32) {
		name, _ := names.Get()
		age,  _ := ages.Get()

		println("rogue name", name)
		println("rogue age", age)
	})
})
```

## Updating Values

In order to update certain items in the collection, you can simply call `Range()` method and use column accessor's `Set()` or `Add()` methods to update a value of a certain column atomically. The updates won't be instantly reflected given that our store supports transactions. Only when transaction is commited, then the update will be applied to the collection, allowing for isolation and rollbacks.

In the example below we're selecting all of the rogues and updating both their balance and age to certain values. The transaction returns `nil`, hence it will be automatically committed when `Query()` method returns.

```go
players.Query(func(txn *Txn) error {
	balance := txn.Float64("balance")
	age     := txn.Int64("age")

	return txn.With("rogue").Range(func(i uint32) {
		balance.Set(10.0) // Update the "balance" to 10.0
		age.Set(50)       // Update the "age" to 50
	})
})
```

In certain cases, you might want to atomically increment or decrement numerical values. In order to accomplish this you can use the provided `Add()` operation. Note that the indexes will also be updated accordingly and the predicates re-evaluated with the most up-to-date values. In the below example we're incrementing the balance of all our rogues by _500_ atomically.

```go
players.Query(func(txn *Txn) error {
	balance := txn.Float64("balance")

	return txn.With("rogue").Range(func(i uint32) {
		balance.Add(500.0) // Increment the "balance" by 500
	})
})
```

## Expiring Values

Sometimes, it is useful to automatically delete certain rows when you do not need them anymore. In order to do this, the library automatically adds an `expire` column to each new collection and starts a cleanup goroutine aynchronously that runs periodically and cleans up the expired objects. In order to set this, you can simply use `InsertWithTTL()` method on the collection that allows to insert an object with a time-to-live duration defined.

In the example below we are inserting an object to the collection and setting the time-to-live to _5 seconds_ from the current time. After this time, the object will be automatically evicted from the collection and its space can be reclaimed.

```go
players.InsertObjectWithTTL(map[string]interface{}{
	"name": "Merlin",
	"class": "mage",
	"age": 55,
	"balance": 500,
}, 5 * time.Second) // The time-to-live of 5 seconds
```

On an interesting note, since `expire` column which is automatically added to each collection is an actual normal column, you can query and even update it. In the example below we query and conditionally update the expiration column. The example loads a time, adds one hour and updates it, but in practice if you want to do it you should use `Add()` method which can perform this atomically.

```go
players.Query(func(txn *column.Txn) error {
	expire := txn.Int64("expire")

	return txn.Range(func(i uint32) {
		if v, ok := expire.Get(); ok && v > 0 {
			oldExpire := time.Unix(0, v) // Convert expiration to time.Time
			newExpire := expireAt.Add(1 * time.Hour).UnixNano()  // Add some time
			expire.Set(newExpire)
		}
	})
})
```

## Transaction Commit and Rollback

Transactions allow for isolation between two concurrent operations. In fact, all of the batch queries must go through a transaction in this library. The `Query` method requires a function which takes in a `column.Txn` pointer which contains various helper methods that support querying. In the example below we're trying to iterate over all of the players and update their balance by setting it to `10.0`. The `Query` method automatically calls `txn.Commit()` if the function returns without any error. On the flip side, if the provided function returns an error, the query will automatically call `txn.Rollback()` so none of the changes will be applied.

```go
// Range over all of the players and update (successfully their balance)
players.Query(func(txn *column.Txn) error {
	balance := txn.Float64("balance")
	txn.Range(func(i uint32) {
		v.Set(10.0) // Update the "balance" to 10.0
	})

	// No error, transaction will be committed
	return nil
})
```

Now, in this example, we try to update balance but a query callback returns an error, in which case none of the updates will be actually reflected in the underlying collection.

```go
// Range over all of the players and update (successfully their balance)
players.Query(func(txn *column.Txn) error {
	balance := txn.Float64("balance")
	txn.Range(func(i uint32) {
		v.Set(10.0) // Update the "balance" to 10.0
	})

	// Returns an error, transaction will be rolled back
	return fmt.Errorf("bug")
})
```

## Streaming Changes

This library also supports streaming out all transaction commits consistently, as they happen. This allows you to implement your own change data capture (CDC) listeners, stream data into kafka or into a remote database for durability. In order to enable it, you can simply provide an implementation of a `commit.Writer` interface during the creation of the collection.

In the example below we take advantage of the `commit.Channel` implementation of a `commit.Writer` which simply publishes the commits into a go channel. Here we create a buffered channel and keep consuming the commits with a separate goroutine, allowing us to view transactions as they happen in the store.

```go
// Create a new commit writer (simple channel) and a new collection
writer  := make(commit.Channel, 1024)
players := NewCollection(column.Options{
	Writer: writer,
})

// Read the changes from the channel
go func(){
	for commit := writer{
		println("commit", commit.ID)
	}
}()

// ... insert, update or delete
```

On a separate note, this change stream is guaranteed to be consistent and serialized. This means that you can also replicate those changes on another database and synchronize both. In fact, this library also provides `Replay()` method on the collection that allows to do just that. In the example below we create two collections `primary` and `replica` and asychronously replicating all of the commits from the `primary` to the `replica` using the `Replay()` method together with the change stream.

```go
// Create a p rimary collection
writer  := make(commit.Channel, 1024)
primary := column.NewCollection(column.Options{
	Writer: &writer,
})
primary.CreateColumnsOf(object)

// Replica with the same schema
replica := column.NewCollection()
replica.CreateColumnsOf(object)

// Keep 2 collections in sync
go func() {
	for change := range writer {
		replica.Replay(change)
	}
}()
```

## Snapshot and Restore

The collection can also be saved in a single binary format while the transactions are running. This can allow you to periodically schedule backups or make sure all of the data is persisted when your application terminates.

In order to take a snapshot, you must first create a valid `io.Writer` destination and then call the `Snapshot()` method on the collection in order to create a snapshot, as demonstrated in the example below.

```go
dst, err := os.Create("snapshot.bin")
if err != nil {
	panic(err)
}

// Write a snapshot into the dst
err := players.Snapshot(dst)
```

Conversely, in order to restore an existing snapshot, you need to first open an `io.Reader` and then call the `Restore()` method on the collection. Note that the collection and its schema must be already initialized, as our snapshots do not carry this information within themselves.

```go
src, err := os.Open("snapshot.bin")
if err != nil {
	panic(err)
}

// Restore from an existing snapshot
err := players.Restore(src)
```

## Complete Example

```go
func main(){

	// Create a new columnar collection
	players := column.NewCollection()
	players.CreateColumn("serial", column.ForKey())
	players.CreateColumn("name", column.ForEnum())
	players.CreateColumn("active", column.ForBool())
	players.CreateColumn("class", column.ForEnum())
	players.CreateColumn("race", column.ForEnum())
	players.CreateColumn("age", column.ForFloat64())
	players.CreateColumn("hp", column.ForFloat64())
	players.CreateColumn("mp", column.ForFloat64())
	players.CreateColumn("balance", column.ForFloat64())
	players.CreateColumn("gender", column.ForEnum())
	players.CreateColumn("guild", column.ForEnum())

	// index on humans
	players.CreateIndex("human", "race", func(r column.Reader) bool {
		return r.String() == "human"
	})

	// index for mages
	players.CreateIndex("mage", "class", func(r column.Reader) bool {
		return r.String() == "mage"
	})

	// index for old
	players.CreateIndex("old", "age", func(r column.Reader) bool {
		return r.Float() >= 30
	})

	// Load the items into the collection
	loaded := loadFixture("players.json")
	players.Query(func(txn *column.Txn) error {
		for _, v := range loaded {
			txn.InsertObject(v)
		}
		return nil
	})

	// Run an indexed query
	players.Query(func(txn *column.Txn) error {
		name := txn.Enum("name")
		return txn.With("human", "mage", "old").Range(func(idx uint32) {
			value, _ := name.Get()
			println("old mage, human:", value)
		})
	})
}
```

## Benchmarks

The benchmarks below were ran on a collection of **100,000 items** containing a dozen columns. Feel free to explore the benchmarks but I strongly recommend testing it on your actual dataset.

```
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/insert-8            2523     469481 ns/op    24356 B/op    500 allocs/op
BenchmarkCollection/select-at-8     22194190      54.23 ns/op        0 B/op      0 allocs/op
BenchmarkCollection/scan-8              2068     568953 ns/op      122 B/op      0 allocs/op
BenchmarkCollection/count-8           571449       2057 ns/op        0 B/op      0 allocs/op
BenchmarkCollection/range-8            28660      41695 ns/op        3 B/op      0 allocs/op
BenchmarkCollection/update-at-8      5911978      202.8 ns/op        0 B/op      0 allocs/op
BenchmarkCollection/update-all-8        1280     946272 ns/op     3726 B/op      0 allocs/op
BenchmarkCollection/delete-at-8      6405852      188.9 ns/op        0 B/op      0 allocs/op
BenchmarkCollection/delete-all-8     2073188      562.6 ns/op        0 B/op      0 allocs/op
```

When testing for larger collections, I added a small example (see `examples` folder) and ran it with **20 million rows** inserted, each entry has **12 columns and 4 indexes** that need to be calculated, and a few queries and scans around them.

```
running insert of 20000000 rows...
-> insert took 20.4538183s

running snapshot of 20000000 rows...
-> snapshot took 2.57960038s

running full scan of age >= 30...
-> result = 10200000
-> full scan took 61.611822ms

running full scan of class == "rogue"...
-> result = 7160000
-> full scan took 81.389954ms

running indexed query of human mages...
-> result = 1360000
-> indexed query took 608.51µs

running indexed query of human female mages...
-> result = 640000
-> indexed query took 794.49µs

running update of balance of everyone...
-> updated 20000000 rows
-> update took 214.182216ms

running update of age of mages...
-> updated 6040000 rows
-> update took 81.292378ms
```

## Contributing

We are open to contributions, feel free to submit a pull request and we'll review it as quickly as we can. This library is maintained by [Roman Atachiants](https://www.linkedin.com/in/atachiants/)

## License

Tile is licensed under the [MIT License](LICENSE.md).
