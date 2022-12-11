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
- Support for **SIMD-enabled aggregate functions** such as "sum", "avg", "min" and "max".
- Support for **SIMD-enabled filtering** (i.e. "where" clause) by leveraging [bitmap indexing](https://github.com/kelindar/bitmap).
- Support for **columnar projection** (i.e. "select" clause) for fast retrieval.
- Support for **computed indexes** that are dynamically calculated based on provided predicate.
- Support for **concurrent updates** using sharded latches to keep things fast.
- Support for **transaction isolation**, allowing you to create transactions and commit/rollback.
- Support for **expiration** of rows based on time-to-live or expiration column.
- Support for **atomic merging** of any values, transactionally.
- Support for **primary keys** for use-cases where offset can't be used.
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
- [Using Primary Keys](#using-primary-keys)
- [Storing Binary Records](#storing-binary-records)
- [Streaming Changes](#streaming-changes)
- [Snapshot and Restore](#snapshot-and-restore)
- [Examples](#examples)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)

## Collection and Columns

In order to get data into the store, you'll need to first create a `Collection` by calling `NewCollection()` method. Each collection requires a schema, which needs to be specified by calling `CreateColumn()` multiple times or automatically inferred from an object by calling `CreateColumnsOf()` function. In the example below we create a new collection with several columns.

```go
// Create a new collection with some columns
players := column.NewCollection()
players.CreateColumn("name", column.ForString())
players.CreateColumn("class", column.ForString())
players.CreateColumn("balance", column.ForFloat64())
players.CreateColumn("age", column.ForInt16())
```

Now that we have created a collection, we can insert a single record by using `Insert()` method on the collection. In this example we're inserting a single row and manually specifying values. Note that this function returns an `index` that indicates the row index for the inserted row.

```go
index, err := players.Insert(func(r column.Row) error {
	r.SetString("name", "merlin")
	r.SetString("class", "mage")
	r.SetFloat64("balance", 99.95)
	r.SetInt16("age", 107)
	return nil
})
```

While the previous example demonstrated how to insert a single row, inserting multiple rows this way is rather inefficient. This is due to the fact that each `Insert()` call directly on the collection initiates a separate transacion and there's a small performance cost associated with it. If you want to do a bulk insert and insert many values, faster, that can be done by calling `Insert()` on a transaction, as demonstrated in the example below. Note that the only difference is instantiating a transaction by calling the `Query()` method and calling the `txn.Insert()` method on the transaction instead the one on the collection.

```go
players.Query(func(txn *column.Txn) error {
	for _, v := range myRawData {
		txn.Insert(...)
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
players.Query(func(txn *column.Txn) error {
	txn.With("rogue").Union("mage").Count()
	return nil
})
```

Next, let's count everyone who isn't a rogue, for that we can use a `Without()` method which performs a difference (i.e. binary `AND NOT` operation) on the collection. This will result in a count of all players in the collection except the rogues.

```go
// How many rogues and mages?
players.Query(func(txn *column.Txn) error {
	txn.Without("rogue").Count()
	return nil
})
```

Now, you can combine all of the methods and keep building more complex queries. When querying indexed and non-indexed fields together it is important to know that as every scan will apply to only the selection, speeding up the query. So if you have a filter on a specific index that selects 50% of players and then you perform a scan on that (e.g. `WithValue()`), it will only scan 50% of users and hence will be 2x faster.

```go
// How many rogues that are over 30 years old?
players.Query(func(txn *column.Txn) error {
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
players.Query(func(txn *column.Txn) error {
	names := txn.String("name") // Create a column reader

	return txn.With("rogue").Range(func(i uint32) {
		name, _ := names.Get()
		println("rogue name", name)
	})
})
```

Similarly, if you need to access more columns, you can simply create the appropriate column reader(s) and use them as shown in the example before.

```go
players.Query(func(txn *column.Txn) error {
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

Taking the `Sum()` of a (numeric) column reader will take into account a transaction's current filtering index.

```go
players.Query(func(txn *column.Txn) error {
	totalAge := txn.With("rouge").Int64("age").Sum()
	totalRouges := int64(txn.Count())

	avgAge := totalAge / totalRouges

	txn.WithInt("age", func(v float64) bool {
		return v < avgAge
	})

	// get total balance for 'all rouges younger than the average rouge'
	balance := txn.Float64("balance").Sum()
	return nil
})
```

## Sorted Indexes

Along with bitmap indexing, collections support consistently sorted indexes. These indexes are transient, and must be recreated when a collection is loading a snapshot. 

In the example below, we create a SortedIndex object and use it to sort filtered records in a transaction.

```go
// Create the sorted index "sortedNames" in advance
out.CreateSortIndex("richest", "balance")

// This filters the transaction with the `rouge` index before
// ranging through the remaining balances in ascending order
players.Query(func(txn *column.Txn) error {
	name    := txn.String("name")
	balance := txn.Float64("balance")

	txn.With("rogue").Ascend("richest", func (i uint32) {
		// save or do something with sorted record
		curName, _ := name.Get()
		balance.Set(newBalance(curName))
	})
	return nil
})
```

## Updating Values

In order to update certain items in the collection, you can simply call `Range()` method and use column accessor's `Set()` or `Add()` methods to update a value of a certain column atomically. The updates won't be instantly reflected given that our store supports transactions. Only when transaction is commited, then the update will be applied to the collection, allowing for isolation and rollbacks.

In the example below we're selecting all of the rogues and updating both their balance and age to certain values. The transaction returns `nil`, hence it will be automatically committed when `Query()` method returns.

```go
players.Query(func(txn *column.Txn) error {
	balance := txn.Float64("balance")
	age     := txn.Int64("age")

	return txn.With("rogue").Range(func(i uint32) {
		balance.Set(10.0) // Update the "balance" to 10.0
		age.Set(50)       // Update the "age" to 50
	})
})
```

In certain cases, you might want to atomically increment or decrement numerical values. In order to accomplish this you can use the provided `Merge()` operation. Note that the indexes will also be updated accordingly and the predicates re-evaluated with the most up-to-date values. In the below example we're incrementing the balance of all our rogues by _500_ atomically.

```go
players.Query(func(txn *column.Txn) error {
	balance := txn.Float64("balance")

	return txn.With("rogue").Range(func(i uint32) {
		balance.Merge(500.0) // Increment the "balance" by 500
	})
})
```

While atomic increment/decrement for numerical values is relatively straightforward, this `Merge()` operation can be specified using `WithMerge()` option and also used for other data types, such as strings. In the example below we are creating a merge function that concatenates two strings together and when `MergeString()` is called, the new string gets appended automatically.

```go
// A merging function that simply concatenates 2 strings together
concat := func(value, delta string) string {
	if len(value) > 0 {
		value += ", "
	}
	return value + delta
}

// Create a column with a specified merge function
db := column.NewCollection()
db.CreateColumn("alphabet", column.ForString(column.WithMerge(concat)))

// Insert letter "A"
db.Insert(func(r column.Row) error {
	r.SetString("alphabet", "A") // now contains "A"
	return nil
})

// Insert letter "B"
db.QueryAt(0, func(r column.Row) error {
	r.MergeString("alphabet", "B") // now contains "A, B"
	return nil
})
```

## Expiring Values

Sometimes, it is useful to automatically delete certain rows when you do not need them anymore. In order to do this, the library automatically adds an `expire` column to each new collection and starts a cleanup goroutine aynchronously that runs periodically and cleans up the expired objects. In order to set this, you can simply use `Insert...()` method on the collection that allows to insert an object with a time-to-live duration defined.

In the example below we are inserting an object to the collection and setting the time-to-live to _5 seconds_ from the current time. After this time, the object will be automatically evicted from the collection and its space can be reclaimed.

```go
players.Insert(func(r column.Row) error {
	r.SetString("name", "Merlin")
	r.SetString("class", "mage")
	r.SetTTL(5 * time.Second) // time-to-live of 5 seconds
	return nil
})
```

On an interesting note, since `expire` column which is automatically added to each collection is an actual normal column, you can query and even update it. In the example below we query and extend the time-to-live by 1 hour using the `Extend()` method.

```go
players.Query(func(txn *column.Txn) error {
	ttl := txn.TTL()
	return txn.Range(func(i uint32) {
		ttl.Extend(1 * time.Hour) // Add some time
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

## Using Primary Keys

In certain cases it is useful to access a specific row by its primary key instead of an index which is generated internally by the collection. For such use-cases, the library provides `Key` column type that enables a seamless lookup by a user-defined _primary key_. In the example below we create a collection with a primary key `name` using `CreateColumn()` method with a `ForKey()` column type. Then, we use `InsertKey()` method to insert a value.

```go
players := column.NewCollection()
players.CreateColumn("name", column.ForKey())     // Create a "name" as a primary-key
players.CreateColumn("class", column.ForString()) // .. and some other columns

// Insert a player with "merlin" as its primary key
players.InsertKey("merlin", func(r column.Row) error {
	r.SetString("class", "mage")
	return nil
})
```

Similarly, you can use primary key to query that data directly, without knowing the exact offset. Do note that using primary keys will have an overhead, as it requires an additional step of looking up the offset using a hash table managed internally.

```go
// Query merlin's class
players.QueryKey("merlin", func(r column.Row) error {
	class, _ := r.String("class")
	return nil
})
```

## Storing Binary Records

If you find yourself in need of encoding a more complex structure as a single column, you may do so by using `column.ForRecord()` function. This allows you to specify a `BinaryMarshaler` / `BinaryUnmarshaler` type that will get automatically encoded as a single column. In th example below we are creating a `Location` type that implements the required methods.

```go
type Location struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

func (l Location) MarshalBinary() ([]byte, error) {
	return json.Marshal(l)
}

func (l *Location) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, l)
}
```

Now that we have a record implementation, we can create a column for this struct by using `ForRecord()` function as shown below.

```go
players.CreateColumn("location", ForRecord(func() *Location {
	return new(Location)
}))
```

In order to manipulate the record, we can use the appropriate `Record()`, `SetRecord()` methods of the `Row`, similarly to other column types.

```go
// Insert a new location
idx, _ := players.Insert(func(r Row) error {
	r.SetRecord("location", &Location{X: 1, Y: 2})
	return nil
})

// Read the location back
players.QueryAt(idx, func(r Row) error {
	location, ok := r.Record("location")
	return nil
})
```

## Streaming Changes

This library also supports streaming out all transaction commits consistently, as they happen. This allows you to implement your own change data capture (CDC) listeners, stream data into kafka or into a remote database for durability. In order to enable it, you can simply provide an implementation of a `commit.Logger` interface during the creation of the collection.

In the example below we take advantage of the `commit.Channel` implementation of a `commit.Logger` which simply publishes the commits into a go channel. Here we create a buffered channel and keep consuming the commits with a separate goroutine, allowing us to view transactions as they happen in the store.

```go
// Create a new commit writer (simple channel) and a new collection
writer  := make(commit.Channel, 1024)
players := NewCollection(column.Options{
	Writer: writer,
})

// Read the changes from the channel
go func(){
	for commit := range writer {
		fmt.Printf("commit %v\n", commit.ID)
	}
}()

// ... insert, update or delete
```

On a separate note, this change stream is guaranteed to be consistent and serialized. This means that you can also replicate those changes on another database and synchronize both. In fact, this library also provides `Replay()` method on the collection that allows to do just that. In the example below we create two collections `primary` and `replica` and asychronously replicating all of the commits from the `primary` to the `replica` using the `Replay()` method together with the change stream.

```go
// Create a primary collection
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

## Examples

Multiple complete usage examples of this library can be found in the [examples](https://github.com/kelindar/column/tree/main/examples) directory in this repository.

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
