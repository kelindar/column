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

This package contains a **hihg-performance, columnar, in-memory storage engine** that supports fast querying and iteration with zero-allocations and bitmap indexing. 

The general idea is to leverage cache-friendly ways of organizing data in [structures of arrays (SoA)](https://en.wikipedia.org/wiki/AoS_and_SoA) otherwise known "columnar" storage in database design. This, in turn allows us to iterate and filter over columns very efficiently. On top of that, this package also adds [bitmap indexing](https://en.wikipedia.org/wiki/Bitmap_index) to the columnar storage, allowing to build filter queries using binary `and`, `and not`, `or` and `xor` (see [kelindar/bitmap](https://github.com/kelindar/bitmap) with SIMD support). 

## Features

 * Cache-friendly **columnar data layout** that minimizes cache-misses.
 * **Zero heap allocation** (or close to it) inside the library (see benchmarks below).
 * Support for **SIMD-enabled filtering** (i.e. "where" clause) by leveraging [bitmap indexing](https://github.com/kelindar/bitmap).
 * Support for **columnar projection**  (i.e. "select" clause) for fast retrieval.
 * Support for **computed indexes** that are dynamically calculated based on provided predicate.
 * Support for **concurrent updates** on a per-column basis (e.g. 2 goroutines can update 2 columns at the same time).
 * Support for **transaction isolation**, allowing you to create transactions and commit/rollback.
 * Optimized **batch updates/deletes**, an update during a transaction takes around `12ns`.


## Example usage

```go
func main(){

	// Create a new columnar collection
	players := column.NewCollection()

	// index on humans
	players.CreateIndex("human", "race", func(v interface{}) bool {
		return v == "human"
	})

	// index for mages
	players.CreateIndex("mage", "class", func(v interface{}) bool {
		return v == "mage"
	})

	// index for old
	players.CreateIndex("old", "age", func(v interface{}) bool {
		return v.(float64) >= 30
	})

	// Load the items into the collection
	loaded := loadFixture("players.json")
	players.CreateColumnsOf(loaded[0])
	for _, v := range loaded {
		players.Insert(v)
	}

	// This performs a full scan on 3 different columns and compares them given the 
	// specified predicates. This is not indexed, but does a columnar scan which is
	// cache-friendly.
	players.Query(func(txn *column.Txn) error {
		println(txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithFloat("age", func(v float64) bool {
			return v >= 30
		}).Count()) // prints the count
		return nil
	})

	// This performs a cound, but instead of scanning through the entire dataset, it scans
	// over pre-built indexes and combines them using a logical AND operation. The result
	// will be the same as the query above but the performance of the query is 10x-100x
	// faster depending on the size of the underlying data.
	players.Query(func(txn *column.Txn) error {
		println(txn.With("human", "mage", "old").Count()) // prints the count
		return nil
	})

	// Same condition as above, but we also select the actual names of those 
	// players and iterate through them.
	players.Query(func(txn *column.Txn) error {
		txn.With("human", "mage", "old").Range(func(v column.Cursor) bool {
			println(v.String()) // prints the name
			return true
		}, "name") // The column to select
		return nil
	})
}
```

## Collection & Columns

In order to get data into the store, you'll need to first create a `Collection` by calling `NewCollection()` method. Each collection requires a schema, which can be either specified manually by calling `CreateColumn()` multiple times or automatically inferred from an object by calling `CreateColumnsOf()` function. 

In the example below we're loading some `JSON` data by using `json.Unmarshal()` and auto-creating colums based on the first element on the loaded slice. After this is done, we can then load our data by inserting the objects one by one into the collection. This is accomplished by calling `Insert()` method on the collection itself repeatedly.

```go
data := loadFromJson("players.json")

// Create a new columnar collection
players := column.NewCollection()
players.CreateColumnsOf(data[0])

// Insert every item from our loaded data
for _, v := range data {
	players.Insert(v)
}
```

Now, let's say we only want specific columns to be added. We can do this by calling `CreateColumn()` method on the collection manually to create the required columns.

```go
// Create a new columnar collection with pre-defined columns
players := column.NewCollection()
players.CreateColumn("name", reflect.String)
players.CreateColumn("balance", reflect.Float64)
players.CreateColumn("age", reflect.Int8)

// Insert every item from our loaded data
for _, v := range loadFromJson("players.json") {
	players.Insert(v)
}
```

## Transaction Commit & Rollback

Transactions allow for isolation between two concurrent operations. In fact, all of the batch queries must go through a transaction in this library. The `Query` method requires a function which takes in a `column.Txn` pointer which contains various helper methods that support querying. In the example below we're trying to iterate over all of the players and update their balance by setting it to `10.0`. The `Query` method automatically calls `txn.Commit()` if the function returns without any error. On the flip side, if the provided function returns an error, the query will automatically call `txn.Rollback()` so none of the changes will be applied.

```go
// Range over all of the players and update (successfully their balance)
players.Query(func(txn *column.Txn) error {
	txn.Range(func(v column.Cursor) bool {
		v.Update(10.0) // Update the "balance" to 10.0
		return true
	}, "balance")

	// No error, txn.Commit() will be called
	return nil
})
```

Now, in this example, we try to update balance but a query callback returns an error, in which case none of the updates will be actually reflected in the underlying collection.

```go
// Range over all of the players and update (successfully their balance)
players.Query(func(txn *column.Txn) error {
	txn.Range(func(v column.Cursor) bool {
		v.Update(10.0) // Update the "balance" to 10.0
		return true
	}, "balance")

	// Returns an error, txn.Rollback() will be called
	return fmt.Errorf("bug") 
})
```

You can (but probablty won't need to) call `Commit()` or `Rollback()` manually, as many times as required. This could be handy to do partial updates but calling them too often will have a performance hit on your application.

```go
// Range over all of the players and update (successfully their balance)
players.Query(func(txn *column.Txn) error {
	txn.Range(func(v column.Cursor) bool {
		v.Update(10.0) // Update the "balance" to 10.0
		return true
	}, "balance")

	txn.Commit() // Manually commit all of the changes
	return nil   // This will call txn.Commit() again, but will be a no-op
})
```


## Benchmarks

The benchmarks below were ran on a collection of *500 items* containing a dozen columns. Feel free to explore the benchmarks but I strongly recommend testing it on your actual dataset.

```
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/insert-8         27589314       43.05 ns/op     1 B/op     0 allocs/op
BenchmarkCollection/fetch-8          21041593       56.84 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/count-slow-8       109107    11001 ns/op        0 B/op     0 allocs/op
BenchmarkCollection/count-8           9300270      128.6 ns/op      0 B/op     0 allocs/op
BenchmarkCollection/range-8           1871557      641.0 ns/op      0 B/op     0 allocs/op
BenchmarkCollection/select-8          1214799      975.8 ns/op      0 B/op     0 allocs/op
BenchmarkCollection/update-at-8      28573945       41.99 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/update-all-8       184694      6481 ns/op       0 B/op     0 allocs/op
BenchmarkCollection/delete-at-8       2613982      459.1 ns/op      0 B/op     0 allocs/op
BenchmarkCollection/delete-all-8       324321     3730 ns/op        0 B/op     0 allocs/op
```

## Contributing

We are open to contributions, feel free to submit a pull request and we'll review it as quickly as we can. This library is maintained by [Roman Atachiants](https://www.linkedin.com/in/atachiants/)

## License

Tile is licensed under the [MIT License](LICENSE.md).