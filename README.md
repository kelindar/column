<p align="center">
<img width="330" height="110" src=".github/logo.png" border="0" alt="kelindar/column">
<br>
<img src="https://img.shields.io/github/go-mod/go-version/kelindar/column" alt="Go Version">
<a href="https://pkg.go.dev/github.com/kelindar/column"><img src="https://pkg.go.dev/badge/github.com/kelindar/column" alt="PkgGoDev"></a>
<a href="https://goreportcard.com/report/github.com/kelindar/column"><img src="https://goreportcard.com/badge/github.com/kelindar/column" alt="Go Report Card"></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License"></a>
<a href="https://coveralls.io/github/kelindar/column"><img src="https://coveralls.io/repos/github/kelindar/column/badge.svg" alt="Coverage"></a>
</p>

# Columnar In-Memory DB with Bitmap Indexing

This package contains a **hihg-performance, columnar, in-memory database** that supports fast querying and iteration with zero-allocations and bitmap indexing. 

The general idea is to leverage cache-friendly ways of organizing data in [structures of arrays (SoA)](https://en.wikipedia.org/wiki/AoS_and_SoA) otherwise known "columnar" storage in database design. This, in turn allows us to iterate and filter over columns very efficiently. On top of that, this package also adds [bitmap indexing](https://en.wikipedia.org/wiki/Bitmap_index) to the columnar storage, allowing to build filter queries using binary `and`, `and not`, `or` and `xor` (see [kelindar/bitmap](https://github.com/kelindar/bitmap) with SIMD support). 

## Features

 * Cache-friendly **columnar data layout** that minimizes cache-misses
 * **Zero heap allocation** (or close to it) inside the library (see benchmarks below)
 * Support for **SIMD-enabled filtering** (i.e. "where" clause) by leveraging [binary indexing](https://github.com/kelindar/bitmap)
 * Support for **columnar projection**  (i.e. "select" clause) for fast retrieval
 * Support for **computed indexes** that are dynamically calculated based on provided predicate

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
	players.Query(func(txn column.Txn) error {
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
	players.Query(func(txn column.Txn) error {
		println(txn.With("human", "mage", "old").Count()) // prints the count
		return nil
	})

	// Same condition as above, but we also select the actual names of those 
	// players and iterate through them.
	players.Query(func(txn column.Txn) error {
		txn.With("human", "mage", "old").Range(func(v column.Selector) bool {
			println(v.String()) // prints the name
			return true
		}, "name") // The column to select
		return nil
	})
}
```

## Benchmarks

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