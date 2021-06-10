# Columnar Collections & Querying


![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kelindar/column)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/kelindar/column)](https://pkg.go.dev/github.com/kelindar/column)
[![Go Report Card](https://goreportcard.com/badge/github.com/kelindar/column)](https://goreportcard.com/report/github.com/kelindar/column)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

This package is my experimental attempt in building a fast, in-memory columnar collections in Go. The basic
idea is to arrange property bags (i.e. `map[string]interface{}`) into columns and be able to write queries effiently 
around them. Under the hood, this uses SIMD-powered bitmaps extensively to provide fast comparisons and selection.

## Features

 * Columnar (Structures of Arrays) data layout for very fast iteration over large sets of data
 * Zero heap allocation (or close to it) inside the library (see benchmarks below)
 * Querying capability with filtering (aka "where" clause) and projections (aka "select" clause)
 * Using dense and fast bitmaps for indexing and free/fill-lists

## Example usage

```go
// oldHumanMages returns a query which performs a full scan on 3 different columns and compares
// them given the specified predicates. This is not indexed.
func oldHumanMages(filter column.Query) {
	filter.
		WithString("race", func(v string) bool {
			return v == "human"
		}).
		WithString("class", func(v string) bool {
			return v == "mage"
		}).
		WithFloat64("age", func(v float64) bool {
			return v >= 30
		})
}

// oldHumanMagesIndexed returns an indexed query which uses exlusively bitmap indexes, the result
// will be the same as the query above but the performance of the query is 10x-100x faster
// depending on the size of the underlying data.
func oldHumanMagesIndexed(filter columnar.Query) {
	filter.With("human").With("mage").With("old")
}

func main(){

	// Create a new columnar collection
	players := column.NewCollection()

	// index on humans
	players.Index("human", "race", func(v interface{}) bool {
		return v == "human"
	})

	// index for mages
	players.Index("mage", "class", func(v interface{}) bool {
		return v == "mage"
	})

	// index for old
	players.Index("old", "age", func(v interface{}) bool {
		return v.(float64) >= 30
	})

	// Load the items into the collection
	for _, v := range loadFixture("players.json") {
		players.Add(v)
	}

	// How many human mages over age of 30? First is unindexed (scan) while the
	// second query is indexed based on the predefined indices built above.
	count := players.Count(oldHumanMages)
	count := players.Count(oldHumanMagesIndexed)

	// Same condition as above, but we also select the actual names of those 
	// players and iterate through them
	players.Find(oldHumanMagesIndexed, func(row column.Selector) bool {
		println(row.String("name")) // prints the name
		return true
	})
}
```

## Benchmarks

```
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/add-8         29772168    47.09 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/count-8         169482     6731 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/count-idx-8   14232207    86.13 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/find-8          169244     7430 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/find-idx-8     1879239    626.3 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/find-one-8      236313     4803 ns/op     0 B/op     0 allocs/op
BenchmarkCollection/fetch-8       39630772    29.77 ns/op     0 B/op     0 allocs/op
```