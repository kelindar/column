# Columnar Collections & Querying

This package is my experimental attempt in building a fast, in-memory columnar collections in Go. The basic
idea is to arrange property bags (i.e. `map[string]interface{}`) into columns and be able to write queries effiently 
around them. Under the hood, this uses roaring bitmaps extensively to provide fast comparisons and selection.

## Features

 * Columnar (Structures of Arrays) data layout for very fast iteration over large sets of data
 * Zero heap allocation (or close to it) inside the library (see benchmarks below)
 * Querying capability with filtering (aka "where" clause) and projections (aka "select" clause)
 * Using dense and fast bitmaps for indexing and free/fill-lists

## Example usage

```go
// oldHumanMages returns a query which performs a full scan on 3 different columns and compares
// them given the specified predicates. This is not indexed.
func oldHumanMages(filter columnar.Query) {
	filter.
		WithString("race", "human").
		WithString("class", "mage").
		WithFilter("age", func(v interface{}) bool {
			return v.(float64) >= 30
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
	players := columnar.New()

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
	players.Find(oldHumanMagesIndexed, func(o Object) bool {
		fmt.Println(o["name"]) // outputs the name
		return true
	}, "name")
}
```

## Benchmarks

```
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/add-8              5004789    234.1 ns/op     82 B/op    0 allocs/op
BenchmarkCollection/fetch-to-8        92310531    12.28 ns/op      0 B/op    0 allocs/op
BenchmarkCollection/count-8            1653796    725.2 ns/op      0 B/op    0 allocs/op
BenchmarkCollection/count-indexed-8   23074526    51.51 ns/op      0 B/op    0 allocs/op
BenchmarkCollection/find-8             1207858    996.8 ns/op    336 B/op    2 allocs/op
BenchmarkCollection/find-indexed-8     3986691    303.9 ns/op    336 B/op    2 allocs/op
```