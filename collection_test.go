// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/stretchr/testify/assert"
)

// BenchmarkCollection/insert-8         	27327879	        42.93 ns/op	       1 B/op	       0 allocs/op
// BenchmarkCollection/fetch-8          	21438984	        55.84 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/count-slow-8     	  111388	     10835 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/count-8          	 9427291	       128.9 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/range-8          	 1870438	       649.2 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/select-8         	 1238320	       972.2 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/update-at-8      	27687408	        41.31 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/update-all-8     	  184694	      6481 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/delete-at-8      	 2583535	       463.5 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/delete-all-8     	  324331	      3712 ns/op	       0 B/op	       0 allocs/op
func BenchmarkCollection(b *testing.B) {
	players := loadPlayers()
	obj := Object{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	b.Run("insert", func(b *testing.B) {
		col := NewCollection()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col.Insert(obj)
			if col.Count() >= 1000 {
				col = NewCollection()
			}
		}
	})

	b.Run("fetch", func(b *testing.B) {
		name := ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			if s, ok := players.Fetch(20); ok {
				name = s.StringAt("name")
			}
		}
		assert.NotEmpty(b, name)
	})

	b.Run("count-slow", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				txn.WithString("race", func(v string) bool {
					return v == "human"
				}).WithString("class", func(v string) bool {
					return v == "mage"
				}).WithFloat("age", func(v float64) bool {
					return v >= 30
				}).Count()
				return nil
			})
		}
	})

	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				txn.With("human", "mage", "old").Count()
				return nil
			})
		}
	})

	b.Run("range", func(b *testing.B) {
		count, name := 0, ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				txn.With("human", "mage", "old").Range("name", func(v Cursor) bool {
					count++
					name = v.String()
					return true
				})
				return nil
			})
		}
		assert.NotEmpty(b, name)
	})

	b.Run("select", func(b *testing.B) {
		count, name := 0, ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				txn.With("human", "mage", "old").Select(func(v Selector) bool {
					count++
					name = v.StringAt("name")
					return true
				})
				return nil
			})
		}
		assert.NotEmpty(b, name)
	})

	b.Run("update-at", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.UpdateAt(20, "balance", 1.0)
		}
	})

	b.Run("update-all", func(b *testing.B) {
		var columns []string
		players.cols.RangeName(func(columnName string, c Column) {
			if _, ok := c.(numerical); ok {
				columns = append(columns, columnName)
			}
		})

		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			columnName := columns[n%len(columns)]
			players.Query(func(txn *Txn) error {
				txn.Range(columnName, func(v Cursor) bool {
					v.Update(1.0)
					return true
				})
				return nil
			})
		}
	})

	b.Run("delete-at", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.DeleteAt(20)
		}
	})

	b.Run("delete-all", func(b *testing.B) {
		var fill bitmap.Bitmap
		c := loadPlayers()  // Clone since we're deleting here
		c.fill.Clone(&fill) // Save the state

		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			fill.Clone(&c.fill) // Restore
			c.Query(func(txn *Txn) error {
				txn.Select(func(v Selector) bool {
					v.Delete()
					return true
				})
				return nil
			})
		}
	})
}

func TestCollection(t *testing.T) {
	obj := Object{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)
	idx := col.Insert(obj)

	// Create a coupe of indices
	assert.Error(t, col.CreateIndex("", "", nil))
	assert.NoError(t, col.CreateIndex("rich", "wallet", func(v interface{}) bool {
		return v.(float64) > 100
	}))

	{ // Find the object by its index
		v, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, "Roman", v.StringAt("name"))
	}

	{ // Remove the object
		col.DeleteAt(idx)
		_, ok := col.Fetch(idx)
		assert.False(t, ok)
	}

	{ // Add a new one, should replace
		idx := col.Insert(obj)
		v, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, "Roman", v.StringAt("name"))
	}

	{ // Update the wallet
		col.UpdateAt(0, "wallet", float64(1000))
		v, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, int64(1000), v.IntAt("wallet"))
		assert.Equal(t, true, v.BoolAt("rich"))
	}
}

// loadPlayers loads a list of players from the fixture
func loadPlayers() *Collection {
	out := NewCollection()

	// index on humans
	out.CreateIndex("human", "race", func(v interface{}) bool {
		return v == "human"
	})

	// index on dwarves
	out.CreateIndex("dwarf", "race", func(v interface{}) bool {
		return v == "dwarf"
	})

	// index on elves
	out.CreateIndex("elf", "race", func(v interface{}) bool {
		return v == "elf"
	})

	// index on orcs
	out.CreateIndex("orc", "race", func(v interface{}) bool {
		return v == "orc"
	})

	// index for mages
	out.CreateIndex("mage", "class", func(v interface{}) bool {
		return v == "mage"
	})

	// index for old
	out.CreateIndex("old", "age", func(v interface{}) bool {
		return v.(float64) >= 30
	})

	// Load the items into the collection
	players := loadFixture("players.json")
	out.CreateColumnsOf(players[0])
	for _, p := range players {
		out.Insert(p)
	}

	return out
}

// loadFixture loads a fixture by its name
func loadFixture(name string) []Object {
	b, err := os.ReadFile("fixtures/" + name)
	if err != nil {
		panic(err)
	}

	var data []Object
	if err := json.Unmarshal(b, &data); err != nil {
		panic(err)
	}

	return data
}
