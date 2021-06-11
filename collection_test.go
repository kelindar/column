// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkCollection/add-8         	21583004	        54.86 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/count-8       	  179092	      6761 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/count-idx-8   	14948353	        81.69 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/find-8        	  166666	      7259 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/find-idx-8    	 1774310	       618.6 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/fetch-8       	43480938	        28.11 ns/op	       0 B/op	       0 allocs/op
func BenchmarkCollection(b *testing.B) {
	players := loadPlayers()
	obj := Object{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	b.Run("add", func(b *testing.B) {
		col := NewCollection()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col.Add(obj)
			if col.Count() >= 1000 {
				col = NewCollection()
			}
		}
	})

	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.View(func(txn Txn) error {
				txn.WithString("race", func(v string) bool {
					return v == "human"
				}).WithString("class", func(v string) bool {
					return v == "mage"
				}).WithFloat64("age", func(v float64) bool {
					return v >= 30
				}).Count()
				return nil
			})
		}
	})

	b.Run("count-idx", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.View(func(txn Txn) error {
				txn.With("human", "mage", "old").Count()
				return nil
			})
		}
	})

	b.Run("find", func(b *testing.B) {
		count, name := 0, ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.View(func(txn Txn) error {
				txn.WithString("race", func(v string) bool {
					return v == "human"
				}).WithString("class", func(v string) bool {
					return v == "mage"
				}).WithFloat64("age", func(v float64) bool {
					return v >= 30
				}).Range(func(v Selector) bool {
					count++
					name = v.String("name")
					return true
				})
				return nil
			})
		}
		assert.NotEmpty(b, name)
	})

	b.Run("find-idx", func(b *testing.B) {
		count, name := 0, ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.View(func(txn Txn) error {
				txn.With("human", "mage", "old").Range(func(v Selector) bool {
					count++
					name = v.String("name")
					return true
				})
				return nil
			})
		}
		assert.NotEmpty(b, name)
	})

	b.Run("fetch", func(b *testing.B) {
		name := ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			if s, ok := players.Fetch(20); ok {
				name = s.String("name")
			}
		}
		assert.NotEmpty(b, name)
	})
}

// BenchmarkFlatMap/count-map-8         	   62560	     18912 ns/op	       0 B/op	       0 allocs/op
func BenchmarkFlatMap(b *testing.B) {
	players := loadFixture("players.json")

	b.Run("count-map", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			count := 0
			for _, p := range players {
				if p["race"] == "human" && p["class"] == "mage" && p["age"].(float64) >= 30 {
					count++
				}
			}
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
	assert.Error(t, col.CreateIndex("", "", nil))

	col.AddColumnsOf(obj)
	idx := col.Add(obj)

	{ // Find the object by its index
		v, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, "Roman", v.String("name"))
	}

	{ // Remove the object
		col.Remove(idx)
		_, ok := col.Fetch(idx)
		assert.False(t, ok)
	}

	{ // Add a new one, should replace
		idx := col.Add(obj)
		v, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, "Roman", v.String("name"))
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
	out.AddColumnsOf(players[0])
	for _, p := range players {
		out.Add(p)
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
