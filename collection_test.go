// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkCollection/add-8         	25438010	        47.00 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/fetch-to-8    	 3174392	       373.9 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/count-8       	  175803	      6621 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/count-idx-8   	13089733	        85.11 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/find-8        	  169615	      7508 ns/op	     336 B/op	       2 allocs/op
// BenchmarkCollection/find-idx-8    	 1257127	       952.0 ns/op	     336 B/op	       2 allocs/op
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
		col := New()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col.Add(obj)
			if col.Count(nil) >= 1000 {
				col = New()
			}
		}
	})

	b.Run("fetch-to", func(b *testing.B) {
		dst := make(Object, 8)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.FetchTo(20, &dst)
		}
	})

	b.Run("count", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Count(oldHumanMages)
		}
	})

	b.Run("count-idx", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Count(oldHumanMagesIndexed)
		}
	})

	b.Run("find", func(b *testing.B) {
		count := 0
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Find(oldHumanMages, func(o Object) bool {
				count++
				return true
			}, "name")
		}
	})

	b.Run("find-idx", func(b *testing.B) {
		count := 0
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Find(oldHumanMagesIndexed, func(o Object) bool {
				count++
				return true
			}, "name")
		}
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

	col := New()
	col.AddColumnsOf(obj)
	idx := col.Add(obj)

	{ // Find the object by its index
		obj, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, "Roman", obj["name"])
	}

	{ // Remove the object
		col.Remove(idx)
		obj, ok := col.Fetch(idx)
		assert.False(t, ok)
		assert.Nil(t, obj)
	}

	{ // Add a new one, should replace
		idx := col.Add(obj)
		obj, ok := col.Fetch(idx)
		assert.True(t, ok)
		assert.Equal(t, "Roman", obj["name"])
	}
}

// loadPlayers loads a list of players from the fixture
func loadPlayers() *Collection {
	out := New()

	// index on humans
	out.AddIndex("human", "race", func(v interface{}) bool {
		return v == "human"
	})

	// index on dwarves
	out.AddIndex("dwarf", "race", func(v interface{}) bool {
		return v == "dwarf"
	})

	// index on elves
	out.AddIndex("elf", "race", func(v interface{}) bool {
		return v == "elf"
	})

	// index on orcs
	out.AddIndex("orc", "race", func(v interface{}) bool {
		return v == "orc"
	})

	// index for mages
	out.AddIndex("mage", "class", func(v interface{}) bool {
		return v == "mage"
	})

	// index for old
	out.AddIndex("old", "age", func(v interface{}) bool {
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
