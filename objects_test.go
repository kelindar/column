package columnar

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// BenchmarkCollection/add-8         	 5681844	       212.8 ns/op	      82 B/op	       0 allocs/op
// BenchmarkCollection/fetch-to-8    	99759745	        12.28 ns/op	       0 B/op	       0 allocs/op
// BenchmarkCollection/where-8       	 2371153	       506.1 ns/op	      40 B/op	       2 allocs/op
// BenchmarkCollection/map-iterate-8 	 1473567	       813.6 ns/op	       0 B/op	       0 allocs/op
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
			if col.Count() >= 1000 {
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

	b.Run("where", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.
				Where("age", func(v interface{}) bool {
					return v.(float64) >= 30
				}).
				AndValue("race", "human").
				AndValue("class", "mage").
				Count()
		}
	})

	b.Run("map-iterate", func(b *testing.B) {
		col := loadFixture("players.json")
		count := 0
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			for _, v := range col {
				if v["age"].(float64) >= 30 && v["race"] == "human" && v["class"] == "mage" {
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
		assert.Equal(t, 1, len(col.props["name"].data))
		assert.True(t, ok)
		assert.Equal(t, "Roman", obj["name"])
	}
}

// loadPlayers loads a list of players from the fixture
func loadPlayers() *Collection {
	players := loadFixture("players.json")
	out := New()
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

	/*out := make([]Object, 0, 10000*len(data))
	for i := 0; i < 10000; i++ {
		out = append(out, data...)
	}*/
	return data
}
