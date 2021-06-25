// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/insert-8         	 5531546	       215.9 ns/op	      18 B/op	       0 allocs/op
BenchmarkCollection/fetch-8          	23749724	        44.97 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/scan-8           	     784	   1527506 ns/op	     160 B/op	       0 allocs/op
BenchmarkCollection/count-8          	 1000000	      1081 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/range-8          	   10000	    100833 ns/op	      14 B/op	       0 allocs/op
BenchmarkCollection/update-at-8      	 4225484	       281.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/update-all-8     	     830	   1388480 ns/op	  105714 B/op	       0 allocs/op
BenchmarkCollection/delete-at-8      	 6928646	       171.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/delete-all-8     	  180884	      6373 ns/op	       1 B/op	       0 allocs/op
*/
func BenchmarkCollection(b *testing.B) {
	amount := 100000
	players := loadPlayers(amount)
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

	b.Run("scan", func(b *testing.B) {
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

	b.Run("update-at", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.UpdateAt(20, "balance", 1.0)
		}
	})

	b.Run("update-all", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				txn.Range("balance", func(v Cursor) bool {
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
		temp := loadPlayers(amount)
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			temp.Query(func(txn *Txn) error {
				txn.DeleteAll()
				return nil
			})
		}
	})
}

// Test replication many times
func TestReplicate(t *testing.T) {
	for x := 0; x < 20; x++ {
		runReplication(t, 5000, 50)
	}
}

// runReplication runs a concurrent replication test
func runReplication(t *testing.T, updates, inserts int) {
	t.Run(fmt.Sprintf("replicate-%v-%v", updates, inserts), func(t *testing.T) {
		writer := make(commit.Channel, 1024)
		object := map[string]interface{}{
			"float64": float64(0),
			"int32":   int32(0),
			"string":  "",
		}

		// Create a primary
		primary := NewCollection(Options{
			Capacity: inserts,
			Writer:   &writer,
		})
		// Replica with the same schema
		replica := NewCollection(Options{
			Capacity: inserts,
		})

		// Create schemas and start streaming replication into the replica
		primary.CreateColumnsOf(object)
		replica.CreateColumnsOf(object)
		var done sync.WaitGroup
		go func() {
			done.Add(1)
			defer done.Done()
			for change := range writer {
				assert.NoError(t, replica.Replay(change))
			}
		}()

		// Write some objects
		for i := 0; i < inserts; i++ {
			primary.Insert(object)
		}

		// Random concurrent updates
		var wg sync.WaitGroup
		wg.Add(updates)
		for i := 0; i < updates; i++ {
			go func() {
				defer wg.Done()

				// Randomly update a column
				offset := uint32(rand.Int31n(int32(inserts - 1)))
				switch rand.Int31n(3) {
				case 0:
					primary.UpdateAt(offset, "float64", math.Round(rand.Float64()*1000)/100)
				case 1:
					primary.UpdateAt(offset, "int32", rand.Int31n(100000))
				case 2:
					primary.UpdateAt(offset, "string", fmt.Sprintf("hi %v", rand.Int31n(100)))
				}

				// Randomly delete an item
				if rand.Int31n(100) == 0 {
					primary.DeleteAt(uint32(rand.Int31n(int32(inserts - 1))))
				}

				// Randomly insert an item
				if rand.Int31n(100) == 0 {
					primary.Insert(object)
				}
			}()
		}

		// Replay all of the changes into the replica
		wg.Wait()
		close(writer)
		done.Wait()

		// Check if replica and primary are the same
		assert.Equal(t, primary.Count(), replica.Count())
		primary.Query(func(txn *Txn) error {
			return txn.Range("float64", func(v Cursor) bool {
				v1, v2 := v.FloatAt("float64"), v.IntAt("int32")
				if v1 != 0 {
					clone, _ := replica.Fetch(v.idx)
					assert.Equal(t, v.FloatAt("float64"), clone.FloatAt("float64"))
				}

				if v2 != 0 {
					clone, _ := replica.Fetch(v.idx)
					assert.Equal(t, v.IntAt("int32"), clone.IntAt("int32"))
				}
				return true
			})
		})
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

	// Should not drop, since it's not an index
	col.DropIndex("name")

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

	{ // Drop the colun
		col.DropColumn("rich")
		col.Query(func(txn *Txn) error {
			assert.Equal(t, 0, txn.With("rich").Count())
			return nil
		})
	}
}

func TestExpire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	obj := Object{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)
	defer col.Close()

	// Insert an object
	col.InsertWithTTL(obj, time.Microsecond)
	col.Query(func(txn *Txn) error {
		return txn.Range(expireColumn, func(v Cursor) bool {
			expireAt := time.Unix(0, v.Int())
			v.Update(expireAt.Add(1 * time.Microsecond).UnixNano())
			return true
		})
	})
	assert.Equal(t, 1, col.Count())

	// Perform a cleanup every microsecond for tests
	go col.vacuum(ctx, time.Microsecond)

	// Wait a bit, should be cleaned up
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, col.Count())
}

func TestInsertParallel(t *testing.T) {
	obj := Object{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			col.Insert(obj)
			wg.Done()
		}()
	}

	wg.Wait()
	assert.Equal(t, 500, col.Count())
}

// loadPlayers loads a list of players from the fixture
func loadPlayers(amount int) *Collection {
	out := NewCollection(Options{
		Capacity: amount,
		Vacuum:   500 * time.Millisecond,
		Writer:   new(noopWriter),
	})

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
	out.CreateColumn("serial", ForAny())
	out.CreateColumn("name", ForAny())
	out.CreateColumn("active", ForBool())
	out.CreateColumn("class", ForEnum())
	out.CreateColumn("race", ForEnum())
	out.CreateColumn("age", ForFloat64())
	out.CreateColumn("hp", ForFloat64())
	out.CreateColumn("mp", ForFloat64())
	out.CreateColumn("balance", ForFloat64())
	out.CreateColumn("gender", ForEnum())
	out.CreateColumn("guild", ForEnum())
	out.CreateColumn("location", ForAny())

	// Load and copy until we reach the amount required
	data := loadFixture("players.json")
	for i := 0; i < amount/len(data); i++ {
		out.Query(func(txn *Txn) error {
			for _, p := range data {
				txn.Insert(p)
			}
			return nil
		})
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

// noopWriter is a writer that simply counts the commits
type noopWriter struct {
	commits uint64
}

// Write clones the commit and writes it into the writer
func (w *noopWriter) Write(commit commit.Commit) error {
	atomic.AddUint64(&w.commits, 1)
	return nil
}
