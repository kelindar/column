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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/async"
	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/insert-8         	    2167	    578821 ns/op	    1223 B/op	       1 allocs/op
BenchmarkCollection/select-at-8      	42703713	        27.72 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/scan-8           	    2032	    598751 ns/op	      49 B/op	       0 allocs/op
BenchmarkCollection/count-8          	  800036	      1498 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/range-8          	   16833	     70556 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/update-at-8      	 3689354	       323.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/update-all-8     	    1198	   1003934 ns/op	    4004 B/op	       0 allocs/op
BenchmarkCollection/delete-at-8      	 8071692	       145.7 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/delete-all-8     	 2328974	       494.7 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkCollection(b *testing.B) {
	b.Run("insert", func(b *testing.B) {
		temp := loadPlayers(500)
		data := loadFixture("players.json")
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			temp.Query(func(txn *Txn) error {
				txn.DeleteAll()
				return nil
			})
			b.StartTimer()

			temp.Query(func(txn *Txn) error {
				for _, p := range data {
					txn.Insert(p)
				}
				return nil
			})
		}
	})

	amount := 100000
	players := loadPlayers(amount)
	b.Run("select-at", func(b *testing.B) {
		name := ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.SelectAt(20, func(v Selector) {
				name = v.StringAt("name")
			})
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
				txn.With("human", "mage", "old").Range("name", func(v Cursor) {
					count++
					name = v.String()
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
			players.UpdateAt(20, "balance", func(v Cursor) error {
				v.Set(1.0)
				return nil
			})
		}
	})

	b.Run("update-all", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				txn.Range("balance", func(v Cursor) {
					v.SetFloat64(0.0)
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
		rand.Seed(int64(x))
		runReplication(t, 10000, 50, runtime.NumCPU())
	}
}

// runReplication runs a concurrent replication test
func runReplication(t *testing.T, updates, inserts, concurrency int) {
	t.Run(fmt.Sprintf("replicate-%v-%v", updates, inserts), func(t *testing.T) {
		writer := make(commit.Channel, 10)
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
		done.Add(1)
		go func() {
			defer done.Done() // Drained
			for change := range writer {
				assert.NoError(t, replica.Replay(change))
			}
		}()

		// Write some objects
		for i := 0; i < inserts; i++ {
			primary.Insert(object)
		}

		work := make(chan async.Task)
		pool := async.Consume(context.Background(), 50, work)
		defer pool.Cancel()

		// Random concurrent updates
		var wg sync.WaitGroup
		wg.Add(updates)
		for i := 0; i < updates; i++ {
			work <- async.NewTask(func(ctx context.Context) (interface{}, error) {
				defer wg.Done()

				// Randomly update a column
				offset := uint32(rand.Int31n(int32(inserts - 1)))
				primary.UpdateAt(offset, "float64", func(v Cursor) error {
					switch rand.Int31n(3) {
					case 0:
						v.SetFloat64(math.Round(rand.Float64()*1000) / 100)
					case 1:
						v.SetInt32At("int32", rand.Int31n(100000))
					case 2:
						v.SetStringAt("string", fmt.Sprintf("hi %v", rand.Int31n(10)))
					}
					return nil
				})

				// Randomly delete an item
				if rand.Int31n(5) == 0 {
					primary.DeleteAt(uint32(rand.Int31n(int32(inserts - 1))))
				}

				// Randomly insert an item
				if rand.Int31n(5) == 0 {
					primary.Insert(object)
				}
				return nil, nil
			})
		}

		// Replay all of the changes into the replica
		wg.Wait()
		close(writer)
		done.Wait()

		// Check if replica and primary are the same
		if !assert.Equal(t, primary.Count(), replica.Count(), "replica and primary should be the same size") {
			return
		}

		primary.Query(func(txn *Txn) error {
			return txn.Range("float64", func(v Cursor) {
				v1, v2 := v.FloatAt("float64"), v.IntAt("int32")
				if v1 != 0 {
					clone, ok := txn.ReadAt(v.idx)
					assert.True(t, ok)
					assert.Equal(t, v.FloatAt("float64"), clone.FloatAt("float64"))
				}

				if v2 != 0 {
					clone, ok := txn.ReadAt(v.idx)
					assert.True(t, ok)
					assert.Equal(t, v.IntAt("int32"), clone.IntAt("int32"))
				}
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
	assert.NoError(t, col.CreateIndex("rich", "wallet", func(r Reader) bool {
		return r.Float() > 100
	}))

	{ // Find the object by its index
		assert.True(t, col.SelectAt(idx, func(v Selector) {
			assert.Equal(t, "Roman", v.StringAt("name"))
		}))
	}

	{ // Remove the object
		col.DeleteAt(idx)
		assert.False(t, col.SelectAt(idx, func(v Selector) {
			assert.Fail(t, "unreachable")
		}))
	}

	{ // Add a new one, should replace
		idx := col.Insert(obj)
		assert.True(t, col.SelectAt(idx, func(v Selector) {
			assert.Equal(t, "Roman", v.StringAt("name"))
		}))
	}

	{ // Update the wallet
		col.UpdateAt(idx, "wallet", func(v Cursor) error {
			v.SetFloat64(1000)
			return nil
		})
		assert.True(t, col.SelectAt(idx, func(v Selector) {
			assert.Equal(t, int64(1000), v.IntAt("wallet"))
			assert.Equal(t, true, v.BoolAt("rich"))
		}))
	}

	{ // Drop the colun
		col.DropColumn("rich")
		col.Query(func(txn *Txn) error {
			assert.Equal(t, 0, txn.With("rich").Count())
			return nil
		})
	}
}

func TestInsertObject(t *testing.T) {
	col := NewCollection()
	col.CreateColumn("name", ForString())
	col.Insert(Object{"name": "A"})
	col.Insert(Object{"name": "B"})

	assert.Equal(t, 2, col.Count())
	assert.NoError(t, col.Query(func(txn *Txn) error {
		selector, ok := txn.ReadAt(0)
		assert.True(t, ok)
		assert.Equal(t, "A", selector.StringAt("name"))
		return nil
	}))
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
		return txn.Range(expireColumn, func(v Cursor) {
			expireAt := time.Unix(0, int64(v.Int()))
			v.SetInt64(expireAt.Add(1 * time.Microsecond).UnixNano())
		})
	})
	assert.Equal(t, 1, col.Count())

	// Perform a cleanup every microsecond for tests
	go col.vacuum(ctx, time.Microsecond)

	// Wait a bit, should be cleaned up
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, col.Count())
}

func TestCreateIndex(t *testing.T) {
	row := Object{
		"age": 35,
	}

	// Create a collection with 1 row
	col := NewCollection()
	col.CreateColumnsOf(row)
	col.Insert(row)
	defer col.Close()

	// Create an index, add 1 more row
	assert.NoError(t, col.CreateIndex("young", "age", func(r Reader) bool {
		return r.Int() < 50
	}))
	col.Insert(row)

	// We now should have 2 rows in the index
	col.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.With("young").Count())
		return nil
	})
}

func TestCreateIndexInvalidColumn(t *testing.T) {
	col := NewCollection()
	defer col.Close()

	assert.Error(t, col.CreateIndex("young", "invalid", func(r Reader) bool {
		return r.Int() < 50
	}))
}

func TestDropIndex(t *testing.T) {
	row := Object{
		"age": 35,
	}

	// Create a collection with 1 row
	col := NewCollection()
	col.CreateColumnsOf(row)
	col.Insert(row)
	defer col.Close()

	// Create an index
	assert.NoError(t, col.CreateIndex("young", "age", func(r Reader) bool {
		return r.Int() < 50
	}))

	// Drop it, should be successful
	assert.NoError(t, col.DropIndex("young"))
}

func TestDropInvalidIndex(t *testing.T) {
	col := NewCollection()
	defer col.Close()
	assert.Error(t, col.DropIndex("young"))
}

func TestDropColumnNotIndex(t *testing.T) {
	col := NewCollection()
	col.CreateColumn("age", ForInt())
	defer col.Close()
	assert.Error(t, col.DropIndex("age"))
}

func TestDropOneOfMultipleIndices(t *testing.T) {
	col := NewCollection()
	col.CreateColumn("age", ForInt())
	defer col.Close()

	// Create a couple of indices
	assert.NoError(t, col.CreateIndex("young", "age", func(r Reader) bool {
		return r.Int() < 50
	}))
	assert.NoError(t, col.CreateIndex("old", "age", func(r Reader) bool {
		return r.Int() >= 50
	}))

	// Drop one of them
	assert.NoError(t, col.DropIndex("old"))
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
	wg.Add(500)
	for i := 0; i < 500; i++ {
		go func() {
			col.Insert(obj)
			wg.Done()
		}()
	}

	wg.Wait()
	assert.Equal(t, 500, col.Count())
	assert.NoError(t, col.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.Count())
		return nil
	}))
}

// loadPlayers loads a list of players from the fixture
func loadPlayers(amount int) *Collection {
	out := NewCollection(Options{
		Capacity: amount,
		Vacuum:   500 * time.Millisecond,
		Writer:   new(noopWriter),
	})

	// Load the items into the collection
	out.CreateColumn("serial", ForEnum())
	out.CreateColumn("name", ForEnum())
	out.CreateColumn("active", ForBool())
	out.CreateColumn("class", ForEnum())
	out.CreateColumn("race", ForEnum())
	out.CreateColumn("age", ForFloat64())
	out.CreateColumn("hp", ForFloat64())
	out.CreateColumn("mp", ForFloat64())
	out.CreateColumn("balance", ForFloat64())
	out.CreateColumn("gender", ForEnum())
	out.CreateColumn("guild", ForEnum())
	//out.CreateColumn("location", ForString())

	// index on humans
	out.CreateIndex("human", "race", func(r Reader) bool {
		return r.String() == "human"
	})

	// index on dwarves
	out.CreateIndex("dwarf", "race", func(r Reader) bool {
		return r.String() == "dwarf"
	})

	// index on elves
	out.CreateIndex("elf", "race", func(r Reader) bool {
		return r.String() == "elf"
	})

	// index on orcs
	out.CreateIndex("orc", "race", func(r Reader) bool {
		return r.String() == "orc"
	})

	// index for mages
	out.CreateIndex("mage", "class", func(r Reader) bool {
		return r.String() == "mage"
	})

	// index for old
	out.CreateIndex("old", "age", func(r Reader) bool {
		return r.Float() >= 30
	})

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
