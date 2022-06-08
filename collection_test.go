// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelindar/column/commit"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/insert-8         	    2797	    483532 ns/op	   24288 B/op	     500 allocs/op
BenchmarkCollection/select-at-8      	21007226	        58.94 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/scan-8           	    1872	    539394 ns/op	     110 B/op	       0 allocs/op
BenchmarkCollection/count-8          	  528410	      2395 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/range-8          	   24799	     44300 ns/op	       7 B/op	       0 allocs/op
BenchmarkCollection/sum-8            	   88164	     13431 ns/op	       2 B/op	       0 allocs/op
BenchmarkCollection/avg-8            	   31342	     37728 ns/op	       8 B/op	       0 allocs/op
BenchmarkCollection/max-8            	   34090	     37313 ns/op	       6 B/op	       0 allocs/op
BenchmarkCollection/update-at-8      	 6242148	       202.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/update-all-8     	    1062	    967519 ns/op	     238 B/op	       0 allocs/op
BenchmarkCollection/delete-at-8      	 7042724	       184.3 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/delete-all-8     	 2043902	       562.5 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkCollection(b *testing.B) {
	amount := 100000
	players := loadPlayers(amount)

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
					txn.InsertObject(p)
				}
				return nil
			})
		}
	})

	b.Run("select-at", func(b *testing.B) {
		name := ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.QueryAt(20, func(r Row) error {
				name, _ = r.Enum("name")
				return nil
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
				names := txn.Enum("name")
				txn.With("human", "mage", "old").Range(func(idx uint32) {
					count++
					name, _ = names.Get()
				})
				return nil
			})
		}
		assert.NotEmpty(b, name)
	})

	b.Run("sum", func(b *testing.B) {
		v := 0.0
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				v = txn.Float64("balance").Sum()
				return nil
			})
		}
		assert.NotEqual(b, float64(0), v)
	})

	b.Run("avg", func(b *testing.B) {
		v := 0.0
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				v = txn.With("human", "mage", "old").Float64("balance").Avg()
				return nil
			})
		}
		assert.NotEqual(b, float64(0), v)
	})

	b.Run("max", func(b *testing.B) {
		v := 0.0
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				v, _ = txn.With("human", "mage", "old").Float64("balance").Max()
				return nil
			})
		}
		assert.NotEqual(b, float64(0), v)
	})

	b.Run("update-at", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.QueryAt(20, func(r Row) error {
				r.SetFloat64("balance", 1.0)
				return nil
			})
		}
	})

	b.Run("update-all", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.Query(func(txn *Txn) error {
				balance := txn.Float64("balance")
				return txn.Range(func(idx uint32) {
					balance.Set(0.0)
				})
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
	idx := col.InsertObject(obj)
	assert.Equal(t, uint32(0), idx)
	assert.Equal(t, uint32(1), col.InsertObject(obj))

	// Should not drop, since it's not an index
	assert.Error(t, col.DropIndex("name"))

	// Create a couple of indexes
	assert.Error(t, col.CreateIndex("", "", nil))
	assert.NoError(t, col.CreateIndex("rich", "wallet", func(r Reader) bool {
		return r.Float() > 100
	}))

	{ // Find the object by its index
		assert.NoError(t, col.QueryAt(idx, func(r Row) error {
			name, ok := r.String("name")
			assert.True(t, ok)
			assert.Equal(t, "Roman", name)
			return nil
		}))
	}

	{ // Remove the object
		assert.True(t, col.DeleteAt(idx))
		assert.Error(t, col.QueryAt(idx, func(r Row) error {
			if _, ok := r.String("name"); !ok {
				return fmt.Errorf("unreachable")
			}

			return nil
		}))
	}

	{ // Add a new one, should replace
		newIdx := col.InsertObject(obj)
		assert.Equal(t, idx, newIdx)
		assert.NoError(t, col.QueryAt(newIdx, func(r Row) error {
			name, ok := r.String("name")
			assert.True(t, ok)
			assert.Equal(t, "Roman", name)
			return nil
		}))
	}

	{ // Update the wallet
		col.QueryAt(idx, func(r Row) error {
			r.SetFloat64("wallet", 1000)
			return nil
		})

		col.QueryAt(idx, func(r Row) error {
			wallet, ok := r.Float64("wallet")
			isRich := r.Bool("rich")

			assert.True(t, ok)
			assert.Equal(t, 1000.0, wallet)
			assert.True(t, isRich)
			return nil
		})

		assert.NoError(t, col.QueryAt(idx, func(r Row) error {
			wallet, _ := r.Float64("wallet")
			isRich := r.Bool("rich")

			assert.Equal(t, 1000.0, wallet)
			assert.True(t, isRich)
			return nil
		}))
	}
}

func TestDropColumn(t *testing.T) {
	obj := Object{
		"wallet": 5000,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)
	assert.NoError(t, col.CreateIndex("rich", "wallet", func(r Reader) bool {
		return r.Float() > 100
	}))

	assert.Equal(t, uint32(0), col.InsertObject(obj))
	assert.Equal(t, uint32(1), col.InsertObject(obj))

	col.DropColumn("rich")
	col.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("rich").Count())
		return nil
	})
}

func TestInsertObject(t *testing.T) {
	col := NewCollection()
	col.CreateColumn("name", ForString())
	col.InsertObject(Object{"name": "A"})
	col.InsertObject(Object{"name": "B"})

	assert.Equal(t, 2, col.Count())
	assert.NoError(t, col.QueryAt(0, func(r Row) error {
		name, ok := r.String("name")
		assert.True(t, ok)
		assert.Equal(t, "A", name)
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
	col.InsertObjectWithTTL(obj, time.Microsecond)
	col.Query(func(txn *Txn) error {
		expire := txn.Int64(expireColumn)
		return txn.Range(func(idx uint32) {
			value, _ := expire.Get()
			expireAt := time.Unix(0, value)
			expire.Set(expireAt.Add(1 * time.Microsecond).UnixNano())
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
	col.InsertObject(row)
	defer col.Close()

	// Create an index, add 1 more row
	assert.NoError(t, col.CreateIndex("young", "age", func(r Reader) bool {
		return r.Int() < 50
	}))
	col.InsertObject(row)

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
	col.InsertObject(row)
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
			col.InsertObject(obj)
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

func TestConcurrentPointReads(t *testing.T) {
	obj := Object{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)
	for i := 0; i < 1000; i++ {
		col.InsertObject(obj)
	}

	var ops int64
	var wg sync.WaitGroup
	wg.Add(2)

	// Reader
	go func() {
		for i := 0; i < 10000; i++ {
			col.QueryAt(99, func(r Row) error {
				_, _ = r.String("name")
				return nil
			})
			atomic.AddInt64(&ops, 1)
			runtime.Gosched()
		}
		wg.Done()
	}()

	// Writer
	go func() {
		for i := 0; i < 10000; i++ {
			col.QueryAt(99, func(r Row) error {
				r.SetString("name", "test")
				return nil
			})
			atomic.AddInt64(&ops, 1)
			runtime.Gosched()
		}
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, 20000, int(atomic.LoadInt64(&ops)))
}

func TestInsert(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	idx, err := c.Insert(func(r Row) error {
		r.SetString("name", "Roman")
		return nil
	})
	assert.Equal(t, uint32(0), idx)
	assert.NoError(t, err)
}

func TestInsertWithTTL(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	idx, err := c.InsertWithTTL(time.Hour, func(r Row) error {
		r.SetString("name", "Roman")
		return nil
	})
	assert.Equal(t, uint32(0), idx)
	assert.NoError(t, err)
	assert.NoError(t, c.QueryAt(idx, func(r Row) error {
		expire, ok := r.Int64(expireColumn)
		assert.True(t, ok)
		assert.NotZero(t, expire)
		return nil
	}))
}

func TestCreateColumnsOfInvalidKind(t *testing.T) {
	obj := map[string]interface{}{
		"name": complex64(1),
	}

	col := NewCollection()
	assert.Error(t, col.CreateColumnsOf(obj))
}

func TestCreateColumnsOfDuplicate(t *testing.T) {
	obj := map[string]interface{}{
		"name": "Roman",
	}

	col := NewCollection()
	assert.NoError(t, col.CreateColumnsOf(obj))
	assert.Error(t, col.CreateColumnsOf(obj))
}

func TestFindFreeIndex(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("name", ForString()))
	for i := 0; i < 100; i++ {
		idx, err := col.Insert(func(r Row) error {
			r.SetString("name", "Roman")
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, i, int(idx))
	}
}

func TestReplica(t *testing.T) {
	w := make(commit.Channel, 1024)
	source := NewCollection(Options{
		Writer: w,
	})
	source.CreateColumn("id", ForString())
	source.CreateColumn("cnt", ForInt())

	target := NewCollection()
	target.CreateColumn("id", ForString())
	target.CreateColumn("cnt", ForInt())

	go func() {
		for change := range w {
			target.Replay(change)
		}
	}()

	source.Insert(func (r Row) error {
		r.SetAny("id", "bob")
		r.SetInt("cnt", 2)
		return nil
	})

	// give the replica stream a moment
	time.Sleep(100 * time.Millisecond)

	target.Query(func (txn *Txn) error {
		assert.Equal(t, 1, txn.Count())
		return nil
	})
}

// --------------------------- Mocks & Fixtures ----------------------------

// loadPlayers loads a list of players from the fixture
func loadPlayers(amount int) *Collection {
	out := newEmpty(amount)

	// Load and copy until we reach the amount required
	data := loadFixture("players.json")
	for i := 0; i < amount/len(data); i++ {
		out.Query(func(txn *Txn) error {
			for _, p := range data {
				txn.InsertObject(p)
			}
			return nil
		})
	}
	return out
}

// newEmpty creates a new empty collection for a the fixture
func newEmpty(capacity int) *Collection {
	out := NewCollection(Options{
		Capacity: capacity,
		Vacuum:   500 * time.Millisecond,
		Writer:   new(noopWriter),
	})

	// Load the items into the collection
	out.CreateColumn("serial", ForKey())
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
