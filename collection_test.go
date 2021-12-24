// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/insert-8                2174            534746 ns/op           25090 B/op        500 allocs/op
BenchmarkCollection/select-at-8         42206409                28.19 ns/op            0 B/op          0 allocs/op
BenchmarkCollection/scan-8                  2116            581193 ns/op            1872 B/op          0 allocs/op
BenchmarkCollection/count-8               748689              1565 ns/op               5 B/op          0 allocs/op
BenchmarkCollection/range-8                16476             73244 ns/op             216 B/op          0 allocs/op
BenchmarkCollection/update-at-8          3717255               316.6 ns/op             1 B/op          0 allocs/op
BenchmarkCollection/update-all-8            1176           1005992 ns/op            7134 B/op          1 allocs/op
BenchmarkCollection/delete-at-8          8403426               145.0 ns/op             0 B/op          0 allocs/op
BenchmarkCollection/delete-all-8         2338410               500.0 ns/op             1 B/op          0 allocs/op
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
					txn.InsertObject(p)
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

	// Should not drop, since it's not an index
	col.DropIndex("name")

	// Create a couple of indexes
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
		idx := col.InsertObject(obj)
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
	col.InsertObject(Object{"name": "A"})
	col.InsertObject(Object{"name": "B"})

	assert.Equal(t, 2, col.Count())
	assert.NoError(t, col.Query(func(txn *Txn) error {
		assert.True(t, txn.SelectAt(0, func(v Selector) {
			assert.Equal(t, "A", v.StringAt("name"))
		}))
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
			col.SelectAt(99, func(v Selector) {
				_ = v.StringAt("name")
			})
			atomic.AddInt64(&ops, 1)
			runtime.Gosched()
		}
		wg.Done()
	}()

	// Writer
	go func() {
		for i := 0; i < 10000; i++ {
			col.UpdateAt(99, "name", func(v Cursor) error {
				v.SetString("test")
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

	idx, err := c.Insert("name", func(v Cursor) error {
		v.Set("Roman")
		return nil
	})
	assert.Equal(t, uint32(0), idx)
	assert.NoError(t, err)
}

func TestInsertWithTTL(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForString())

	idx, err := c.InsertWithTTL("name", time.Hour, func(v Cursor) error {
		v.Set("Roman")
		return nil
	})
	assert.Equal(t, uint32(0), idx)
	assert.NoError(t, err)

	c.SelectAt(idx, func(v Selector) {
		assert.NotZero(t, v.IntAt(expireColumn))
	})
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
