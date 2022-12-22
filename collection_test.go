// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"strconv"

	"github.com/kelindar/column/commit"
	"github.com/kelindar/column/fixtures"
	"github.com/kelindar/xxrand"
	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkCollection/select-at-8      	18796579	        56.27 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/scan-8           	    2220	    614501 ns/op	     114 B/op	       0 allocs/op
BenchmarkCollection/count-8          	  499893	      2414 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/range-8          	   25044	     47727 ns/op	       5 B/op	       0 allocs/op
BenchmarkCollection/sum-8            	   84232	     14045 ns/op	       2 B/op	       0 allocs/op
BenchmarkCollection/avg-8            	   41404	     30238 ns/op	       3 B/op	       0 allocs/op
BenchmarkCollection/max-8            	   41194	     28929 ns/op	       6 B/op	       0 allocs/op
BenchmarkCollection/update-at-8      	 5999748	       199.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/update-all-8     	    1269	    947274 ns/op	    4179 B/op	       0 allocs/op
BenchmarkCollection/delete-at-8      	 7177303	       184.5 ns/op	       0 B/op	       0 allocs/op
BenchmarkCollection/delete-all-8     	 2063108	       614.8 ns/op	       0 B/op	       0 allocs/op
*/
func BenchmarkCollection(b *testing.B) {
	amount := 100000
	players := loadPlayers(amount)

	b.Run("select-at", func(b *testing.B) {
		name := ""
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			players.QueryAt(20, func(r Row) error {
				name, _ = r.String("name")
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
				names := txn.String("name")
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

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkRecord/get-8         	 6342373	       172.3 ns/op	      24 B/op	       1 allocs/op
BenchmarkRecord/set-8         	 4228219	       275.3 ns/op	      32 B/op	       2 allocs/op
BenchmarkRecord/merge-8       	 2673375	       443.7 ns/op	      32 B/op	       2 allocs/op
*/
func BenchmarkRecord(b *testing.B) {
	const amount = 100000

	// Create a test collection for records
	newCollection := func() *Collection {
		col := NewCollection()
		col.CreateColumn("ts", ForRecord(func() *time.Time {
			return new(time.Time)
		}))

		for i := 0; i < amount; i++ {
			col.Insert(func(r Row) error {
				now := time.Unix(1667745766, 0)
				r.SetRecord("ts", &now)
				return nil
			})
		}
		return col
	}

	// Decodes records at random indices
	b.Run("get", func(b *testing.B) {
		col := newCollection()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col.QueryAt(xxrand.Uint32n(amount), func(r Row) error {
				r.Record("ts")
				return nil
			})
		}
	})

	// Merges records at random indices
	b.Run("set", func(b *testing.B) {
		col := newCollection()
		now := time.Unix(1667745766, 0)

		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col.QueryAt(xxrand.Uint32n(amount), func(r Row) error {
				r.SetRecord("ts", &now)
				return nil
			})
		}
	})

	// Merges records at random indices
	b.Run("merge", func(b *testing.B) {
		col := newCollection()
		now := time.Unix(1667745766, 0)

		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col.QueryAt(xxrand.Uint32n(amount), func(r Row) error {
				r.MergeRecord("ts", &now)
				return nil
			})
		}
	})
}

func TestCollection(t *testing.T) {
	obj := map[string]any{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)

	// Insert first row
	idx, err := col.Insert(func(r Row) error {
		return r.SetMany(obj)
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), idx)

	// Insert second row
	idx, err = col.Insert(func(r Row) error {
		return r.SetMany(obj)
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), idx)

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
		newIdx, err := col.Insert(func(r Row) error {
			return r.SetMany(obj)
		})

		assert.NoError(t, err)
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
	obj := map[string]any{
		"wallet": 5000,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)
	assert.NoError(t, col.CreateIndex("rich", "wallet", func(r Reader) bool {
		return r.Float() > 100
	}))

	for i := 0; i < 2; i++ {
		idx, err := col.Insert(func(r Row) error {
			return r.SetMany(obj)
		})
		assert.NoError(t, err)
		assert.Equal(t, uint32(i), idx)
	}

	col.DropColumn("rich")
	col.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("rich").Count())
		return nil
	})
}

func TestInsertMany(t *testing.T) {
	col := NewCollection()
	col.CreateColumn("name", ForString())
	col.Insert(func(r Row) error {
		return r.SetMany(map[string]any{"name": "A"})
	})
	col.Insert(func(r Row) error {
		return r.SetMany(map[string]any{"name": "B"})
	})

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
	obj := map[string]any{
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
	col.Insert(func(r Row) error {
		r.SetTTL(time.Microsecond)
		return r.SetMany(obj)
	})

	col.Query(func(txn *Txn) error {
		ttl := txn.TTL()
		return txn.Range(func(idx uint32) {
			remaining, ok := ttl.TTL()
			assert.True(t, ok)
			assert.NotZero(t, remaining)
		})
	})
	assert.Equal(t, 1, col.Count())

	// Perform a cleanup every microsecond for tests
	go col.vacuum(ctx, time.Microsecond)

	// Wait a bit, should be cleaned up
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, col.Count())
}

func TestExpireExtend(t *testing.T) {
	col := loadPlayers(500)
	assert.NoError(t, col.Query(func(txn *Txn) error {
		ttl := txn.TTL()
		return txn.Range(func(idx uint32) {

			// When loaded, we should n ot have any expiration set
			_, hasExpiration := ttl.ExpiresAt()
			assert.False(t, hasExpiration)
			_, hasRemaining := ttl.TTL()
			assert.False(t, hasRemaining)

			// Extend by 2 hours
			ttl.Set(time.Hour)
			ttl.Extend(time.Hour)
		})
	}))

	// Now we should have expiration time set
	assert.NoError(t, col.Query(func(txn *Txn) error {
		ttl := txn.TTL()
		return txn.Range(func(idx uint32) {
			_, hasExpiration := ttl.ExpiresAt()
			assert.True(t, hasExpiration)
			_, hasRemaining := ttl.TTL()
			assert.True(t, hasRemaining)
		})
	}))

	// Reset back to zero
	assert.NoError(t, col.Query(func(txn *Txn) error {
		ttl := txn.TTL()
		return txn.Range(func(idx uint32) {
			ttl.Set(0) // Reset to zero
		})
	}))
}

func TestCreateIndex(t *testing.T) {
	row := map[string]any{
		"age": 35,
	}

	// Create a collection with 1 row
	col := NewCollection()
	defer col.Close()

	col.CreateColumnsOf(row)
	col.Insert(func(r Row) error {
		return r.SetMany(row)
	})

	// Create an index, add 1 more row
	assert.NoError(t, col.CreateIndex("young", "age", func(r Reader) bool {
		return r.Int() < 50
	}))
	col.Insert(func(r Row) error {
		return r.SetMany(row)
	})

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
	row := map[string]any{
		"age": 35,
	}

	// Create a collection with 1 row
	col := NewCollection()
	defer col.Close()

	col.CreateColumnsOf(row)
	col.Insert(func(r Row) error {
		return r.SetMany(row)
	})

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
	obj := map[string]any{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)

	var wg sync.WaitGroup
	wg.Add(500)
	for i := 0; i < 500; i++ {
		go func() {
			_, err := col.Insert(func(r Row) error {
				return r.SetMany(obj)
			})
			assert.NoError(t, err)
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

func BenchmarkParallelSort(b *testing.B) {
	getobj := func (n string) map[string]any {
		return map[string]any{
			"name":   n,
			"age":    35,
			"wallet": 50.99,
			"health": 100,
			"mana":   200,
		}
	}

	b.Run("in-asc", func(b *testing.B) { 
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			col := NewCollection()
			col.CreateColumnsOf(getobj("n"))
			col.CreateSortIndex("sorted_name", "name")
			var wg sync.WaitGroup
			wg.Add(20)
			for i := 0; i < 20; i++ {
				go func(ii int) {
					for x := 0; x < 5000; x++ {
						tobj := getobj("n")
						tobj["name"] = strconv.Itoa((ii*20)+x)
						col.Insert(func(r Row) error {
							return r.SetMany(tobj)
						})
					}
					wg.Done()
				}(i)
				go func(ii int) {
					for y := 0; y < 5; y++ {
						col.Query(func(txn *Txn) error {
							health := txn.Int("health")
							return txn.Ascend("sorted_name", func (i uint32) {
								health.Set((ii*20)+y)
							})
						})
					}
				}(i)
			}
			wg.Wait()
		}
	})
}

func TestParallelSort(t *testing.T) {
	getobj := func (n string) map[string]any {
		return map[string]any{
			"name":   n,
			"age":    35,
			"wallet": 50.99,
			"health": 100,
			"mana":   200,
		}
	}

	col := NewCollection()
	col.CreateColumnsOf(getobj("n"))
	col.CreateSortIndex("sorted_name", "name")

	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(ii int) {
			for x := 0; x < 5000; x++ {
				tobj := getobj("n")
				tobj["name"] = strconv.Itoa((ii*20)+x)
				col.Insert(func(r Row) error {
					return r.SetMany(tobj)
				})
			}
			wg.Done()
		}(i)
		go func(ii int) {
			col.Query(func(txn *Txn) error {
				health := txn.Int("health")
				return txn.Ascend("sorted_name", func (i uint32) {
					health.Set(ii)
				})
			})
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 100_000, col.Count())
}

func TestConcurrentPointReads(t *testing.T) {
	obj := map[string]any{
		"name":   "Roman",
		"age":    35,
		"wallet": 50.99,
		"health": 100,
		"mana":   200,
	}

	col := NewCollection()
	col.CreateColumnsOf(obj)
	for i := 0; i < 1000; i++ {
		col.Insert(func(r Row) error {
			return r.SetMany(obj)
		})
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

	idx, err := c.Insert(func(r Row) error {
		if _, ok := r.TTL(); !ok {
			assert.True(t, r.SetTTL(0).IsZero())
			assert.False(t, r.SetTTL(time.Hour).IsZero())
			r.SetString("name", "Roman")
		}
		return nil
	})

	assert.Equal(t, uint32(0), idx)
	assert.NoError(t, err)
	assert.NoError(t, c.QueryAt(idx, func(r Row) error {
		ttl, ok := r.TTL()
		assert.True(t, ok)
		assert.NotZero(t, ttl)
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

	source.Insert(func(r Row) error {
		r.SetAny("id", "bob")
		r.SetInt("cnt", 2)
		return nil
	})

	// give the replica stream a moment
	time.Sleep(100 * time.Millisecond)

	target.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.Count())
		return nil
	})
}

// --------------------------- Create/Drop Trigger ----------------------------

func TestTriggerCreate(t *testing.T) {
	updates := make([]string, 0, 128)
	players := loadPlayers(500)
	players.CreateTrigger("on_balance", "balance", func(r Reader) {
		switch {
		case r.IsDelete():
			updates = append(updates, fmt.Sprintf("delete %d", r.Index()))
		case r.IsUpsert():
			updates = append(updates, fmt.Sprintf("upsert %d=%v", r.Index(), r.Float()))
		}
	})

	// Perform a few deletions and insertions
	for i := 0; i < 3; i++ {
		players.DeleteAt(uint32(i))
		players.Insert(func(r Row) error {
			r.SetFloat64("balance", 50.0)
			return nil
		})
	}

	// Must keep track of all operations
	assert.Len(t, updates, 6)
	assert.Equal(t, []string{"delete 0", "upsert 500=50", "delete 1", "upsert 501=50", "delete 2", "upsert 502=50"}, updates)
	assert.NoError(t, players.DropTrigger("on_balance"))

	// Must not drop if doesn't exist or not a trigger
	assert.Error(t, players.DropTrigger("on_balance"))
	assert.Error(t, players.DropTrigger("balance"))

	// After dropping, should not trigger anymore
	players.DeleteAt(100)
	assert.Len(t, updates, 6)
}

func TestTriggerInvalid(t *testing.T) {
	players := newEmpty(10)
	assert.Error(t, players.CreateTrigger("on_balance", "invalid", func(r Reader) {}))
	assert.Error(t, players.CreateTrigger("", "", nil))
}

func TestTriggerImpl(t *testing.T) {
	column := newTrigger("test", "target", func(r Reader) {}).Column
	v, ok := column.Value(0)

	assert.Nil(t, v)
	assert.False(t, ok)
	assert.False(t, column.Contains(0))
	assert.Nil(t, column.Index(0))
	assert.NotPanics(t, func() {
		column.Grow(100)
		column.Snapshot(0, nil)
	})
}

// --------------------------- Mocks & Fixtures ----------------------------

// loadPlayers loads a list of players from the fixture
func loadPlayers(amount int) *Collection {
	out := newEmpty(amount)

	// Load and copy until we reach the amount required
	data := fixtures.Players()
	for i := 0; i < amount/len(data); i++ {
		insertPlayers(out, data)
	}
	return out
}

func insertPlayers(dst *Collection, data []fixtures.Player) error {
	return dst.Query(func(txn *Txn) error {
		for _, v := range data {
			txn.Insert(func(r Row) error {
				r.SetString("serial", v.Serial)
				r.SetString("name", v.Name)
				r.SetBool("active", v.Active)
				r.SetEnum("class", v.Class)
				r.SetEnum("race", v.Race)
				r.SetInt("age", v.Age)
				r.SetInt("hp", v.Hp)
				r.SetInt("mp", v.Mp)
				r.SetFloat64("balance", v.Balance)
				r.SetEnum("gender", v.Gender)
				r.SetEnum("guild", v.Guild)
				r.SetRecord("location", &v.Location)
				return nil
			})
		}
		return nil
	})
}

// newEmpty creates a new empty collection for a the fixture
func newEmpty(capacity int) *Collection {
	out := NewCollection(Options{
		Capacity: capacity,
		Vacuum:   500 * time.Millisecond,
		Writer:   new(noopWriter),
	})

	// Load the items into the collection
	out.CreateColumn("serial", ForString())
	out.CreateColumn("name", ForString())
	out.CreateColumn("active", ForBool())
	out.CreateColumn("class", ForEnum())
	out.CreateColumn("race", ForEnum())
	out.CreateColumn("age", ForInt())
	out.CreateColumn("hp", ForInt())
	out.CreateColumn("mp", ForInt())
	out.CreateColumn("balance", ForFloat64())
	out.CreateColumn("gender", ForEnum())
	out.CreateColumn("guild", ForEnum())
	out.CreateColumn("location", ForRecord(func() *fixtures.Location {
		return new(fixtures.Location)
	}))

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
		return r.Int() >= 30
	})

	return out
}

// --------------------------- Mock Record ----------------------------

type mockRecord struct {
	errDecode bool
	errEncode bool
}

func (r mockRecord) MarshalBinary() ([]byte, error) {
	if r.errEncode {
		return nil, io.ErrUnexpectedEOF
	}
	return []byte("OK"), nil
}

func (r mockRecord) UnmarshalBinary(b []byte) error {
	if r.errDecode {
		return io.ErrUnexpectedEOF
	}
	return nil
}
