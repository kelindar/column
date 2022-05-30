// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFind(t *testing.T) {
	players := loadPlayers(500)
	count := 0
	players.Query(func(txn *Txn) error {
		names := txn.Enum("name")

		txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithUint("age", func(v uint64) bool {
			return v >= 30
		}).Range(func(index uint32) {
			count++
			name, _ := names.Get()
			assert.NotEmpty(t, name)
		})
		return nil
	})

	assert.Equal(t, 21, count)
}

func TestMany(t *testing.T) {
	players := loadPlayers(20000)
	count := 0
	players.Query(func(txn *Txn) error {
		txn.Range(func(index uint32) {
			count++
		})
		return nil
	})

	assert.Equal(t, 20000, count)
}

func TestCount(t *testing.T) {
	players := loadPlayers(500)

	// Count all players
	assert.Equal(t, 500, players.Count())

	// How many humans?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 138, txn.WithValue("race", func(v interface{}) bool {
			return v == "human"
		}).Count())
		return nil
	})

	// How many elves + dwarves?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 254, txn.With("elf").Union("dwarf").Count())
		return nil
	})

	// How many elves + dwarves + human?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 392, txn.With("elf").Union("dwarf", "human").Count())
		return nil
	})

	// How many not elves, dwarfs or humans?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 108, txn.Without("elf", "dwarf", "human").Count())
		return nil
	})

	// How many active players?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 247, txn.With("active").Count())
		return nil
	})

	// How many inactive players?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 253, txn.Without("active").Count())
		return nil
	})

	// How many players with a name?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("name").Count())
		return nil
	})

	// How many wealthy?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 222, txn.WithInt("balance", func(v int64) bool {
			return v > 2500
		}).Count())
		return nil
	})

	// How many wealthy?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 222, txn.WithUint("balance", func(v uint64) bool {
			return v > 2500
		}).Count())
		return nil
	})
}

func TestIndexInvalid(t *testing.T) {
	players := loadPlayers(500)
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("invalid-index").Count())
		return nil
	})

	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("human", "invalid-index").Count())
		return nil
	})

	assert.NoError(t, players.Query(func(txn *Txn) error {
		return txn.Range(func(index uint32) {
			// do nothing
		})
	}))

	players.Query(func(txn *Txn) error {
		assert.Error(t, txn.QueryAt(999999, func(Row) error {
			return fmt.Errorf("not found")
		}))
		assert.NoError(t, txn.QueryAt(0, func(Row) error {
			return nil
		}))
		return nil
	})

	assert.Panics(t, func() {
		players.Query(func(txn *Txn) error {
			invalid := txn.Float64("invalid-column")
			return txn.Range(func(index uint32) {
				invalid.Add(1)
			})
		})
	})

	assert.NoError(t, players.Query(func(txn *Txn) error {
		txn.WithString("class", func(v string) bool {
			return v == "rogue"
		}).DeleteAll()
		return nil
	}))

	assert.Equal(t, 321, players.Count())

	// Invalid index search
	players.Query(func(txn *Txn) error {
		txn.WithFloat("x", func(v float64) bool { return true }).
			WithInt("x", func(v int64) bool { return true }).
			WithUint("x", func(v uint64) bool { return true }).
			WithValue("x", func(v interface{}) bool { return true }).
			WithString("x", func(v string) bool { return true })
		assert.Equal(t, 0, txn.Count())
		return nil
	})

	// Invalid delete at
	players.Query(func(txn *Txn) error {
		assert.False(t, txn.DeleteAt(9999))
		return nil
	})
}

func TestIndexed(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() > 3500
	})

	// How many players are rich?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 74, txn.With("rich").Count())
		return nil
	})

	// Drop the index and check again
	players.DropIndex("rich")
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("rich").Count())
		return nil
	})

	// How many human mages over age of 30?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 21, txn.With("human", "mage", "old").Count())
		return nil
	})

	// Check the index value
	players.Query(func(txn *Txn) error {
		age := txn.Float64("age")
		old := txn.Bool("old")
		class := txn.Enum("class")
		txn.With("human", "mage", "old").
			Range(func(i uint32) {
				age, _ := age.Get()
				class, _ := class.Get()

				assert.True(t, age >= 30)
				assert.True(t, old.Get())
				assert.Equal(t, "mage", class)
			})
		return nil
	})
}

func TestDeleteAll(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	// Delete all old people from the collection
	players.Query(func(txn *Txn) error {
		txn.With("old").DeleteAll()
		return nil
	})

	assert.Equal(t, 245, players.Count())
	assert.NoError(t, players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.Without("old").Count())
		return nil
	}))
}

func TestDeleteFromIndex(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	// Delete all old people from the collection
	players.Query(func(txn *Txn) error {
		txn.With("old").DeleteAll()
		return nil
	})

	// How many human mages left?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 13, txn.With("human", "mage").Count())
		return nil
	})
}

func TestUpdateBulkWithIndex(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("broke", "balance", func(r Reader) bool {
		return r.Float() < 100
	})

	// Make everyone poor
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			balance.Set(1.0)
		})
		return nil
	})

	// Every player should be now poor
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.WithFloat("balance", func(v float64) bool {
			return v == 1.0
		}).Count())
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("broke").Count())
		return nil
	})
}

func TestIndexWithAtomicAdd(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() >= 3000
	})

	// Add balance 30 times by 50+50 = 3000
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		for i := 0; i < 30; i++ {
			txn.Range(func(index uint32) {
				balance.Add(50.0)
				balance.Add(50.0)
			})
		}
		return nil
	})

	// Everyone should now be rich and the indexes updated
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			value, ok := balance.Get()
			assert.True(t, ok)
			assert.GreaterOrEqual(t, value, 3000.0)
		})

		assert.Equal(t, 500, txn.With("rich").Count())
		return nil
	})
}

func TestUpdateWithRollback(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() >= 3000
	})

	// Make everyone rich
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			balance.Set(5000.0)
		})
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("rich").Count())
		return nil
	})

	// Try out the rollback
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			balance.Set(1.0)
		})
		return fmt.Errorf("trigger rollback")
	})

	// Everyone should still be rich
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("rich").Count())
		return nil
	})
}

// Details: https://github.com/kelindar/column/issues/17
func TestCountTwice(t *testing.T) {
	model := NewCollection()
	model.CreateColumnsOf(map[string]interface{}{
		"string": "",
	})

	model.Query(func(txn *Txn) error {
		for i := 0; i < 20000; i++ {
			_, err := txn.InsertObject(map[string]interface{}{
				"string": fmt.Sprint(i),
			})

			assert.NoError(t, err)
		}
		return nil
	})

	assert.NoError(t, model.Query(func(txn *Txn) error {
		assert.Equal(t, 20000, txn.Count())
		assert.Equal(t, 1, txn.WithValue("string", func(v interface{}) bool {
			return v.(string) == "5"
		}).Count())
		assert.Equal(t, 1, txn.WithString("string", func(v string) bool {
			return v == "5"
		}).Count())
		return nil
	}))
}

// Details: https://github.com/kelindar/column/issues/15
func TestUninitializedSet(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())
	c.CreateColumn("col2", ForFloat64())
	c.CreateColumn("col3", ForString())
	someMap := map[string][]interface{}{
		"1": {"A", 1.0},
		"2": {"B", 2.0},
	}

	assert.NoError(t, c.Query(func(txn *Txn) error {
		for i := 0; i < 20000; i++ {
			txn.InsertObject(map[string]interface{}{
				"col1": fmt.Sprint(i % 3),
			})
		}
		return nil
	}))

	assert.NoError(t, c.Query(func(txn *Txn) error {
		col1 := txn.String("col1")
		col2 := txn.Float64("col2")
		col3 := txn.String("col3")

		assert.NoError(t, txn.Range(func(index uint32) {
			col2.Set(0)
		}))
		return txn.Range(func(index uint32) {
			value, _ := col1.Get()
			if a, h := someMap[value]; h {
				col2.Set(a[1].(float64))
				col3.Set(a[0].(string))
			}
		})
	}))
}

func TestUpdateAt(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())
	index := c.InsertObject(map[string]interface{}{
		"col1": "hello",
	})

	assert.NoError(t, c.QueryAt(index, func(r Row) error {
		r.SetString("col1", "hi")
		return nil
	}))
}

func TestUpdateAtInvalid(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.Panics(t, func() {
		c.QueryAt(0, func(r Row) error {
			r.SetString("col2", "hi")
			return nil
		})
	})
}
func TestUpdateAtNoChanges(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.NoError(t, c.QueryAt(20000, func(r Row) error {
		r.SetString("col1", "Roman")
		return nil
	}))

	assert.NoError(t, c.QueryAt(0, func(r Row) error {
		r.txn.bufferFor("xxx").PutInt(123, 123)
		return nil
	}))
}

func TestUpsertKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("val", ForString())
	assert.NoError(t, c.QueryKey("1", func(r Row) error {
		r.SetString("val", "Roman")
		return nil
	}))

	count := 0
	assert.NoError(t, c.QueryKey("1", func(r Row) error {
		count++
		return nil
	}))

	assert.Equal(t, 1, count)
}

func TestUpsertKeyNoColumn(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())

	assert.Panics(t, func() {
		c.QueryKey("1", func(r Row) error {
			r.Enum("xxx")
			return nil
		})
	})
}

func TestDuplicateKey(t *testing.T) {
	c := NewCollection()
	assert.NoError(t, c.CreateColumn("key1", ForKey()))
	assert.Error(t, c.CreateColumn("key2", ForKey()))
}

func TestRowMethods(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("bool", ForBool())
	c.CreateColumn("name", ForString())
	c.CreateColumn("int", ForInt())
	c.CreateColumn("int16", ForInt16())
	c.CreateColumn("int32", ForInt32())
	c.CreateColumn("int64", ForInt64())
	c.CreateColumn("uint", ForUint())
	c.CreateColumn("uint16", ForUint16())
	c.CreateColumn("uint32", ForUint32())
	c.CreateColumn("uint64", ForUint64())
	c.CreateColumn("float32", ForFloat32())
	c.CreateColumn("float64", ForFloat64())

	c.Insert(func(r Row) error {
		r.SetKey("key")
		r.SetBool("bool", true)
		r.SetAny("name", "Roman")

		// Numbers
		r.SetInt("int", 1)
		r.SetInt16("int16", 1)
		r.SetInt32("int32", 1)
		r.SetInt64("int64", 1)
		r.SetUint("uint", 1)
		r.SetUint16("uint16", 1)
		r.SetUint32("uint32", 1)
		r.SetUint64("uint64", 1)
		r.SetFloat32("float32", 1)
		r.SetFloat64("float64", 1)

		// Increment
		r.AddInt("int", 1)
		r.AddInt16("int16", 1)
		r.AddInt32("int32", 1)
		r.AddInt64("int64", 1)
		r.AddUint("uint", 1)
		r.AddUint16("uint16", 1)
		r.AddUint32("uint32", 1)
		r.AddUint64("uint64", 1)
		r.AddFloat32("float32", 1)
		r.AddFloat64("float64", 1)
		return nil
	})

	exists := func(v interface{}, ok bool) {
		assert.NotNil(t, v)
		assert.True(t, ok)
	}

	c.QueryKey("key", func(r Row) error {
		assert.True(t, r.Bool("bool"))
		exists(r.Key())
		exists(r.Any("name"))
		exists(r.Int("int"))
		exists(r.Int16("int16"))
		exists(r.Int32("int32"))
		exists(r.Int64("int64"))
		exists(r.Uint("uint"))
		exists(r.Uint16("uint16"))
		exists(r.Uint32("uint32"))
		exists(r.Uint64("uint64"))
		exists(r.Float32("float32"))
		exists(r.Float64("float64"))
		return nil
	})
}

func TestRow(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForKey())

	var wg sync.WaitGroup
	wg.Add(2)

	go c.Query(func(txn *Txn) error {
		txn.Insert(func(r Row) error {
			name := txn.Key()
			name.Set("Roman")
			return nil
		})
		wg.Done()
		return nil
	})

	go c.Query(func(txn *Txn) error {
		txn.With("human").Count()
		wg.Done()
		return nil
	})

	wg.Wait()
}

func TestUnion(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("d_a", ForString())

	c.CreateIndex("d_a_1", "d_a", func(r Reader) bool { return r.String() == "1" })
	c.CreateIndex("d_a_2", "d_a", func(r Reader) bool { return r.String() == "2" })
	c.CreateIndex("d_a_3", "d_a", func(r Reader) bool { return r.String() == "on99" })

	c.InsertObject(map[string]interface{}{
		"d_a": "1",
	})
	c.InsertObject(map[string]interface{}{
		"d_a": "2",
	})
	c.InsertObject(map[string]interface{}{
		"d_a": "on99",
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.Union("d_a_1").Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Union("d_a_1").Union("d_a_2").Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Union("d_a_2", "d_a_3").Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 3, txn.Union("d_a_1", "d_a_2").Union("d_a_3").Count())
		return nil
	})
}

func TestSumBalance(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	players.Query(func(txn *Txn) error {
		sum := int(txn.Float64("balance").Sum())
		assert.Equal(t, 1212084, sum)
		return nil
	})

	players.Query(func(txn *Txn) error {
		sum := int(txn.With("old", "mage").Float64("balance").Sum())
		assert.Equal(t, 186440, sum)
		return nil
	})
}

func TestSumConcurrently(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("int", ForInt64())

	var curTotal int64
	var wg sync.WaitGroup
	wg.Add(2)

	// Reader
	go func(){
		for j := 1; j <= 1000; j++ {
			c.Query(func (txn *Txn) error {
				assert.Equal(t, txn.Int64("int").Sum(), atomic.LoadInt64(&curTotal))
				return nil
			})
		}
		wg.Done()
	}()

	// Writer
	go func(){
		for i := 1; i <= 1000; i++ {
			c.Insert(func (r Row) error {
				r.SetInt64("int", int64(1))
				return nil
			})
			atomic.AddInt64(&curTotal, 1)
		}
		wg.Done()
	}()

	wg.Wait()
}
