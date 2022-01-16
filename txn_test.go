// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"
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
		assert.Error(t, txn.UpdateAt(999999, func(*Txn) error {
			return fmt.Errorf("not found")
		}))
		assert.NoError(t, txn.UpdateAt(0, func(*Txn) error {
			return nil
		}))
		return nil
	})

	assert.Panics(t, func() {
		players.Query(func(txn *Txn) error {
			invalid := txn.Float64("invalid-column")
			return txn.Range(func(index uint32) {
				invalid.Increment(1)
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

	// Increment balance 30 times by 50+50 = 3000
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		for i := 0; i < 30; i++ {
			txn.Range(func(index uint32) {
				balance.Increment(50.0)
				balance.Increment(50.0)
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

	assert.NoError(t, c.UpdateAt(index, func(txn *Txn) error {
		name := txn.String("col1")
		name.Set("hi")
		return nil
	}))
}

func TestUpdateAtInvalid(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.Panics(t, func() {
		c.UpdateAt(0, func(txn *Txn) error {
			name := txn.String("col2")
			name.Set("hi")
			return nil
		})
	})
}
func TestUpdateAtNoChanges(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.NoError(t, c.UpdateAt(20000, func(txn *Txn) error {
		txn.String("col1").Set("Roman")
		return nil
	}))

	assert.NoError(t, c.UpdateAt(0, func(txn *Txn) error {
		txn.bufferFor("xxx").PutInt(123, 123)
		return nil
	}))
}

func TestUpsertKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("val", ForString())
	assert.NoError(t, c.UpdateAtKey("1", func(txn *Txn) error {
		name := txn.String("val")
		name.Set("Roman")
		return nil
	}))

	count := 0
	assert.NoError(t, c.UpdateAtKey("1", func(txn *Txn) error {
		count++
		return nil
	}))

	assert.Equal(t, 1, count)
}

func TestUpsertKeyNoColumn(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())

	assert.Panics(t, func() {
		c.UpdateAtKey("1", func(txn *Txn) error {
			txn.Enum("xxx")
			return nil
		})
	})
}

func TestDuplicateKey(t *testing.T) {
	c := NewCollection()
	assert.NoError(t, c.CreateColumn("key1", ForKey()))
	assert.Error(t, c.CreateColumn("key2", ForKey()))
}

func TestDataRace(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForKey())

	var wg sync.WaitGroup
	wg.Add(2)

	go c.Query(func(txn *Txn) error {
		txn.Insert(func(txn *Txn) error {
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
