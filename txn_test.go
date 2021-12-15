// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFind(t *testing.T) {
	players := loadPlayers(500)
	count := 0
	players.Query(func(txn *Txn) error {
		txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithUint("age", func(v uint64) bool {
			return v >= 30
		}).Range("name", func(v Cursor) {
			count++
			assert.NotEmpty(t, v.String())
		})
		return nil
	})

	assert.Equal(t, 21, count)
}

func TestMany(t *testing.T) {
	players := loadPlayers(20000)
	count := 0
	players.Query(func(txn *Txn) error {
		txn.Range("name", func(v Cursor) {
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

	assert.Error(t, players.Query(func(txn *Txn) error {
		return txn.Range("invalid-column", func(v Cursor) {
			// do nothing
		})
	}))

	players.Query(func(txn *Txn) error {
		assert.False(t, txn.SelectAt(999999, func(v Selector) {}))
		assert.True(t, txn.SelectAt(0, func(v Selector) {}))
		return nil
	})

	assert.NoError(t, players.Query(func(txn *Txn) error {
		return txn.Range("balance", func(v Cursor) {
			v.AddFloat64At("invalid-column", 1)
		})
	}))

	assert.NoError(t, players.Query(func(txn *Txn) error {
		txn.DeleteIf(func(v Selector) bool {
			return v.StringAt("class") == "rogue"
		})
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
		txn.With("human", "mage", "old").
			Select(func(v Selector) {
				assert.True(t, v.FloatAt("age") >= 30)
				assert.True(t, v.IntAt("age") >= 30)
				assert.True(t, v.UintAt("age") >= 30)
				assert.True(t, v.ValueAt("old").(bool))
				assert.True(t, v.BoolAt("old"))
				assert.Equal(t, "mage", v.StringAt("class"))
				assert.False(t, v.BoolAt("xxx"))
			})
		return nil
	})

	// Check with multiple Selectors
	players.Query(func(txn *Txn) error {
		result := txn.With("human", "mage", "old")

		result.Range("age", func(v Cursor) {
			assert.True(t, v.Float() >= 30)
			assert.True(t, v.Int() >= 30)
			assert.True(t, v.Uint() >= 30)
		})

		result.Range("old", func(v Cursor) {
			assert.True(t, v.Value().(bool))
			assert.True(t, v.Bool())
		})

		result.Range("class", func(v Cursor) {
			//assert.Equal(t, "mage", v.String())
			assert.Equal(t, float64(0), v.Float())
			assert.Equal(t, int(0), v.Int())
			assert.Equal(t, uint(0), v.Uint())
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
		txn.Range("balance", func(v Cursor) {
			v.SetFloat64(1.0)
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
		for i := 0; i < 30; i++ {
			txn.Range("balance", func(v Cursor) {
				v.AddFloat64(50.0)
				v.AddFloat64At("balance", 50.0)
			})
		}
		return nil
	})

	// Everyone should now be rich and the indexes updated
	players.Query(func(txn *Txn) error {
		txn.Range("balance", func(v Cursor) {
			assert.GreaterOrEqual(t, v.Float(), 3000.0)
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
		txn.Range("balance", func(v Cursor) {
			v.SetFloat64(5000.0)
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
		txn.Range("balance", func(v Cursor) {
			v.SetFloat64(1.0)
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
			txn.InsertObject(map[string]interface{}{
				"string": fmt.Sprint(i),
			})
		}
		return nil
	})

	model.Query(func(txn *Txn) error {
		assert.Equal(t, 20000, txn.Count())
		assert.Equal(t, 1, txn.WithValue("string", func(v interface{}) bool {
			return v.(string) == "5"
		}).Count())
		assert.Equal(t, 1, txn.WithString("string", func(v string) bool {
			return v == "5"
		}).Count())
		return nil
	})
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
		assert.NoError(t, txn.Range("col2", func(v Cursor) {
			v.SetFloat64(0)
		}))
		return txn.Range("col1", func(v Cursor) {
			if a, h := someMap[v.String()]; h {
				v.SetFloat64At("col2", a[1].(float64))
				v.SetStringAt("col3", a[0].(string))
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

	assert.NoError(t, c.UpdateAt(index, "col1", func(v Cursor) error {
		v.Set("hi")
		return nil
	}))

	assert.True(t, c.SelectAt(index, func(v Selector) {
		assert.Equal(t, "hi", v.StringAt("col1"))
	}))
}

func TestUpdateAtInvalid(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.Error(t, c.UpdateAt(0, "col2", func(v Cursor) error {
		v.SetString("hi")
		return nil
	}))
}

func TestUpsertKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("val", ForString())
	assert.NoError(t, c.UpdateAtKey("1", "val", func(v Cursor) error {
		v.Set("Roman")
		return nil
	}))

	count := 0
	c.SelectAtKey("1", func(v Selector) {
		assert.Equal(t, "Roman", v.StringAt("val"))
		count++
	})
	assert.Equal(t, 1, count)
}

func TestUpsertKeyNoColumn(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	assert.Error(t, c.UpdateAtKey("1", "xxx", func(v Cursor) error {
		return nil
	}))
}

func TestDuplicateKey(t *testing.T) {
	c := NewCollection()
	assert.NoError(t, c.CreateColumn("key1", ForKey()))
	assert.Error(t, c.CreateColumn("key2", ForKey()))
}
