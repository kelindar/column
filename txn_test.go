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
			return
		})
	}))

	players.Query(func(txn *Txn) error {
		_, ok := txn.ReadAt(999999)
		assert.False(t, ok)

		_, ok = txn.ReadAt(0)
		assert.True(t, ok)
		return nil
	})

	assert.NoError(t, players.Query(func(txn *Txn) error {
		return txn.Range("balance", func(v Cursor) {
			v.AddAt("invalid-column", 1)
			return
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
	players.CreateIndex("rich", "balance", func(v interface{}) bool {
		return v.(float64) > 3500
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
			assert.Equal(t, int64(0), v.Int())
			assert.Equal(t, uint64(0), v.Uint())
		})
		return nil
	})

}

func TestUpdate(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("broke", "balance", func(v interface{}) bool {
		return v.(float64) < 100
	})
	players.CreateIndex("rich", "balance", func(v interface{}) bool {
		return v.(float64) >= 3000
	})

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

	// Make everyone poor
	players.Query(func(txn *Txn) error {
		txn.Range("balance", func(v Cursor) {
			v.Update(1.0)
		})
		return nil
	})

	// Every player should be now poor
	count := players.Count()
	players.Query(func(txn *Txn) error {
		assert.Equal(t, count, txn.WithFloat("balance", func(v float64) bool {
			return v == 1.0
		}).Count())
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.With("broke").Count())
		return nil
	})

	// Make everyone rich
	players.Query(func(txn *Txn) error {
		txn.Range("balance", func(v Cursor) {
			v.Update(5000.0)
		})
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.With("rich").Count())
		return nil
	})

	// Try out the rollback
	players.Query(func(txn *Txn) error {
		txn.Range("balance", func(v Cursor) {
			v.Update(1.0)
		})
		return fmt.Errorf("trigger rollback")
	})

	// Everyone should still be rich
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.With("rich").Count())
		return nil
	})

	// Reset balance back to zero
	println("reset balance")
	players.Query(func(txn *Txn) error {
		return txn.Range("balance", func(v Cursor) {
			v.Update(0.0)
		})
	})

	// Everyone should be poor
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.With("broke").Count())
		return nil
	})

	// Increment balance 30 times by 100+100 = 6000
	players.Query(func(txn *Txn) error {
		for i := 0; i < 30; i++ {
			txn.Range("balance", func(v Cursor) {
				v.Add(100.0)
				v.AddAt("balance", 100.0)
			})
			txn.commit()
		}
		return nil
	})

	// Everyone should now be rich and the indexes updated
	players.Query(func(txn *Txn) error {
		txn.Range("balance", func(v Cursor) {
			assert.Equal(t, 6000.0, v.Float())
		})

		assert.Equal(t, 245, txn.With("rich").Count())
		return nil
	})
}
