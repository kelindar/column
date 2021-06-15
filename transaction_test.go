// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFind(t *testing.T) {
	players := loadPlayers()
	count := 0
	players.Query(func(txn *Txn) error {
		txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithFloat("age", func(v float64) bool {
			return v >= 30
		}).Range(func(v Cursor) bool {
			count++
			assert.NotEmpty(t, v.String())
			return true
		}, "name")
		return nil
	})

	assert.Equal(t, 21, count)
}

func TestCount(t *testing.T) {
	players := loadPlayers()

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
	players := loadPlayers()
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("invalid-index").Count())
		return nil
	})

	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("human", "invalid-index").Count())
		return nil
	})
}

func TestIndexed(t *testing.T) {
	players := loadPlayers()
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
			Select(func(v Selector) bool {
				assert.True(t, v.FloatAt("age") >= 30)
				assert.True(t, v.IntAt("age") >= 30)
				assert.True(t, v.UintAt("age") >= 30)
				assert.True(t, v.ValueAt("old").(bool))
				assert.True(t, v.BoolAt("old"))
				assert.Equal(t, "mage", v.StringAt("class"))
				assert.False(t, v.BoolAt("xxx"))
				return true
			})
		return nil
	})

	// Check with multiple Selectors
	players.Query(func(txn *Txn) error {
		result := txn.With("human", "mage", "old")

		result.Range(func(v Cursor) bool {
			assert.True(t, v.Float() >= 30)
			assert.True(t, v.Int() >= 30)
			assert.True(t, v.Uint() >= 30)
			return true
		}, "age")

		result.Range(func(v Cursor) bool {
			assert.True(t, v.Value().(bool))
			assert.True(t, v.Bool())
			assert.Equal(t, "", v.String())
			return true
		}, "old")

		result.Range(func(v Cursor) bool {
			assert.Equal(t, "mage", v.String())
			assert.Equal(t, float64(0), v.Float())
			assert.Equal(t, int64(0), v.Int())
			assert.Equal(t, uint64(0), v.Uint())
			return true
		}, "class")
		return nil
	})

}

func TestUpdate(t *testing.T) {
	players := loadPlayers()
	players.CreateIndex("broke", "balance", func(v interface{}) bool {
		return v.(float64) < 100
	})
	players.CreateIndex("rich", "balance", func(v interface{}) bool {
		return v.(float64) > 3000
	})

	// Delete all old people from the collection
	players.Query(func(txn *Txn) error {
		txn.With("old").Select(func(v Selector) bool {
			v.Delete()
			return true
		})
		return nil
	})

	// How many human mages left?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 13, txn.With("human", "mage").Count())
		return nil
	})

	// Make everyone poor
	players.Query(func(txn *Txn) error {
		txn.Select(func(v Selector) bool {
			v.UpdateAt("balance", 1.0)
			return true
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
		txn.Range(func(v Cursor) bool {
			v.Update(5000.0)
			return true
		}, "balance")
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.With("rich").Count())
		assert.Equal(t, 0, txn.With("broke").Count())
		return nil
	})

	// Try out the rollback
	players.Query(func(txn *Txn) error {
		txn.Select(func(v Selector) bool {
			v.UpdateAt("balance", 1.0)
			return true
		})
		return fmt.Errorf("trigger rollback")
	})

	// Everyone should still be rich
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.With("rich").Count())
		assert.Equal(t, 0, txn.With("broke").Count())
		return nil
	})

}
