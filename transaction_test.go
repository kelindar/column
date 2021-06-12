// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFind(t *testing.T) {
	players := loadPlayers()
	count := 0
	players.Query(func(txn Txn) error {
		txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithFloat64("age", func(v float64) bool {
			return v >= 30
		}).Range(func(v Cursor) bool {
			count++
			assert.NotEmpty(t, v.String("name"))
			return true
		})
		return nil
	})

	assert.Equal(t, 21, count)
}

func TestCount(t *testing.T) {
	players := loadPlayers()

	// Count all players
	assert.Equal(t, 500, players.Count())

	// How many humans?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 138, txn.WithValue("race", func(v interface{}) bool {
			return v == "human"
		}).Count())
		return nil
	})

	// How many elves + dwarves?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 254, txn.With("elf").Union("dwarf").Count())
		return nil
	})

	// How many elves + dwarves + human?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 392, txn.With("elf").Union("dwarf", "human").Count())
		return nil
	})

	// How many not elves, dwarfs or humans?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 108, txn.Without("elf", "dwarf", "human").Count())
		return nil
	})

	// How many active players?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 247, txn.With("active").Count())
		return nil
	})

	// How many inactive players?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 253, txn.Without("active").Count())
		return nil
	})

	// How many players with a name?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 500, txn.With("name").Count())
		return nil
	})
}

func TestIndexInvalid(t *testing.T) {
	players := loadPlayers()
	players.Query(func(txn Txn) error {
		assert.Equal(t, 0, txn.With("invalid-index").Count())
		return nil
	})

	players.Query(func(txn Txn) error {
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
	players.Query(func(txn Txn) error {
		assert.Equal(t, 74, txn.With("rich").Count())
		return nil
	})

	// Drop the index and check again
	players.DropIndex("rich")
	players.Query(func(txn Txn) error {
		assert.Equal(t, 0, txn.With("rich").Count())
		return nil
	})

	// How many human mages over age of 30?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 21, txn.With("human", "mage", "old").Count())
		return nil
	})

	// Check the index value
	players.Query(func(txn Txn) error {
		txn.With("human", "mage", "old").
			Range(func(v Cursor) bool {
				assert.True(t, v.Float64("age") >= 30)
				assert.True(t, v.Int64("age") >= 30)
				assert.True(t, v.Uint64("age") >= 30)
				assert.True(t, v.Value("old").(bool))
				assert.True(t, v.Bool("old"))
				return true
			})
		return nil
	})
}

func TestUpdate(t *testing.T) {
	players := loadPlayers()

	// Delete all old people from the collection
	players.Query(func(txn Txn) error {
		txn.With("old").Range(func(v Cursor) bool {
			v.Delete()
			return true
		})
		return nil
	})

	// How many human mages left?
	players.Query(func(txn Txn) error {
		assert.Equal(t, 13, txn.With("human", "mage").Count())
		return nil
	})

	// Make everyone poor
	players.Query(func(txn Txn) error {
		txn.Range(func(v Cursor) bool {
			v.Update("balance", 1.0)
			return true
		})
		return nil
	})

	// Every player should be now poor
	count := players.Count()
	players.Query(func(txn Txn) error {
		assert.Equal(t, count, txn.WithFloat64("balance", func(v float64) bool {
			return v == 1.0
		}).Count())
		return nil
	})
}
