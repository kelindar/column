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
	players.View(func(txn Txn) error {
		txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithFloat64("age", func(v float64) bool {
			return v >= 30
		}).Range(func(v Selector) bool {
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
	players.View(func(txn Txn) error {
		assert.Equal(t, 138, txn.WithValue("race", func(v interface{}) bool {
			return v == "human"
		}).Count())
		return nil
	})

	// How many elves + dwarves?
	players.View(func(txn Txn) error {
		assert.Equal(t, 254, txn.With("elf").Union("dwarf").Count())
		return nil
	})

	// How many active players?
	players.View(func(txn Txn) error {
		assert.Equal(t, 247, txn.With("active").Count())
		return nil
	})

	// How many inactive players?
	players.View(func(txn Txn) error {
		assert.Equal(t, 253, txn.Without("active").Count())
		return nil
	})

	// How many players with a name?
	players.View(func(txn Txn) error {
		assert.Equal(t, 500, txn.With("name").Count())
		return nil
	})
}

func TestIndexInvalid(t *testing.T) {
	players := loadPlayers()
	players.View(func(txn Txn) error {
		assert.Equal(t, 0, txn.With("invalid-index").Count())
		return nil
	})

	players.View(func(txn Txn) error {
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
	players.View(func(txn Txn) error {
		assert.Equal(t, 74, txn.With("rich").Count())
		return nil
	})

	// Drop the index and check again
	players.DropIndex("rich")
	players.View(func(txn Txn) error {
		assert.Equal(t, 0, txn.With("rich").Count())
		return nil
	})

	// How many human mages over age of 30?
	players.View(func(txn Txn) error {
		assert.Equal(t, 21, txn.With("human", "mage", "old").Count())
		return nil
	})

	// Check the index value
	players.View(func(txn Txn) error {
		txn.With("human", "mage", "old").
			Range(func(v Selector) bool {
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
