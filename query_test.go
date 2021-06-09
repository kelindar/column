// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// oldHumanMages returns a query
func oldHumanMages(filter Query) {
	filter.
		WithString("race", "human").
		WithString("class", "mage").
		WithFilter("age", func(v interface{}) bool {
			return v.(float64) >= 30
		})
}

// oldHumanMages returns an indexed query
func oldHumanMagesIndexed(filter Query) {
	filter.With("human").With("mage").With("old")
}

func TestFind(t *testing.T) {
	players := loadPlayers()
	count := 0
	players.Find(oldHumanMages, func(o Object) bool {
		count++
		return true
	}, "name")

	assert.Equal(t, 21, count)
}

func TestCount(t *testing.T) {
	players := loadPlayers()

	// Count all players
	assert.Equal(t, 500, players.Count(nil))

	// How many humans?
	assert.Equal(t, 138, players.Count(func(filter Query) {
		filter.WithFilter("race", func(v interface{}) bool {
			return v == "human"
		})
	}))

	// How many elves + dwarves?
	assert.Equal(t, 254, players.Count(func(filter Query) {
		filter.With("elf").Union("dwarf")
	}))

	// How many active players?
	assert.Equal(t, 247, players.Count(func(filter Query) {
		filter.With("active")
	}))

	// How many inactive players?
	assert.Equal(t, 253, players.Count(func(filter Query) {
		filter.Without("active")
	}))

	// How many players with a name?
	assert.Equal(t, 500, players.Count(func(filter Query) {
		filter.With("name")
	}))

	// How many human mages over age of 30?
	assert.Equal(t, 21, players.Count(oldHumanMages))
}

func TestIndexed(t *testing.T) {
	players := loadPlayers()

	// How many human mages over age of 30?
	assert.Equal(t, 21, players.Count(oldHumanMagesIndexed))
}
