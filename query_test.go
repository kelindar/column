package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// oldHumanMages returns a query
func oldHumanMages(where Query) {
	where.
		String("race", "human").
		String("class", "mage").
		Value("age", func(v interface{}) bool {
			return v.(float64) >= 30
		})
}

func TestFind(t *testing.T) {
	players := loadPlayers()
	count := 0
	players.Find(oldHumanMages, func(o Object) bool {
		count++
		return true
	}, "name")

	assert.Equal(t, 3, count)
}

func TestCount(t *testing.T) {
	players := loadPlayers()

	// Count all players
	assert.Equal(t, 50, players.Count(nil))

	// How many humans
	assert.Equal(t, 14, players.Count(func(where Query) {
		where.Value("race", func(v interface{}) bool {
			return v == "human"
		})
	}))

	// How many human mages over age of 30?
	assert.Equal(t, 3, players.Count(oldHumanMages))
}
