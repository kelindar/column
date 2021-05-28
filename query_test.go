package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	players := loadPlayers()

	// Count all players
	assert.Equal(t, 50, players.Count())

	// How many humans
	assert.Equal(t, 14, players.
		Where("race", func(v interface{}) bool {
			return v == "human"
		}).
		Count())

	// How many human mages over age of 30?
	assert.Equal(t, 3, players.
		Where("age", func(v interface{}) bool {
			return v.(float64) >= 30
		}).
		AndValue("race", "human").
		AndValue("class", "mage").
		Count())

}
