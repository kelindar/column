package columnar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	players := loadPlayers()

	// Count all players
	assert.Equal(t, 50, players.Count())

	// How many human?
	assert.Equal(t, 14, players.
		Where("race", func(v interface{}) bool {
			return v == "human"
		}).
		Count())

	// How many human + mage?
	assert.Equal(t, 5, players.
		Where("race", func(v interface{}) bool {
			return v == "human"
		}).
		Where("class", func(v interface{}) bool {
			return v == "mage"
		}).
		Count())
}
