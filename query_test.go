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
		Where(func(v interface{}) bool {
			return v.(string) == "human"
		}, "race").
		Count())

	// How many human + mage?
	assert.Equal(t, 5, players.
		Where(func(v interface{}) bool {
			return v.(string) == "human"
		}, "race").
		Where(func(v interface{}) bool {
			return v.(string) == "mage"
		}, "class").
		Count())
}
