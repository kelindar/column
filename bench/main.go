package main

import (
	"encoding/json"
	"os"

	"github.com/kelindar/columnar"
	"github.com/pkg/profile"
)

func main() {
	players := loadPlayers()
	p := profile.Start(profile.MemProfile, profile.MemProfileHeap, profile.ProfilePath("."), profile.NoShutdownHook)
	defer p.Stop()

	for n := 0; n < 3000000; n++ {
		players.Where("race", func(v interface{}) bool {
			return v.(string) == "human"
		}).Count()
	}
}

// loadPlayers loads a list of players from the fixture
func loadPlayers() *columnar.Collection {
	players := loadFixture("players.json")
	out := columnar.New()
	for _, p := range players {
		out.Add(p)
	}
	return out
}

// loadFixture loads a fixture by its name
func loadFixture(name string) []columnar.Object {
	b, err := os.ReadFile("../fixtures/" + name)
	if err != nil {
		panic(err)
	}

	var data []columnar.Object
	if err := json.Unmarshal(b, &data); err != nil {
		panic(err)
	}
	return data
}
