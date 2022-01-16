package main

import (
	"encoding/json"
	"os"

	"github.com/kelindar/column"
)

func main() {

	// Create a new columnar collection
	players := column.NewCollection()
	players.CreateColumn("serial", column.ForKey())
	players.CreateColumn("name", column.ForEnum())
	players.CreateColumn("active", column.ForBool())
	players.CreateColumn("class", column.ForEnum())
	players.CreateColumn("race", column.ForEnum())
	players.CreateColumn("age", column.ForFloat64())
	players.CreateColumn("hp", column.ForFloat64())
	players.CreateColumn("mp", column.ForFloat64())
	players.CreateColumn("balance", column.ForFloat64())
	players.CreateColumn("gender", column.ForEnum())
	players.CreateColumn("guild", column.ForEnum())

	// index on humans
	players.CreateIndex("human", "race", func(r column.Reader) bool {
		return r.String() == "human"
	})

	// index for mages
	players.CreateIndex("mage", "class", func(r column.Reader) bool {
		return r.String() == "mage"
	})

	// index for old
	players.CreateIndex("old", "age", func(r column.Reader) bool {
		return r.Float() >= 30
	})

	// Load the items into the collection
	loaded := loadFixture("players.json")
	players.Query(func(txn *column.Txn) error {
		for _, v := range loaded {
			txn.InsertObject(v)
		}
		return nil
	})

	// Run an indexed query
	players.Query(func(txn *column.Txn) error {
		name := txn.Enum("name")
		return txn.With("human", "mage", "old").Range(func(idx uint32) {
			value, _ := name.Get()
			println("old mage, human:", value)
		})
	})
}

// loadFixture loads a fixture by its name
func loadFixture(name string) []column.Object {
	b, err := os.ReadFile("../../fixtures/" + name)
	if err != nil {
		panic(err)
	}

	var data []column.Object
	if err := json.Unmarshal(b, &data); err != nil {
		panic(err)
	}

	return data
}
