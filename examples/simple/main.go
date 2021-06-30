package main

import (
	"encoding/json"
	"os"

	"github.com/kelindar/column"
)

func main() {

	// Create a new columnar collection
	players := column.NewCollection()

	// index on humans
	players.CreateIndex("human", "race", func(v interface{}) bool {
		return v == "human"
	})

	// index for mages
	players.CreateIndex("mage", "class", func(v interface{}) bool {
		return v == "mage"
	})

	// index for old
	players.CreateIndex("old", "age", func(v interface{}) bool {
		return v.(float64) >= 30
	})

	// Load the items into the collection
	loaded := loadFixture("players.json")
	players.CreateColumnsOf(loaded[0])

	// Perform a bulk insert
	players.Query(func(txn *column.Txn) error {
		for _, v := range loaded {
			txn.Insert(v)
		}
		return nil
	})

	// Run an indexed query
	players.Query(func(txn *column.Txn) error {
		return txn.With("human", "mage", "old").Range("name", func(v column.Cursor) {
			println("human old mage", v.String())
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
