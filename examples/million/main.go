// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/kelindar/column"
)

func main() {
	amount, runs := 1000000, 50
	players := column.NewCollection(column.Options{
		Capacity: amount,
	})

	// insert the data first
	measure("insert", fmt.Sprintf("%v rows", amount), func() {
		createCollection(players, amount)
	}, 1)

	// run a full scan
	measure("full scan", "age >= 30", func() {
		players.Query(func(txn *column.Txn) error {
			count := txn.WithFloat("age", func(v float64) bool {
				return v >= 30
			}).Count()
			fmt.Printf("-> result = %v\n", count)
			return nil
		})
	}, runs)

	// run a full scan
	measure("full scan", `class == "rogue"`, func() {
		players.Query(func(txn *column.Txn) error {
			count := txn.WithString("class", func(v string) bool {
				return v == "rogue"
			}).Count()
			fmt.Printf("-> result = %v\n", count)
			return nil
		})
	}, runs)

	// run a query over human mages
	measure("indexed query", "human mages", func() {
		players.Query(func(txn *column.Txn) error {
			fmt.Printf("-> result = %v\n", txn.With("human", "mage").Count())
			return nil
		})
	}, runs*1000)

	// run a query over human mages
	measure("indexed query", "human female mages", func() {
		players.Query(func(txn *column.Txn) error {
			fmt.Printf("-> result = %v\n", txn.With("human", "female", "mage").Count())
			return nil
		})
	}, runs*1000)

	// update everyone
	measure("update", "balance of everyone", func() {
		updates := 0
		players.Query(func(txn *column.Txn) error {
			return txn.Range("balance", func(v column.Cursor) {
				updates++
				v.Update(1000.0)
			})
		})
		fmt.Printf("-> updated %v rows\n", updates)
	}, runs)

	// update age of mages
	measure("update", "age of mages", func() {
		updates := 0
		players.Query(func(txn *column.Txn) error {
			return txn.With("mage").Range("age", func(v column.Cursor) {
				updates++
				v.Update(99.0)
			})
		})
		fmt.Printf("-> updated %v rows\n", updates)
	}, runs)

	// update names of males
	measure("update", "name of males", func() {
		updates := 0
		players.Query(func(txn *column.Txn) error {
			return txn.With("male").Range("name", func(v column.Cursor) {
				updates++
				v.Update("Sir " + v.String())
			})
		})
		fmt.Printf("-> updated %v rows\n", updates)
	}, runs)
}

// createCollection loads a collection of players
func createCollection(out *column.Collection, amount int) *column.Collection {
	out.CreateColumn("serial", column.ForEnum())
	out.CreateColumn("name", column.ForEnum())
	out.CreateColumn("active", column.ForBool())
	out.CreateColumn("class", column.ForEnum())
	out.CreateColumn("race", column.ForEnum())
	out.CreateColumn("age", column.ForFloat64())
	out.CreateColumn("hp", column.ForFloat64())
	out.CreateColumn("mp", column.ForFloat64())
	out.CreateColumn("balance", column.ForFloat64())
	out.CreateColumn("gender", column.ForEnum())
	out.CreateColumn("guild", column.ForEnum())
	out.CreateColumn("location", column.ForAny())

	// index for humans
	out.CreateIndex("human", "race", func(v interface{}) bool {
		return v == "human"
	})

	// index for mages
	out.CreateIndex("mage", "class", func(v interface{}) bool {
		return v == "mage"
	})

	// index for males
	out.CreateIndex("male", "gender", func(v interface{}) bool {
		return v == "male"
	})

	// index for females
	out.CreateIndex("female", "gender", func(v interface{}) bool {
		return v == "female"
	})

	// Load the 500 rows from JSON
	b, err := os.ReadFile("../../fixtures/players.json")
	if err != nil {
		panic(err)
	}

	// Unmarshal the items
	var data []map[string]interface{}
	if err := json.Unmarshal(b, &data); err != nil {
		panic(err)
	}

	// Load and copy until we reach the amount required
	for i := 0; i < amount/len(data); i++ {
		if i%200 == 0 {
			fmt.Printf("-> inserted %v rows\n", out.Count())
		}

		out.Query(func(txn *column.Txn) error {
			for _, p := range data {
				txn.Insert(p)
			}
			return nil
		})
	}
	return out
}

// measure runs a function and measures it
func measure(action, name string, fn func(), iterations int) {
	defer func(start time.Time, stdout *os.File) {
		os.Stdout = stdout
		elapsed := time.Since(start) / time.Duration(iterations)
		fmt.Printf("-> %v took %v\n", action, elapsed.String())
	}(time.Now(), os.Stdout)

	fmt.Println()
	fmt.Printf("running %v of %v...\n", action, name)

	// Run a few times so the results are more stable
	null, _ := os.Open(os.DevNull)
	for i := 0; i < iterations; i++ {
		if i > 0 { // Silence subsequent runs
			os.Stdout = null
		}

		fn()
	}
}
