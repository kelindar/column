// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/kelindar/async"
	"github.com/kelindar/column"
	"github.com/kelindar/rand"
)

var (
	classes = []string{"fighter", "mage", "rogue"}
	races   = []string{"human", "elf", "dwarf", "orc"}
)

func main() {
	amount := 1000000
	players := column.NewCollection(column.Options{
		Capacity: amount,
	})

	// insert the data first
	createCollection(players, amount)

	// Iterate over various workloads
	for _, w := range []int{10, 50, 90} {

		// Iterate over various concurrency levels
		for _, n := range []int{1, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096} {

			// Create a pool of N goroutines
			work := make(chan async.Task, n)
			pool := async.Consume(context.Background(), n, work)

			//run(fmt.Sprintf("(%v/%v)-%v", 100-w, w, n), func(b *testing.B) {
			var reads int64
			var writes int64

			var wg sync.WaitGroup

			start := time.Now()
			for time.Since(start) < 2*time.Second {
				wg.Add(1)
				work <- async.NewTask(func(ctx context.Context) (interface{}, error) {
					defer wg.Done()
					offset := uint32(rand.Uint32n(uint32(amount - 1)))

					// Given our write probabiliy, randomly update an offset
					if rand.Uint32n(100) < uint32(w) {
						players.UpdateAt(offset, "balance", 0.0)
						atomic.AddInt64(&writes, 1)
						return nil, nil
					}

					// Otherwise, randomly read something
					players.Query(func(txn *column.Txn) error {
						var count int64
						txn.With(races[rand.Uint32n(4)]).Range("balance", func(v column.Cursor) {
							count++
						})
						atomic.AddInt64(&reads, count)
						return nil
					})

					return nil, nil
				})
			}

			elapsed := time.Since(start)
			readsPerSec := int64(float64(reads) / elapsed.Seconds())
			writesPerSec := int64(float64(writes) / elapsed.Seconds())

			wg.Wait()
			pool.Cancel()
			fmt.Printf("%v%%-%v%%    %4v procs    %20v    %15v\n", 100-w, w, n,
				humanize.Comma(readsPerSec)+" read/s",
				humanize.Comma(writesPerSec)+" write/s",
			)
		}

	}
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

	for _, v := range classes {
		class := v
		out.CreateIndex(class, "class", func(r column.Reader) bool {
			return r.String() == class
		})
	}

	for _, v := range races {
		race := v
		out.CreateIndex(race, "race", func(r column.Reader) bool {
			return r.String() == race
		})
	}
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

	// Load the data in
	for i := 0; i < amount/len(data); i++ {
		out.Query(func(txn *column.Txn) error {
			for _, p := range data {
				txn.Insert(p)
			}
			return nil
		})
	}

	return out
}
