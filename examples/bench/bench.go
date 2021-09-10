// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/kelindar/async"
	"github.com/kelindar/column"
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
	createCollection(players, amount)

	// This runs point query benchmarks
	runBenchmark("Point Reads/Writes", func(v uint32, writeTxn bool) (reads int, writes int) {

		// To avoid task granuarity problem, load up a bit more work on each
		// of the goroutines, a few hundred reads should be enough to amortize
		// the cost of scheduling goroutines, so we can actually test our code.
		for i := 0; i < 1000; i++ {
			offset := randN(v, amount-1)
			if writeTxn {
				players.UpdateAt(offset, "balance", func(v column.Cursor) error {
					v.SetFloat64(0)
					return nil
				})
				writes++
			} else {
				players.SelectAt(offset, func(v column.Selector) {
					_ = v.FloatAt("balance") // Read
				})
				reads++
			}
		}
		return
	})
}

// runBenchmark runs a benchmark
func runBenchmark(name string, fn func(uint32, bool) (int, int)) {
	fmt.Printf("Benchmarking %v ...\n", name)
	fmt.Printf("%7v\t%6v\t%17v\t%13v\n", "WORK", "PROCS", "READ RATE", "WRITE RATE")
	for _, workload := range []int{0, 10, 50, 90, 100} {

		// Iterate over various concurrency levels
		for _, n := range []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512} {
			work := make(chan async.Task, n)
			pool := async.Consume(context.Background(), n, work)

			var reads, writes int64
			var wg sync.WaitGroup
			start := time.Now()
			for i := uint32(0); time.Since(start) < time.Second; i++ {
				wg.Add(1)
				work <- async.NewTask(func(ctx context.Context) (interface{}, error) {
					defer wg.Done()

					r, w := fn(i, chanceOf(i, workload))
					atomic.AddInt64(&reads, int64(r))
					atomic.AddInt64(&writes, int64(w))
					return nil, nil
				})
			}

			wg.Wait()
			pool.Cancel()

			elapsed := time.Since(start)
			fmt.Printf("%v%%-%v%%\t%6v\t%17v\t%13v\n", 100-workload, workload, n,
				humanize.Comma(int64(float64(reads)/elapsed.Seconds()))+" txn/s",
				humanize.Comma(int64(float64(writes)/elapsed.Seconds()))+" txn/s",
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

// This random number generator not the most amazing one, but much better
// than using math.Rand for our benchmarks, since it would create a lock
// contention and bias the results.
func randN(v uint32, n int) uint32 {
	return uint32(xxhash(v) % uint64(n))
}

func chanceOf(v uint32, chance int) bool {
	return randN(v, 100) < uint32(chance)
}

func xxhash(v uint32) uint64 {
	packed := uint64(v) + uint64(v)<<32
	x := packed ^ (0x1cad21f72c81017c ^ 0xdb979083e96dd4de)
	x ^= bits.RotateLeft64(x, 49) ^ bits.RotateLeft64(x, 24)
	x *= 0x9fb21c651e98df25
	x ^= (x >> 35) + 4 // len
	x *= 0x9fb21c651e98df25
	x ^= (x >> 28)
	return x
}
