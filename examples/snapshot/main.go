package main

import (
	"fmt"
	"os"

	"github.com/kelindar/column"
)

func main() {

	// Create a collection and a corresponding schema
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
	fmt.Printf("snapshot: created an empty collection (%v rows)\n", players.Count())

	// Open the file containing a snapshot
	src, err := os.Open("../../fixtures/players.bin")
	if err != nil {
		panic(err)
	}

	// Restore from an existing snapshot
	fmt.Printf("snapshot: restoring from '%v' ...\n", src.Name())
	if err := players.Restore(src); err != nil {
		panic(err)
	}

	fmt.Printf("snapshot: restored %v rows\n", players.Count())
	dst, err := os.Create("snapshot.bin")
	if err != nil {
		panic(err)
	}

	fmt.Printf("snapshot: saving state into '%v' ...\n", dst.Name())
	if err := players.Snapshot(dst); err != nil {
		panic(err)
	}

}
