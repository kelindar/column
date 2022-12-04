// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"fmt"
	"sync"
	"testing"
	"strconv"

	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

func FuzzInsert(f *testing.F) {
	players := newEmpty(10)
	defer players.Close()

	f.Add("name", true, 30, 7.5)
	f.Fuzz(func(t *testing.T, name string, active bool, age int, balance float64) {

		idx, err := players.Insert(func(r Row) error {
			r.SetString("name", name)
			r.SetBool("active", active)
			r.SetInt("age", age)
			r.SetFloat64("balance", balance)
			return nil
		})

		assert.NoError(t, err)
		assert.NoError(t, players.QueryAt(idx, func(r Row) error {
			assert.Equal(t, active, r.Bool("active"))

			s, ok := r.String("name")
			assert.True(t, ok)
			assert.Equal(t, name, s)

			i, ok := r.Int("age")
			assert.True(t, ok)
			assert.Equal(t, age, i)

			f, ok := r.Float64("balance")
			assert.True(t, ok)
			assert.Equal(t, balance, f)
			return nil
		}))

		assert.True(t, players.DeleteAt(idx))
	})
}

func TestFind(t *testing.T) {
	players := loadPlayers(500)
	count := 0
	players.Query(func(txn *Txn) error {
		names := txn.String("name")

		txn.WithString("race", func(v string) bool {
			return v == "human"
		}).WithString("class", func(v string) bool {
			return v == "mage"
		}).WithInt("age", func(v int64) bool {
			return v >= 30
		}).Range(func(index uint32) {
			count++
			name, _ := names.Get()
			assert.NotEmpty(t, name)
		})
		return nil
	})

	assert.Equal(t, 21, count)
}

func TestMany(t *testing.T) {
	players := loadPlayers(20000)
	count := 0
	players.Query(func(txn *Txn) error {
		txn.Range(func(index uint32) {
			count++
		})
		return nil
	})

	assert.Equal(t, 20000, count)
}

func TestCount(t *testing.T) {
	players := loadPlayers(500)

	// Count all players
	assert.Equal(t, 500, players.Count())

	// How many humans?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 138, txn.WithValue("race", func(v interface{}) bool {
			return v == "human"
		}).Count())
		return nil
	})

	// How many elves + dwarves?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 254, txn.With("elf").Union("dwarf").Count())
		return nil
	})

	// How many elves + dwarves + human?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 392, txn.With("elf").Union("dwarf", "human").Count())
		return nil
	})

	// How many not elves, dwarfs or humans?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 108, txn.Without("elf", "dwarf", "human").Count())
		return nil
	})

	// How many active players?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 247, txn.With("active").Count())
		return nil
	})

	// How many inactive players?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 253, txn.Without("active").Count())
		return nil
	})

	// How many players with a name?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("name").Count())
		return nil
	})

	// How many wealthy?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 222, txn.WithInt("balance", func(v int64) bool {
			return v > 2500
		}).Count())
		return nil
	})

	// How many wealthy?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 222, txn.WithUint("balance", func(v uint64) bool {
			return v > 2500
		}).Count())
		return nil
	})
}

func TestIndexInvalid(t *testing.T) {
	players := loadPlayers(500)
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("invalid-index").Count())
		return nil
	})

	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("human", "invalid-index").Count())
		return nil
	})

	assert.NoError(t, players.Query(func(txn *Txn) error {
		return txn.Range(func(index uint32) {
			// do nothing
		})
	}))

	players.Query(func(txn *Txn) error {
		assert.Error(t, txn.QueryAt(999999, func(Row) error {
			return fmt.Errorf("not found")
		}))
		assert.NoError(t, txn.QueryAt(0, func(Row) error {
			return nil
		}))
		return nil
	})

	assert.Panics(t, func() {
		players.Query(func(txn *Txn) error {
			invalid := txn.Float64("invalid-column")
			return txn.Range(func(index uint32) {
				invalid.Merge(1)
			})
		})
	})

	assert.NoError(t, players.Query(func(txn *Txn) error {
		txn.WithString("class", func(v string) bool {
			return v == "rogue"
		}).DeleteAll()
		return nil
	}))

	assert.Equal(t, 321, players.Count())

	// Invalid index search
	players.Query(func(txn *Txn) error {
		txn.WithFloat("x", func(v float64) bool { return true }).
			WithInt("x", func(v int64) bool { return true }).
			WithUint("x", func(v uint64) bool { return true }).
			WithValue("x", func(v interface{}) bool { return true }).
			WithString("x", func(v string) bool { return true })
		assert.Equal(t, 0, txn.Count())
		return nil
	})

	// Invalid delete at
	players.Query(func(txn *Txn) error {
		assert.False(t, txn.DeleteAt(9999))
		return nil
	})
}

func TestIndexed(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() > 3500
	})

	// How many players are rich?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 74, txn.With("rich").Count())
		return nil
	})

	// Drop the index and check again
	players.DropIndex("rich")
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 0, txn.With("rich").Count())
		return nil
	})

	// How many human mages over age of 30?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 21, txn.With("human", "mage", "old").Count())
		return nil
	})

	// Check the index value
	players.Query(func(txn *Txn) error {
		age := txn.Int("age")
		old := txn.Bool("old")
		class := txn.Enum("class")
		txn.With("human", "mage", "old").
			Range(func(i uint32) {
				age, _ := age.Get()
				class, _ := class.Get()

				assert.True(t, age >= 30)
				assert.True(t, old.Get())
				assert.Equal(t, "mage", class)
			})
		return nil
	})
}

func TestSortIndex(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())
	c.CreateSortIndex("sortedCol1", "col1")

	assert.Error(t, c.CreateSortIndex("", ""))
	assert.Error(t, c.CreateSortIndex("no_col", "nonexistent"))
	assert.Error(t, c.CreateSortIndex("sortedCol1", "col1"))

	c.Insert(func (r Row) error {
		r.SetString("col1", "bob")
		return nil
	})
	c.Insert(func (r Row) error {
		r.SetString("col1", "carter")
		return nil
	})
	c.Insert(func (r Row) error {
		r.SetString("col1", "dan")
		return nil
	})
	c.Insert(func (r Row) error {
		r.SetString("col1", "alice")
		return nil
	})
	assert.NoError(t, c.QueryAt(3, func(r Row) error {
		r.SetString("col1", "rob")
		return nil
	}))
	assert.Equal(t, true, c.DeleteAt(1))

	assert.Error(t, c.Query(func (txn *Txn) error {
		return txn.SortedRange("nonexistent", func (i uint32) {
			return
		})
	}))

	indexCol, _ := c.cols.Load("sortedCol1")
	assert.Equal(t, 3, indexCol.Column.(*columnSortIndex).btree.Len())

	var res [3]string
	var resN int = 0
	c.Query(func (txn *Txn) error {
		col1 := txn.String("col1")
		return txn.SortedRange("sortedCol1", func (i uint32) {
			name, _ := col1.Get()
			res[resN] = name
			resN++
		})
	})

	assert.Equal(t, "bob", res[0])
	assert.Equal(t, "dan", res[1])
	assert.Equal(t, "rob", res[2])
}

func TestSortIndexLoad(t *testing.T) {

	players := loadPlayers(500)
	players.CreateSortIndex("sorted_names", "name")

	checkN := 0
	checks := map[int]string{
		4: "Buckner Frazier",
		16: "Marla Todd",
		30: "Shelly Kirk",
		35: "out of range",
	}

	players.Query(func (txn *Txn) error {
		txn = txn.With("human", "mage")
		name := txn.String("name")
		txn.SortedRange("sorted_names", func (i uint32) {
			n, _ := name.Get()
			if res, exists := checks[checkN]; exists {
				assert.Equal(t, res, n)
			}
			checkN++
		})
		return nil
	})

}

func TestSortIndexChunks(t *testing.T) {
	N := 100_000
	obj := map[string]any{
		"name": "1",
		"balance": 12.5,
	}

	players := NewCollection()
	players.CreateColumnsOf(obj)
	players.CreateSortIndex("sorted_names", "name")

	for i := 0; i < N; i++ {
		players.Insert(func (r Row) error {
			return r.SetMany(map[string]any{
				"name": strconv.Itoa(i),
				"balance": float64(i) + 0.5,
			})
		})
	}

	players.Query(func (txn *Txn) error {
		name := txn.String("name")
		txn.SortedRange("sorted_names", func (i uint32) {
			n, _ := name.Get()
			if i % 400 == 0 {
				nInt, _ := strconv.Atoi(n)
				assert.Equal(t, nInt, int(i))
			}
		})
		return nil
	})
}


func TestDeleteAll(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	// Delete all old people from the collection
	players.Query(func(txn *Txn) error {
		txn.With("old").DeleteAll()
		return nil
	})

	assert.Equal(t, 245, players.Count())
	assert.NoError(t, players.Query(func(txn *Txn) error {
		assert.Equal(t, 245, txn.Without("old").Count())
		return nil
	}))
}

func TestDeleteFromIndex(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	// Delete all old people from the collection
	players.Query(func(txn *Txn) error {
		txn.With("old").DeleteAll()
		return nil
	})

	// How many human mages left?
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 13, txn.With("human", "mage").Count())
		return nil
	})
}

func TestUpdateBulkWithIndex(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("broke", "balance", func(r Reader) bool {
		return r.Float() < 100
	})

	// Make everyone poor
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			balance.Set(1.0)
		})
		return nil
	})

	// Every player should be now poor
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.WithFloat("balance", func(v float64) bool {
			return v == 1.0
		}).Count())
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("broke").Count())
		return nil
	})
}

func TestIndexWithAtomicAdd(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() >= 3000
	})

	// Add balance 30 times by 50+50 = 3000
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		for i := 0; i < 30; i++ {
			txn.Range(func(index uint32) {
				balance.Merge(50.0)
				balance.Merge(50.0)
			})
		}
		return nil
	})

	// Everyone should now be rich and the indexes updated
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			value, ok := balance.Get()
			assert.True(t, ok)
			assert.GreaterOrEqual(t, value, 3000.0)
		})

		assert.Equal(t, 500, txn.With("rich").Count())
		return nil
	})
}

func TestUpdateWithRollback(t *testing.T) {
	players := loadPlayers(500)
	players.CreateIndex("rich", "balance", func(r Reader) bool {
		return r.Float() >= 3000
	})

	// Make everyone rich
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			balance.Set(5000.0)
		})
		return nil
	})

	// Now the index should also be updated
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("rich").Count())
		return nil
	})

	// Try out the rollback
	players.Query(func(txn *Txn) error {
		balance := txn.Float64("balance")
		txn.Range(func(index uint32) {
			balance.Set(1.0)
		})
		return fmt.Errorf("trigger rollback")
	})

	// Everyone should still be rich
	players.Query(func(txn *Txn) error {
		assert.Equal(t, 500, txn.With("rich").Count())
		return nil
	})
}

// Details: https://github.com/kelindar/column/issues/17
func TestCountTwice(t *testing.T) {
	model := NewCollection()
	model.CreateColumnsOf(map[string]interface{}{
		"string": "",
	})

	model.Query(func(txn *Txn) error {
		for i := 0; i < 20000; i++ {
			_, err := txn.Insert(func(r Row) error {
				return r.SetMany(map[string]any{
					"string": fmt.Sprint(i),
				})
			})
			assert.NoError(t, err)
		}
		return nil
	})

	assert.NoError(t, model.Query(func(txn *Txn) error {
		assert.Equal(t, 20000, txn.Count())
		assert.Equal(t, 1, txn.WithValue("string", func(v interface{}) bool {
			return v.(string) == "5"
		}).Count())
		assert.Equal(t, 1, txn.WithString("string", func(v string) bool {
			return v == "5"
		}).Count())
		return nil
	}))
}

// Details: https://github.com/kelindar/column/issues/15
func TestUninitializedSet(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())
	c.CreateColumn("col2", ForFloat64())
	c.CreateColumn("col3", ForString())
	someMap := map[string][]interface{}{
		"1": {"A", 1.0},
		"2": {"B", 2.0},
	}

	assert.NoError(t, c.Query(func(txn *Txn) error {
		for i := 0; i < 20000; i++ {
			txn.Insert(func(r Row) error {
				return r.SetMany(map[string]any{
					"col1": fmt.Sprint(i % 3),
				})
			})
		}
		return nil
	}))

	assert.NoError(t, c.Query(func(txn *Txn) error {
		col1 := txn.String("col1")
		col2 := txn.Float64("col2")
		col3 := txn.String("col3")

		assert.NoError(t, txn.Range(func(index uint32) {
			col2.Set(0)
		}))
		return txn.Range(func(index uint32) {
			value, _ := col1.Get()
			if a, h := someMap[value]; h {
				col2.Set(a[1].(float64))
				col3.Set(a[0].(string))
			}
		})
	}))
}

func TestUpdateAt(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())
	index, err := c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{
			"col1": "hello",
		})
	})

	assert.NoError(t, err)
	assert.NoError(t, c.QueryAt(index, func(r Row) error {
		r.SetString("col1", "hi")
		return nil
	}))
}

func TestUpdateAtInvalid(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.Panics(t, func() {
		c.QueryAt(0, func(r Row) error {
			r.SetString("col2", "hi")
			return nil
		})
	})
}
func TestUpdateAtNoChanges(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("col1", ForString())

	assert.NoError(t, c.QueryAt(20000, func(r Row) error {
		r.SetString("col1", "Roman")
		return nil
	}))

	assert.NoError(t, c.QueryAt(0, func(r Row) error {
		r.txn.bufferFor("xxx").PutInt(commit.Put, 123, 123)
		return nil
	}))
}

func TestUpsertKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("val", ForString())
	assert.NoError(t, c.UpsertKey("1", func(r Row) error {
		r.SetString("val", "Roman")
		return nil
	}))

	count := 0
	assert.NoError(t, c.UpsertKey("1", func(r Row) error {
		count++
		return nil
	}))

	assert.Equal(t, 1, count)
}

func TestUpsertKeyNoColumn(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())

	assert.Panics(t, func() {
		c.UpsertKey("1", func(r Row) error {
			r.Enum("xxx")
			return nil
		})
	})
}

func TestDeleteKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("val", ForString())
	assert.NoError(t, c.InsertKey("1", func(r Row) error {
		r.SetString("val", "Roman")
		return nil
	}))

	// Only one should succeed
	assert.NoError(t, c.DeleteKey("1"))
	assert.Error(t, c.DeleteKey("1"))
	assert.Equal(t, 0, c.Count())
}

func TestInsertKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())

	// Only one should succeed
	assert.NoError(t, c.InsertKey("1", func(r Row) error {
		return nil
	}))
	assert.Error(t, c.InsertKey("1", func(r Row) error {
		return nil
	}))
	assert.Equal(t, 1, c.Count())
}

func TestQueryKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("val", ForString())

	assert.Error(t, c.QueryKey("1", func(r Row) error {
		return nil
	}))

	assert.NoError(t, c.InsertKey("1", func(r Row) error {
		r.SetString("val", "Roman")
		return nil
	}))

	assert.NoError(t, c.QueryKey("1", func(r Row) error {
		return nil
	}))
}

func TestChangeKey(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())

	// Try to change the key from "1" to "2"
	assert.NoError(t, c.InsertKey("1", func(r Row) error { return nil }))
	assert.NoError(t, c.QueryKey("1", func(r Row) error {
		r.SetKey("2")
		return nil
	}))

	// Must now have "2"
	assert.NoError(t, c.QueryKey("2", func(r Row) error { return nil }))
	assert.Equal(t, 1, c.Count())
}

func TestRollbackInsert(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("name", ForString()))

	// Insert successfully
	idx0, err := col.Insert(func(r Row) error {
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(0), idx0)

	// Insert with error
	idx1, err := col.Insert(func(r Row) error {
		return fmt.Errorf("error")
	})
	assert.Error(t, err)
	assert.Equal(t, uint32(1), idx1)

	// Should only have 1 element
	assert.Equal(t, 1, col.Count())
}

func TestUnkeyedInsert(t *testing.T) {
	col := NewCollection()
	assert.NoError(t, col.CreateColumn("key", ForKey()))

	// Insert should fail, as one should use InsertKey() method
	_, err := col.Insert(func(r Row) error {
		return nil
	})
	assert.Error(t, err)
}

func TestDuplicateKeyColumn(t *testing.T) {
	c := NewCollection()
	assert.NoError(t, c.CreateColumn("key1", ForKey()))
	assert.Error(t, c.CreateColumn("key2", ForKey()))
}

func TestRowMethods(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("key", ForKey())
	c.CreateColumn("bool", ForBool())
	c.CreateColumn("name", ForString())
	c.CreateColumn("int", ForInt())
	c.CreateColumn("int16", ForInt16())
	c.CreateColumn("int32", ForInt32())
	c.CreateColumn("int64", ForInt64())
	c.CreateColumn("uint", ForUint())
	c.CreateColumn("uint16", ForUint16())
	c.CreateColumn("uint32", ForUint32())
	c.CreateColumn("uint64", ForUint64())
	c.CreateColumn("float32", ForFloat32())
	c.CreateColumn("float64", ForFloat64())

	c.InsertKey("key", func(r Row) error {
		r.SetBool("bool", true)
		r.SetAny("name", "Roman")

		// Numbers
		r.SetInt("int", 1)
		r.SetInt16("int16", 1)
		r.SetInt32("int32", 1)
		r.SetInt64("int64", 1)
		r.SetUint("uint", 1)
		r.SetUint16("uint16", 1)
		r.SetUint32("uint32", 1)
		r.SetUint64("uint64", 1)
		r.SetFloat32("float32", 1)
		r.SetFloat64("float64", 1)

		// Increment
		r.MergeInt("int", 1)
		r.MergeInt16("int16", 1)
		r.MergeInt32("int32", 1)
		r.MergeInt64("int64", 1)
		r.MergeUint("uint", 1)
		r.MergeUint16("uint16", 1)
		r.MergeUint32("uint32", 1)
		r.MergeUint64("uint64", 1)
		r.MergeFloat32("float32", 1)
		r.MergeFloat64("float64", 1)
		return nil
	})

	exists := func(v interface{}, ok bool) {
		assert.NotNil(t, v)
		assert.True(t, ok)
	}

	c.QueryKey("key", func(r Row) error {
		assert.True(t, r.Bool("bool"))
		exists(r.Key())
		exists(r.Any("name"))
		exists(r.Int("int"))
		exists(r.Int16("int16"))
		exists(r.Int32("int32"))
		exists(r.Int64("int64"))
		exists(r.Uint("uint"))
		exists(r.Uint16("uint16"))
		exists(r.Uint32("uint32"))
		exists(r.Uint64("uint64"))
		exists(r.Float32("float32"))
		exists(r.Float64("float64"))
		return nil
	})
}

func TestRow(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("name", ForKey())

	var wg sync.WaitGroup
	wg.Add(2)

	go c.Query(func(txn *Txn) error {
		txn.InsertKey("Roman", func(r Row) error {
			return nil
		})
		wg.Done()
		return nil
	})

	go c.Query(func(txn *Txn) error {
		txn.With("human").Count()
		wg.Done()
		return nil
	})

	wg.Wait()
}

func TestUnion(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("d_a", ForString())

	c.CreateIndex("d_a_1", "d_a", func(r Reader) bool { return r.String() == "1" })
	c.CreateIndex("d_a_2", "d_a", func(r Reader) bool { return r.String() == "2" })
	c.CreateIndex("d_a_3", "d_a", func(r Reader) bool { return r.String() == "on99" })

	c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{"d_a": "1"})
	})

	c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{"d_a": "2"})
	})

	c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{"d_a": "on99"})
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 1, txn.Union("d_a_1").Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Union("d_a_1").Union("d_a_2").Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 2, txn.Union("d_a_2", "d_a_3").Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		assert.Equal(t, 3, txn.Union("d_a_1", "d_a_2").Union("d_a_3").Count())
		return nil
	})
}

func TestWithUnion(t *testing.T) {
	c := NewCollection()
	c.CreateColumn("tester", ForString())
	c.CreateColumn("testerB", ForString())

	c.CreateIndex("tester_1", "tester", func(r Reader) bool { return r.String() == "1" })
	c.CreateIndex("tester_2", "tester", func(r Reader) bool { return r.String() == "2" })
	c.CreateIndex("tester_3", "tester", func(r Reader) bool { return r.String() == "3" })
	c.CreateIndex("testerB_4", "testerB", func(r Reader) bool { return r.String() == "4" })
	c.CreateIndex("testerB_5", "testerB", func(r Reader) bool { return r.String() == "5" })
	c.CreateIndex("testerB_6", "testerB", func(r Reader) bool { return r.String() == "6" })

	c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{
			"tester":  "1",
			"testerB": "4",
		})
	})

	c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{
			"tester":  "2",
			"testerB": "5",
		})
	})

	c.Insert(func(r Row) error {
		return r.SetMany(map[string]any{
			"tester":  "3",
			"testerB": "6",
		})
	})

	// account for normal use-case
	c.Query(func(txn *Txn) error {
		txn.WithUnion("tester_1", "tester_2")
		txn.Union("testerB_5", "testerB_6")

		assert.Equal(t, 3, txn.Count())
		return nil
	})

	// where tester in ['1', '2'] and testerB in ['5', '6']
	c.Query(func(txn *Txn) error {
		txn.Union("tester_1", "tester_2")
		txn.WithUnion("testerB_5", "testerB_6")

		assert.Equal(t, 1, txn.Count())
		return nil
	})

	c.Query(func(txn *Txn) error {
		txn.Without("tester_1", "testerB_5")
		txn.WithUnion("tester_2", "tester_1", "tester_3")

		assert.Equal(t, 1, txn.Count())
		return nil
	})
}

func TestWithUnionPlayers(t *testing.T) {
	trueCount := 0
	players := loadPlayers(100000)

	players.Query(func(txn *Txn) error {
		ageCol := txn.Any("age")
		raceCol := txn.Any("race")
		classCol := txn.Any("class")

		return txn.Range(func(i uint32) {
			age, _ := ageCol.Get()
			race, _ := raceCol.Get()
			class, _ := classCol.Get()

			if race == "dwarf" && (age.(int) >= 30.0 || class == "mage") {
				trueCount++
			}
		})
	})

	players.Query(func(txn *Txn) error {
		txn.With("dwarf")
		txn.WithUnion("mage", "old")

		assert.Equal(t, trueCount, txn.Count())
		return nil
	})

	players.Query(func(txn *Txn) error {
		txn.With("dwarf", "mage", "old")
		assert.True(t, txn.Count() < trueCount)
		return nil
	})

	players.Query(func(txn *Txn) error {
		txn.Union("dwarf", "mage", "old")
		assert.True(t, txn.Count() > trueCount)
		return nil
	})

	// dwarf & elf cancel out
	players.Query(func(txn *Txn) error {
		txn.With("dwarf")
		txn.WithUnion("mage", "old", "elf")
		assert.Equal(t, trueCount, txn.Count())
		return nil
	})
}

func TestSumBalance(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	players.Query(func(txn *Txn) error {
		sum := int(txn.Float64("balance").Sum())
		assert.Equal(t, 1212084, sum)
		return nil
	})

	players.Query(func(txn *Txn) error {
		sum := int(txn.With("old", "mage").Float64("balance").Sum())
		assert.Equal(t, 186440, sum)
		return nil
	})
}

func TestAvgBalance(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	players.Query(func(txn *Txn) error {
		sum := int(txn.Float64("balance").Avg())
		assert.Equal(t, 2424, sum)
		return nil
	})

	players.Query(func(txn *Txn) error {
		sum := int(txn.With("old", "mage").Float64("balance").Avg())
		assert.Equal(t, 2421, sum)
		return nil
	})
}

func TestMinBalance(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	players.Query(func(txn *Txn) error {
		min, ok := txn.Float64("balance").Min()
		assert.Equal(t, float64(1010.06), min)
		assert.True(t, ok)
		return nil
	})

	players.Query(func(txn *Txn) error {
		min, ok := txn.With("old", "mage", "human").Float64("balance").Min()
		assert.Equal(t, float64(1023.76), min)
		assert.True(t, ok)
		return nil
	})
}

func TestMaxBalance(t *testing.T) {
	players := loadPlayers(500)
	assert.Equal(t, 500, players.Count())

	players.Query(func(txn *Txn) error {
		max, ok := txn.Float64("balance").Max()
		assert.Equal(t, float64(3982.14), max)
		assert.True(t, ok)
		return nil
	})

	players.Query(func(txn *Txn) error {
		max, ok := txn.With("old", "mage", "human").Float64("balance").Max()
		assert.Equal(t, float64(3978.83), max)
		assert.True(t, ok)
		return nil
	})
}

func TestSetManyErr(t *testing.T) {
	players := loadPlayers(500)
	t.Run("invalid", func(t *testing.T) {
		_, err := players.Insert(func(r Row) error {
			return r.SetMany(map[string]any{
				"invalid": 1,
			})
		})
		assert.Error(t, err)
	})

	t.Run("write", func(t *testing.T) {
		_, err := players.Insert(func(r Row) error {
			return r.SetMany(map[string]any{
				"age": complex64(1),
			})
		})
		assert.Error(t, err)
	})
}
