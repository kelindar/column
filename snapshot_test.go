// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/kelindar/async"
	"github.com/kelindar/column/commit"
	"github.com/stretchr/testify/assert"
)

/*
cpu: Intel(R) Core(TM) i7-9700K CPU @ 3.60GHz
BenchmarkSave/write-to-8         	       8	 131800350 ns/op	 981.98 MB/s	 6539521 B/op	    1950 allocs/op
BenchmarkSave/read-from-8        	      13	  79411685 ns/op	1629.80 MB/s	135661336 B/op	    4610 allocs/op
*/
func BenchmarkSave(b *testing.B) {
	b.Run("write-state", func(b *testing.B) {
		output := bytes.NewBuffer(nil)
		input := loadPlayers(1e6)

		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			output.Reset()
			n, _ := input.writeState(output)
			b.SetBytes(n)
		}
	})

	b.Run("read-state", func(b *testing.B) {
		buffer := bytes.NewBuffer(nil)
		output := NewCollection()
		input := loadPlayers(1e6)
		input.writeState(buffer)

		runtime.GC()
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			output.readState(bytes.NewBuffer(buffer.Bytes()))
			b.SetBytes(int64(buffer.Len()))
		}
	})
}

// --------------------------- Streaming ----------------------------

// Test replication many times
func TestReplicate(t *testing.T) {
	for x := 0; x < 20; x++ {
		rand.Seed(int64(x))
		runReplication(t, 10000, 50, runtime.NumCPU())
	}
}

// runReplication runs a concurrent replication test
func runReplication(t *testing.T, updates, inserts, concurrency int) {
	t.Run(fmt.Sprintf("replicate-%v-%v", updates, inserts), func(t *testing.T) {
		writer := make(commit.Channel, 10)
		object := map[string]interface{}{
			"float64": float64(0),
			"int32":   int32(0),
			"string":  "",
		}

		// Create a primary
		primary := NewCollection(Options{
			Capacity: inserts,
			Writer:   &writer,
		})
		// Replica with the same schema
		replica := NewCollection(Options{
			Capacity: inserts,
		})

		// Create schemas and start streaming replication into the replica
		primary.CreateColumnsOf(object)
		replica.CreateColumnsOf(object)
		var done sync.WaitGroup
		done.Add(1)
		go func() {
			defer done.Done() // Drained
			for change := range writer {
				assert.NoError(t, replica.Replay(change))
			}
		}()

		// Write some objects
		for i := 0; i < inserts; i++ {
			primary.InsertObject(object)
		}

		work := make(chan async.Task)
		pool := async.Consume(context.Background(), 50, work)
		defer pool.Cancel()

		// Random concurrent updates
		var wg sync.WaitGroup
		wg.Add(updates)
		for i := 0; i < updates; i++ {
			work <- async.NewTask(func(ctx context.Context) (interface{}, error) {
				defer wg.Done()

				// Randomly update a column
				offset := uint32(rand.Int31n(int32(inserts - 1)))
				primary.UpdateAt(offset, "float64", func(v Cursor) error {
					switch rand.Int31n(3) {
					case 0:
						v.SetFloat64(math.Round(rand.Float64()*1000) / 100)
					case 1:
						v.SetInt32At("int32", rand.Int31n(100000))
					case 2:
						v.SetStringAt("string", fmt.Sprintf("hi %v", rand.Int31n(10)))
					}
					return nil
				})

				// Randomly delete an item
				if rand.Int31n(5) == 0 {
					primary.DeleteAt(uint32(rand.Int31n(int32(inserts - 1))))
				}

				// Randomly insert an item
				if rand.Int31n(5) == 0 {
					primary.InsertObject(object)
				}
				return nil, nil
			})
		}

		// Replay all of the changes into the replica
		wg.Wait()
		close(writer)
		done.Wait()

		// Check if replica and primary are the same
		if !assert.Equal(t, primary.Count(), replica.Count(), "replica and primary should be the same size") {
			return
		}

		primary.Query(func(txn *Txn) error {
			return txn.Range("float64", func(v Cursor) {
				v1, v2 := v.FloatAt("float64"), v.IntAt("int32")
				if v1 != 0 {
					assert.True(t, txn.SelectAt(v.idx, func(s Selector) {
						assert.Equal(t, v.FloatAt("float64"), s.FloatAt("float64"))
					}))
				}

				if v2 != 0 {
					assert.True(t, txn.SelectAt(v.idx, func(s Selector) {
						assert.Equal(t, v.IntAt("int32"), s.IntAt("int32"))
					}))
				}
			})
		})
	})
}

// --------------------------- Snapshotting ----------------------------

func TestSnapshot(t *testing.T) {
	amount := 50000
	buffer := bytes.NewBuffer(nil)
	input := loadPlayers(amount)

	var wg sync.WaitGroup
	wg.Add(amount)
	go func() {
		for i := 0; i < amount; i++ {
			assert.NoError(t, input.UpdateAt(uint32(i), "name", func(v Cursor) error {
				v.SetString("Roman")
				return nil
			}))
			wg.Done()
		}
	}()

	// Start snapshotting
	assert.NoError(t, input.Snapshot(buffer))
	assert.NotZero(t, buffer.Len())

	// Restore the snapshot
	wg.Wait()
	output := newEmpty(amount)
	assert.NoError(t, output.Restore(buffer))
	assert.Equal(t, amount, output.Count())
}

func TestSnapshotFailures(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())
	input.Insert("name", func(v Cursor) error {
		v.Set("Roman")
		return nil
	})

	go input.Insert("name", func(v Cursor) error {
		v.Set("Roman")
		return nil
	})

	for size := 0; size < 80; size++ {
		output := &limitWriter{Limit: size}

		assert.Error(t, input.Snapshot(output),
			fmt.Sprintf("write failure size=%d", size))
	}
}

func TestRestoreIncomplete(t *testing.T) {
	buffer := bytes.NewBuffer(nil)
	output := newEmpty(500)
	assert.Error(t, output.Restore(buffer))
}

func TestSnapshotFailedAppendCommit(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())
	input.record = commit.Open(&limitWriter{Limit: 0})
	_, err := input.Insert("name", func(v Cursor) error {
		v.SetString("Roman")
		return nil
	})
	assert.NoError(t, err)
}

// --------------------------- State Codec ----------------------------

func TestWriteTo(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForEnum())
	for i := 0; i < 2e4; i++ {
		input.Insert("name", func(v Cursor) error {
			v.Set("Roman")
			return nil
		})
	}

	// Write a snapshot into a buffer
	buffer := bytes.NewBuffer(nil)
	n, err := input.writeState(buffer)
	assert.NotZero(t, n)
	assert.NoError(t, err)

	// Restore the collection from the snapshot
	output := NewCollection()
	output.CreateColumn("name", ForEnum())
	m, err := output.readState(buffer)
	assert.NotEmpty(t, m)
	assert.NoError(t, err)
	assert.Equal(t, input.Count(), output.Count())

	output.SelectAt(0, func(v Selector) {
		assert.Equal(t, "Roman", v.StringAt("name"))
	})
}

func TestCollectionCodec(t *testing.T) {
	input := loadPlayers(5e4)

	// Write a snapshot into a buffer
	buffer := bytes.NewBuffer(nil)
	n, err := input.writeState(buffer)
	assert.NotZero(t, n)
	assert.NoError(t, err)

	// Restore the collection from the snapshot
	output := newEmpty(5e4)
	m, err := output.readState(buffer)
	assert.NotEmpty(t, m)
	assert.NoError(t, err)
	assert.Equal(t, input.Count(), output.Count())
}

func TestWriteToSizeUncompresed(t *testing.T) {
	input := loadPlayers(1e4) // 10K
	output := bytes.NewBuffer(nil)
	_, err := input.writeState(output)
	assert.NoError(t, err)
	assert.Equal(t, 1264179, output.Len())
}

func TestWriteToFailures(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())
	input.Insert("name", func(v Cursor) error {
		v.Set("Roman")
		return nil
	})

	for size := 0; size < 69; size++ {
		output := &limitWriter{Limit: size}
		_, err := input.writeState(output)
		assert.Error(t, err, fmt.Sprintf("write failure size=%d", size))
	}
}

func TestWriteEmpty(t *testing.T) {
	buffer := bytes.NewBuffer(nil)

	{ // Write the collection
		input := NewCollection()
		input.CreateColumn("name", ForString())
		_, err := input.writeState(buffer)
		assert.NoError(t, err)
	}

	{ // Read the collection back
		output := NewCollection()
		output.CreateColumn("name", ForString())
		_, err := output.readState(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 0, output.Count())
	}
}

func TestReadFromFailures(t *testing.T) {
	input := NewCollection()
	input.CreateColumn("name", ForString())
	input.Insert("name", func(v Cursor) error {
		v.Set("Roman")
		return nil
	})

	buffer := bytes.NewBuffer(nil)
	_, err := input.writeState(buffer)
	assert.NoError(t, err)

	for size := 0; size < buffer.Len()-1; size++ {
		output := NewCollection()

		output.CreateColumn("name", ForString())
		_, err := output.readState(bytes.NewReader(buffer.Bytes()[:size]))
		assert.Error(t, err, fmt.Sprintf("read size %v", size))
	}
}

// --------------------------- Mocks & Fixtures ----------------------------

// noopWriter is a writer that simply counts the commits
type noopWriter struct {
	commits uint64
}

// Write clones the commit and writes it into the writer
func (w *noopWriter) Append(commit commit.Commit) error {
	atomic.AddUint64(&w.commits, 1)
	return nil
}

// limitWriter is a io.Writer that allows for limiting input
type limitWriter struct {
	value uint32
	Limit int
}

// Write returns either an error or no error, depending on whether the limit is reached
func (w *limitWriter) Write(p []byte) (int, error) {
	if n := atomic.AddUint32(&w.value, uint32(len(p))); int(n) > w.Limit {
		return 0, io.ErrShortBuffer
	}
	return len(p), nil
}

func (w *limitWriter) Read(p []byte) (int, error) {
	return 0, nil
}
