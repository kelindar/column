package column

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/zeebo/xxh3"
)

type rlockedMap struct {
	sync.RWMutex
	data map[string]Column
}

func (m *rlockedMap) Range(fn func(string, Column) bool) {
	m.RLock()
	for k, v := range m.data {
		fn(k, v)
	}
	m.RUnlock()
}

func (m *rlockedMap) Load(key string) (c Column, ok bool) {
	m.RLock()
	c, ok = m.data[key]
	m.RUnlock()
	return
}

var _ = xxh3.HashString("test")

// ----------------

type atomicSet struct {
	data atomic.Value
}

type entry struct {
	key   uint64
	value Column
}

func (m *atomicSet) Range(fn func(Column) bool) {
	items := m.data.Load().([]entry)
	for _, v := range items {
		fn(v.value)
	}
}

func (m *atomicSet) Load(key string) (c Column, ok bool) {
	hashkey := xxh3.HashString(key)
	items := m.data.Load().([]entry)

	for _, v := range items {
		if hashkey == v.key {
			return v.value, true
		}
	}
	return nil, false
}

func (m *atomicSet) Store(key string, v Column) {
	hashkey := xxh3.HashString(key)
	items := m.data.Load()
	if items == nil {
		items = []entry{}
	}

	items = append(items.([]entry), entry{
		key:   hashkey,
		value: v,
	})
	m.data.Store(items)
	return
}

// BenchmarkMap/load-2-8         	59410940	        20.33 ns/op	       0 B/op	       0 allocs/op
// BenchmarkMap/load-6-8         	100000000	        11.97 ns/op	       0 B/op	       0 allocs/op
// BenchmarkMap/load-7-8         	66665925	        18.09 ns/op	       0 B/op	       0 allocs/op
// BenchmarkMap/range-2-8        	 6055888	       198.2 ns/op	       0 B/op	       0 allocs/op
// BenchmarkMap/range-6-8        	35293390	        32.07 ns/op	       0 B/op	       0 allocs/op
// BenchmarkMap/range-7-8        	23776123	        48.15 ns/op	       0 B/op	       0 allocs/op
func BenchmarkMap(b *testing.B) {
	map2 := rlockedMap{data: map[string]Column{}}
	map6 := atomicSet{}
	map7 := makeColumns(20)

	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("column_%v", i)
		map2.data[name] = makeAny()
		map6.Store(name, makeAny())
		map7.Store(name, makeAny())
	}

	b.Run("load-2", func(b *testing.B) {
		var v interface{}
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			v, _ = map2.Load("column_10")
		}
		assert.NotNil(b, v)
	})

	b.Run("load-6", func(b *testing.B) {
		var v interface{}
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			v, _ = map6.Load("column_10")
		}
		assert.NotNil(b, v)
	})

	b.Run("load-7", func(b *testing.B) {
		var v interface{}
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			v, _ = map7.Load("column_10")
		}
		assert.NotNil(b, v)
	})

	b.Run("range-2", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			map2.Range(func(s string, c Column) bool {
				return true
			})
		}
	})

	b.Run("range-6", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			map6.Range(func(c Column) bool {
				return true
			})
		}
	})

	b.Run("range-7", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			map7.Range(func(c Column) {
				return
			})
		}
	})
}

func toBytes(v string) (b []byte) {
	strHeader := (*reflect.StringHeader)(unsafe.Pointer(&v))
	byteHeader := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	byteHeader.Data = strHeader.Data

	l := len(v)
	byteHeader.Len = l
	byteHeader.Cap = l
	return
}
