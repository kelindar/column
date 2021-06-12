package column

import (
	"testing"

	"github.com/kelindar/bitmap"
	"github.com/stretchr/testify/assert"
)

func TestOfnumbers(t *testing.T) {
	c := makenumbers().(*columnnumber)

	{ // Set the value at index
		c.Update(9, number(99))
		assert.Equal(t, 10, len(c.data))
		assert.True(t, c.Contains(9))
	}

	{ // Get the values
		v, ok := c.Value(9)
		assert.Equal(t, number(99), v)
		assert.True(t, ok)

		f, ok := c.Float64(9)
		assert.Equal(t, float64(99), f)
		assert.True(t, ok)

		i, ok := c.Int64(9)
		assert.Equal(t, int64(99), i)
		assert.True(t, ok)

		u, ok := c.Uint64(9)
		assert.Equal(t, uint64(99), u)
		assert.True(t, ok)
	}

	{
		other := bitmap.Bitmap{0xffffffffffffffff}
		c.And(&other)
		assert.Equal(t, uint64(0b1000000000), other[0])
	}

	{
		other := bitmap.Bitmap{0xffffffffffffffff}
		c.AndNot(&other)
		assert.Equal(t, uint64(0xfffffffffffffdff), other[0])
	}

	{
		other := bitmap.Bitmap{0xffffffffffffffff}
		c.Or(&other)
		assert.Equal(t, uint64(0xffffffffffffffff), other[0])
	}

	{ // Remove the value
		c.Delete(9)
		c.DeleteMany(&bitmap.Bitmap{0xffffffffffffffff})

		v, ok := c.Value(9)
		assert.Equal(t, number(0), v)
		assert.False(t, ok)

		f, ok := c.Float64(9)
		assert.Equal(t, float64(0), f)
		assert.False(t, ok)

		i, ok := c.Int64(9)
		assert.Equal(t, int64(0), i)
		assert.False(t, ok)

		u, ok := c.Uint64(9)
		assert.Equal(t, uint64(0), u)
		assert.False(t, ok)
	}
}
