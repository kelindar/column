# Columnar Collections & Querying

This package is my experimental attempt in building a fast, in-memory columnar collections in Go. The basic
idea is to arrange property bags (i.e. `map[string]interface{}`) into columns and be able to write queries effiently 
around them. Under the hood, this uses roaring bitmaps extensively to provide fast comparisons and selection.

## Example usage

```go
humanMageCount := players.
    Where(func(v interface{}) bool {
        return v.(string) == "human"
    }, "race").
    Where(func(v interface{}) bool {
        return v.(string) == "mage"
    }, "class").
    Count()

```