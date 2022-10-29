// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package opt

// --------------------------- String ----------------------------

// String represents options for various string-based columns.
type String struct {
	Merge func(value string, delta string) string
}

// init sets the default behavior.
func (s *String) init() {
	s.Merge = func(value, delta string) string { return delta }
}

// WithStringMerge sets an optional merge function that allows you to merge a delta value to
// an existing value, atomically. The operation is performed transactionally. If not specified
// merge function will act the same as a normal store and will overwrite.
func WithStringMerge(fn func(value string, delta string) string) func(*String) {
	return func(v *String) {
		v.Merge = fn
	}
}

// --------------------------- Configuration ----------------------------

// Configure initializes and creates a new options structure.
func Configure[T any](opts ...func(*T)) T {
	options := new(T)

	// If options needs to be initialized, call the init() method
	var x any = options
	if v, ok := x.(interface {
		init()
	}); ok {
		v.init()
	}

	// Apply options provided
	for _, opt := range opts {
		opt(options)
	}
	return *options
}
