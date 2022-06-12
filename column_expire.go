// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package column

import (
	"context"
	"time"
)

// --------------------------- Expiration (Vacuum) ----------------------------

// vacuum cleans up the expired objects on a specified interval.
func (c *Collection) vacuum(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ctx.Done():
			ticker.Stop()
			return
		case <-ticker.C:
			c.Query(func(txn *Txn) error {
				ttl, now := txn.TTL(), time.Now()
				return txn.With(expireColumn).Range(func(idx uint32) {
					if expiresAt, ok := ttl.ExpiresAt(); ok && now.After(expiresAt) {
						txn.DeleteAt(idx)
					}
				})
			})
		}
	}
}

// --------------------------- Expiration (Column) ----------------------------

// TTL returns a read-write accessor for the time-to-live column
func (txn *Txn) TTL() ttlWriter {
	return ttlWriter{
		rw: int64Writer{
			numericReader: numericReaderFor[int64](txn, expireColumn),
			writer:        txn.bufferFor(expireColumn),
		},
	}
}

type ttlWriter struct {
	rw int64Writer
}

// TTL returns the remaining time-to-live duration
func (s ttlWriter) TTL() (time.Duration, bool) {
	if expireAt, ok := s.rw.Get(); ok && expireAt != 0 {
		return readTTL(expireAt), true
	}
	return 0, false
}

// ExpiresAt returns the expiration time
func (s ttlWriter) ExpiresAt() (time.Time, bool) {
	if expireAt, ok := s.rw.Get(); ok && expireAt != 0 {
		return time.Unix(0, expireAt), true
	}
	return time.Time{}, false
}

// Set sets the time-to-live value at the current transaction cursor
func (s ttlWriter) Set(ttl time.Duration) {
	s.rw.Set(writeTTL(ttl))
}

// Extend extends time-to-live of the row current transaction cursor by a specified amount
func (s ttlWriter) Extend(delta time.Duration) {
	s.rw.Add(int64(delta.Nanoseconds()))
}

// readTTL converts expiration to a TTL
func readTTL(expireAt int64) time.Duration {
	return time.Unix(0, expireAt).Sub(time.Now())
}

// writeTTL converts ttl to expireAt
func writeTTL(ttl time.Duration) int64 {
	if ttl > 0 {
		return time.Now().Add(ttl).UnixNano()
	}
	return 0
}

// --------------------------- Expiration (Row) ----------------------------

// TTL retrieves the time left before the row will be cleaned up
func (r Row) TTL() (time.Duration, bool) {
	if expireAt, ok := r.Int64(expireColumn); ok {
		return readTTL(expireAt), true
	}
	return 0, false
}

// SetTTL sets a time-to-live for a row
func (r Row) SetTTL(ttl time.Duration) {
	if ttl > 0 {
		r.SetInt64(expireColumn, time.Now().Add(ttl).UnixNano())
	} else {
		r.SetInt64(expireColumn, 0)
	}
}
