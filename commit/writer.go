// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package commit

// Writer represents a contract that a commit writer must implement
type Writer interface {
	Write(commit Commit) error
}

// --------------------------- Channel ----------------------------

var _ Writer = new(Channel)

// Channel represents an impementation of a commit writer that simply sends each commit
// into the channel.
type Channel chan Commit

// Write clones the commit and writes it into the writer
func (w *Channel) Write(commit Commit) error {
	*w <- commit.Clone()
	return nil
}
