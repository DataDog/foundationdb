package fdb

import "testing"
import "github.com/stretchr/testify/assert"

func TestSnapshotOptions(t *testing.T) {
	MustAPIVersion(400)
	db := MustOpenDefault()
	tr, err := db.CreateTransaction()
	assert.NoError(t, err)
	snapshot := tr.Snapshot()
	snapshot.Options()
}
