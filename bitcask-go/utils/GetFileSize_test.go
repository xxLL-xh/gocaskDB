package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetDirSize(t *testing.T) {
	dir := "/tmp/kv"
	dirSize, err := GetDirSize(dir)
	assert.Nil(t, err)
	assert.True(t, dirSize >= 0)
	t.Log(dirSize)
}
