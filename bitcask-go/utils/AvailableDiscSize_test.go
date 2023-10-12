package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAvailableDiscSize(t *testing.T) {
	size, err := AvailableDiscSize()
	assert.Nil(t, err)
	t.Log(size / 1024 / 1024 / 1024)
}
