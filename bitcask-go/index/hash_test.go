package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewSafeHashTable_Put(t *testing.T) {
	hash := NewSafeHashTable()
	res1 := hash.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)
	res2 := hash.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := hash.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res3)
	res4 := hash.Put([]byte("key-3"), &data.LogRecordPos{Fid: 99, Offset: 88})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(12), res4.Offset)
}

func TestNewSafeHashTable_Get(t *testing.T) {
	hash := NewSafeHashTable()
	hash.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := hash.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := hash.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	hash.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1123, Offset: 990})
	pos2 := hash.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

func TestNewSafeHashTable_Delete(t *testing.T) {
	hash := NewSafeHashTable()

	res1, ok1 := hash.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	hash.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2, ok2 := hash.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(12), res2.Offset)

	pos := hash.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestNewSafeHashTable_Size(t *testing.T) {
	hash := NewSafeHashTable()

	assert.Equal(t, 0, hash.Size())

	hash.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	hash.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	hash.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 2, hash.Size())
}
