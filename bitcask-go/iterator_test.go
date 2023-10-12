package bitcask_go

import (
	"bitcask-go/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/kv/DB-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/kv/DB-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/kv/DB-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("user1"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("user2"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("user3"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("admin"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("provider1"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("provider2"), utils.RandomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("p")); iter1.Valid(); iter1.Next() {
		//t.Log(fmt.Printf("keys after 'p':%v", string(iter1.Key())))
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("p")); iter2.Valid(); iter2.Next() {
		//t.Log(fmt.Printf("keys before 'p':%v", string(iter2.Key())))
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// 指定了 prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("user")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log(fmt.Printf("keys with prefix 'user':%v", string(iter3.Key())))
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}

func TestDB_Iterator(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/kv/DB-iterator"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 5; i++ {
		key := fmt.Sprint("User-", i)
		err = db.Put([]byte(key), utils.RandomValue(10))
		assert.Nil(t, err)
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprint("Provider-", i)
		err = db.Put([]byte(key), utils.RandomValue(10))
		assert.Nil(t, err)
	}

	Keys := db.ListKeys()
	strs := make([]string, len(Keys))
	for i := 0; i < len(Keys); i++ {
		strs[i] = string(Keys[i])
	}
	t.Log(fmt.Printf("List Keys: %v", strs))

	// 正向迭代
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	t.Log("Iterate Forward")
	for iter1.Seek([]byte("Provider-3")); iter1.Valid(); iter1.Next() {
		t.Log(fmt.Printf("keys after 'Provider-3':%v", string(iter1.Key())))
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	t.Log("Iterate Backward")
	for iter2.Seek([]byte("Provider-3")); iter2.Valid(); iter2.Next() {
		t.Log(fmt.Printf("keys before 'Provider-3':%v", string(iter2.Key())))
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()
	t.Log("prefix query")
	// 指定了 prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("User")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log(fmt.Printf("keys with prefix 'User':%v", string(iter3.Key())))
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
