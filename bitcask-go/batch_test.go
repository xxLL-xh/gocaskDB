package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/kv/DB-batch"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后并不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.PendingPut(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	// 不存在，直接返回
	err = wb.PendingDelete(utils.GetTestKey(2))
	assert.Nil(t, err)
	// 事务还没提交，Get不到key为1的数据
	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	// 提交之后可以Get到了
	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.PendingDelete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/kv/DB-batch2"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.PendingPut(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.PendingDelete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.PendingPut(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// restart
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// check the seqNo
	assert.Equal(t, uint64(2), db.seqNo)
}

//func TestDB_WriteBatch3(t *testing.T) {
//	opts := DefaultOptions
//	//dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
//	dir := "/tmp/bitcask-go-batch-3"
//	opts.DirPath = dir
//	db, err := Open(opts)
//	//defer destroyDB(db)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	keys := db.ListKeys()
//	t.Log(len(keys))
//	//
//	//wbOpts := DefaultWriteBatchOptions
//	//wbOpts.MaxBatchNum = 10000000
//	//wb := db.NewWriteBatch(wbOpts)
//	//for i := 0; i < 500000; i++ {
//	//	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
//	//	assert.Nil(t, err)
//	//}
//	//err = wb.Commit()
//	//assert.Nil(t, err)
//}
