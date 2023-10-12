package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func Test_MergeAllDataValidOrInvalid(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/DB-mergeTest"
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatioThreshold = 0

	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 5*32*1024; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 4*32*1024; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	fid, err := db.GetFirstNonMergedFid("/tmp/DB-mergeTest")
	assert.Nil(t, err)
	t.Log("FirstNonMergedFid in merged-mark file:", fid)

	// 重启校验
	err = db.Close()
	assert.Nil(t, err)
}

func Test_MergeSomeDataValid(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/DB-mergeTest"
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatioThreshold = 0

	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	/*for i := 0; i < 5*32*1024; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 2*32*1024; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 2 * 32 * 1024; i < 4*32*1024; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}*/

	/*err = db.Merge()
	assert.Nil(t, err)*/

	fid, err := db.GetFirstNonMergedFid("/tmp/DB-mergeTest")
	assert.Nil(t, err)
	t.Log("FirstNonMergedFid in merged-mark file:", fid)

	/*// 重启校验
	err = db.Close()
	assert.Nil(t, err)*/
}

func Test_PutDuringMerge(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/DB-mergeTest"
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatioThreshold = 0

	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 20000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 20000; i < 40000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	keys := db.ListKeys()
	t.Log("size of valid keys:", len(keys))

	fid, err := db.GetFirstNonMergedFid("/tmp/DB-mergeTest")
	assert.Nil(t, err)
	t.Log("FirstNonMergedFid in merged-mark file:", fid)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 40000; i < 50000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
		for i := 50000; i < 80000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}

	}()
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

}

func Test_MergeRatioThreshold(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = "/tmp/DB-mergeTest"
	opts.DataFileSize = 32 * 1024 * 1024
	opts.MergeRatioThreshold = 0.4

	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 5*32*1024; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 2*29*1024; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	t.Log("invalid size:", db.invalidSize)
	// 检查是否达到了merge的阈值
	totalSize, _ := utils.GetDirSize(db.options.DirPath)
	t.Log("total size occupied by gocaskDB:", totalSize)

	mergeRatio := float32(db.invalidSize) / float32(totalSize)
	t.Log("mergeRatio:", mergeRatio)
	t.Log("mergeRatio threshold:", 0.4)

	err = db.Merge()
	t.Log("err:", err)
	assert.NotNil(t, err)
}
