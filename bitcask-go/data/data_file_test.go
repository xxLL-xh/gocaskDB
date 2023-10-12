package data

import (
	"bitcask-go/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile("/tmp/kv", 0, fio.StandardFIO)

	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile("/tmp/kv", 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile("/tmp/kv", 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

	t.Log(t, os.TempDir())
}

func TestFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile("/tmp/kv", 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err1 := dataFile.Write([]byte("abc"))
	assert.Nil(t, err1)

	err2 := dataFile.Write([]byte("def"))
	assert.Nil(t, err2)

	err3 := dataFile.Write([]byte("1234567890"))
	assert.Nil(t, err3)
}

func TestFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile("/tmp/kv", 123, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err1 := dataFile.Write([]byte("abc"))
	assert.Nil(t, err1)

	err2 := dataFile.Close()
	assert.Nil(t, err2)
}

func TestFile_SyncFile(t *testing.T) {
	dataFile, err := OpenDataFile("/tmp/kv", 456, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err1 := dataFile.Write([]byte("abc"))
	assert.Nil(t, err1)

	err2 := dataFile.SyncFile()
	assert.Nil(t, err2)
}

func TestFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile("/tmp/kv", 222, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有1条记录
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)

	// 多条 LogRecord，从不同的位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
