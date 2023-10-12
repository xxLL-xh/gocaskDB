package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// 用于删除测试生成的文件
func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp/kv", "0001.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp/kv", "0001.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	content, err := fio.Write([]byte(""))
	assert.Equal(t, 0, content)
	assert.Nil(t, err)

	content, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, content)
	assert.Nil(t, err)

	content, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, content)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {

	// ****************还需要测试n<len(b)（返回一个non-nil error）****************
	// ****************以及到达文件末尾的情况（到达文件末尾返回错误为EOF）****************
	path := filepath.Join("/tmp/kv", "0001.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a")) // 写入时不需要指定offset，因为已经指定了使用追加写入。
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp/kv", "0001.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp/kv", "0001.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
