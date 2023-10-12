package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap 用于启动加速(用了官方扩展包，只能用来读取数据)
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMap 初始化MMap IO
func NewMMap(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(b, offset)
}

func (m *MMap) Write(bytes []byte) (int, error) {
	panic("not implemented")
}

func (m *MMap) Sync() error {
	panic("not implemented")
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
