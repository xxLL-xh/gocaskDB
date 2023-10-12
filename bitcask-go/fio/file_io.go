package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	f *os.File // 系统文件描述符
}

// NewFileIOManager 初始化标准文件 IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fileName,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // ***************************
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{f: fd}, nil
}

// 根据offset读取
// ReadAt always returns a non-nil error when n < len(b). At end of file, that error is io.EOF.
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.f.ReadAt(b, offset)
}

// Write 将b追加写进文件
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.f.Write(b)
}

// Sync 将内存中的file system copy刷入不易失的磁盘
func (fio *FileIO) Sync() error {
	return fio.f.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.f.Close()
}

// Size 从文件统计信息中查询文件大小
func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
