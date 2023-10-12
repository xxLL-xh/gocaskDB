package utils

import (
	"io/fs"
	"path/filepath"
)

// GetDirSize 得到数据目录下的文件大小
func GetDirSize(dir string) (int64, error) {
	var size int64
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
