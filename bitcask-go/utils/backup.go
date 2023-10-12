package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标目标不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	// filepath.Walk() 对目录下的所有文件（子目录）遍历执行用户自定义的方法。
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1) // 取出文件名称
		if fileName == "" {
			return nil
		}

		// 检查是否是不需要拷贝的文件
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		// 如果是文件夹，则在目标文件目录创建文件夹
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		// 将数据拷贝到新目录的文件中
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
