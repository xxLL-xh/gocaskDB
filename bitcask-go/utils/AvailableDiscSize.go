package utils

import "syscall"

// AvailableDiscSize 获取磁盘可用数据量大小
func AvailableDiscSize() (int64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	err = syscall.Statfs(wd, &stat)
	if err != nil {
		return 0, err
	}
	return int64(stat.Bavail) * stat.Bsize, nil
}
