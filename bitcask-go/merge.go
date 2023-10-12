package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

// Merge clear up invalid records, and generate hint file
func (db *DB) Merge() error {

	// 数据库没有旧文件，直接返回
	if len(db.olderFiles) == 0 {
		return nil
	}
	// S1
	// 先找出所有需要merge的文件（将所有需要被merge的文件记录在一个list中），并且从小到大排序
	// 这里目前是将包括活跃文件在内的所有文件一起merge。
	// 在此期间先上锁。
	db.mu.Lock()

	// 确保只有一个进程在merge
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	// 检查是否达到了merge的阈值
	totalSize, err := utils.GetDirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	mergeRatio := float32(db.invalidSize) / float32(totalSize)

	if mergeRatio < db.options.MergeRatioThreshold {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	// 检查磁盘剩余的容量是否可以够merge
	availableDiscSize, err := utils.AvailableDiscSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if totalSize-db.invalidSize >= availableDiscSize {
		db.mu.Unlock()
		return ErrNotHaveEnoughSpaceForMerge
	}

	// 正式开始merge
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 把当前活跃文件加入旧文件，创建一个新活跃文件
	if err = db.activeFile.SyncFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.Fid] = db.activeFile
	if err = db.SetActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 记录第一个未参与merge的文件的id、
	firstNonMergedFid := db.activeFile.Fid

	// 创建列表记录需要merge的文件
	var filesToBeMerged []*data.File
	for _, file := range db.olderFiles {
		filesToBeMerged = append(filesToBeMerged, file)
	}

	// 找到需要merge的文件就可以解锁了。之后用户可以进行写入操作，都发生在新的活跃文件中，不会对merge的文件产生影响
	db.mu.Unlock()

	// 对需要被merge的文件从小到大依次排序。从小到大进行merge（从新到旧）
	sort.Slice(filesToBeMerged, func(i, j int) bool {
		return filesToBeMerged[i].Fid < filesToBeMerged[j].Fid
	})

	// S2 打开一个临时的DB实例，用于merge
	// 创建一个用于存放merge文件的目录，并在该目录上打开一个新的db实例
	mergePath := db.getMergePath()
	// 如果该目录存在，说明之前发生过merge行为。先将之前的merge目录的全部内容删除
	if _, err = os.Stat(mergePath); err == nil {
		//  os.Stat(path) 返回path的信息。如果path不存在，返回一个error。反之，如果没有返回error，代表目录已存在
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// 新建目录
	if err = os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新的db实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false // merge过程中，如果每次写入都sync，会非常慢。写入中发生错误时，merge是不成功的，所以不必每次都sync
	// 打开一个mergeDB实例
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// S3 正式开始merge
	// 遍历所有需要merge的文件，重写有效数据,并创建hint文件
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	for _, file := range filesToBeMerged {
		var offset int64 = 0
		for {
			record, size, err1 := file.ReadLogRecord(offset)
			if err1 != nil {
				// 读到文件末尾
				if err1 == io.EOF {
					break
				}
				return err1
			}
			// 拿到实际的key
			realKey, _ := decodeKeyWithSeqNo(record.Key)
			// 将该key在内存索引中的位置信息与当前位置进行比较。检查记录是否有效,如果有效，则重写进merge目录的活跃文件；如果无效就忽略掉。
			positionFromIndex := db.index.Get(realKey)
			if positionFromIndex != nil &&
				positionFromIndex.Fid == file.Fid &&
				positionFromIndex.Offset == offset {
				// 清除事务序列号
				record.Key = encodeKeyWithSeqNo(realKey, NonTransaction)
				// 拿到位置信息
				pos, err2 := mergeDB.appendLogRecordWithoutLock(record)
				if err2 != nil {
					return err2
				}
				// 将位置信息写入hint文件

				if err3 := hintFile.WriteHintFile(realKey, pos); err3 != nil {
					return err3
				}
			}
			// 读取下一条记录
			offset += size
		}
	}

	if err = hintFile.SyncFile(); err != nil {
		return err
	}
	if err = mergeDB.Sync(); err != nil {
		return err
	}

	// 创建一个文件用于标识merge的完成（该文件存在代表merge完成，且其中记录了该次merge清理了哪几个旧文件）
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte("merge finished"),
		Value: []byte(strconv.Itoa(int(firstNonMergedFid))), // 比这个id小的文件都参与过merge
	}
	encodedMFR, _ := data.EncodeLogRecord(mergeFinishedRecord)
	err = mergeFinishedFile.Write(encodedMFR)
	if err != nil {
		return err
	}
	err = mergeFinishedFile.SyncFile()
	if err != nil {
		return err
	}
	return nil

}

// Default normal files' path: "/tmp/kv"
// Default merge files' path: "/tmp/kv-merge"
func (db *DB) getMergePath() string {
	// path.Dir 返回数据文件目录的父目录
	dir := path.Dir(path.Clean(db.options.DirPath)) // "/tmp"
	base := path.Base(db.options.DirPath)           // "kv"
	// 由上面两个字符串组合成用于存放merge文件的目录
	return filepath.Join(dir, base+"-merge")
}

func (db *DB) loadIndexFromHint() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	// hint文件不存在直接返回
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开hint文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}
	// 读取记录，加载进index
	var offset int64 = 0
	for {
		record, size, err2 := hintFile.ReadLogRecord(offset)
		if err2 != nil {
			// 读到了文件末尾
			if err2 == io.EOF {
				break
			}
			return err2
		}
		key := record.Key
		decodedPosition := data.DecodeLogRecordPos(record.Value)
		db.index.Put(key, decodedPosition)
		offset += size
	}
	return nil
}

// 加载merge目录下的数据（应该在什么时候加载？？？只在启动时加载不对吧？？？）
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// 如果不存在merge目录直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	// 打开merge文件目录
	DirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 首先先查看merge完成的标识是否存在。如果已merge完成，就将文件名记录下来。
	mergeFinished := false
	var mergeFilesNames []string
	for _, entry := range DirEntries {
		if entry.Name() == data.MergeFinishedFile {
			mergeFinished = true
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFilesNames = append(mergeFilesNames, entry.Name())
	}

	// merge 没有完成直接返回
	if !mergeFinished {
		mergeFilesNames = nil
		return nil
	}

	// 如果merge完成，就将原DB目录下已被merge的文件删掉，用merged files替代
	firstNonMergedFid, err := db.GetFirstNonMergedFid(mergePath)
	if err != nil {
		return err
	}

	// 删除比firstNonMergedFid小的旧文件
	for fid := uint32(0); fid < firstNonMergedFid; fid++ {
		fileName := data.GetDataFileName(db.options.DirPath, fid)
		// 如果旧数据文件存在，就将它删除掉。
		if _, err1 := os.Stat(fileName); err1 == nil {
			err = os.Remove(fileName)
			if err != nil {
				return err
			}
		}
	}

	// 加载新的merged后的数据文件
	// /tmp/kv          01.data  02.data  03.data
	// /tmp/kv-merge
	for _, mergedFile := range mergeFilesNames {
		src := filepath.Join(mergePath, mergedFile)
		dest := filepath.Join(db.options.DirPath, mergedFile) // 新的file又是从0开始的？
		err = os.Rename(src, dest)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetFirstNonMergedFid 在merge目录中的mergeFinishedFile中，找到第一个未被merge的文件的id
func (db *DB) GetFirstNonMergedFid(mergePath string) (uint32, error) {

	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}

	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	firstNonMergedFid, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(firstNonMergedFid), nil
}
