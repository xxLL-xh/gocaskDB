package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

/*
	db.go定义上层用户接口
*/

// 文件锁
const fileLockName = "flock"

// DB 存储引擎实例 不使用int是为了保证各个平台上都能运行。
type DB struct {
	options       Options
	mu            *sync.RWMutex
	activeFile    *data.File            // 当前活跃文件，用于写入
	olderFiles    map[uint32]*data.File // 旧文件的map，用于读数据
	index         index.Indexer         // 这里为啥不用指针？
	loadedFileIds []int                 // 加载时，加载到的数据文件。****该列表仅仅只用于数据库引擎启动时的数据文件加载和内存索引构建****
	seqNo         uint64                // 事务序列号，全局递增
	isMerging     bool                  // 数据库引擎是否正在merge（同一时刻只允许一个进程在merge）
	flock         *flock.Flock          // 文件锁，保证多进程互斥访问数据库目录，open时创建，close时关闭
	bytesWrite    uint                  // 当前距离上一次持久化累计写了多少字节
	invalidSize   int64                 // 记录有多少数据是被update或delete的，只有这些数据是无效的，需要被merge
	// 其中，delete时，原来的LogRecord和新加的logRecord都是不需要的
}

// Stat 数据引擎的统计信息
type Stat struct {
	KeyNum           uint  // Key的总数量
	DataFileNum      uint  // 磁盘上的数据文件数量
	ReclaimableSize  int64 // 通过merge操作可以回收的空间大小（无效数据的数据量），以字节为单位
	OccupiedDiscSize int64 // 数据库数据目录所占磁盘空间的大小
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database directory is empty")
	}
	if options.DataFileSize < 0 {
		return errors.New("data file size should > 0")
	}
	if options.MergeRatioThreshold < 0 || options.MergeRatioThreshold > 1 {
		return errors.New("merge Ratio Threshold should be within [0, 1]")
	}
	return nil
}

// Open 打开数据库引擎 open database engine instance
func Open(options Options) (*DB, error) {

	// S1 检查用户配置项，校验数据目录。如果没有错误就初始化DB结构体
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	// 检验数据目录是否存在
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断当前数据目录是否正在被使用（为了简洁性，只允许一个进程打开一个DB实例）
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	lockIsHold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !lockIsHold {
		return nil, ErrDataBaseIsBeingUsed
	}

	// 对DB结构体进行初始化
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.File),
		index:      index.NewIndexer(options.IndexType),
		flock:      fileLock,
	}

	// S2 加载merge数据目录
	err = db.loadMergeFiles()
	if err != nil {
		return nil, err
	}

	// S3 加载数据文件
	// 确定启动时是否需要启用MemoryMap
	ioType := fio.StandardFIO
	if db.options.MMapAtStartupNeeded {
		ioType = fio.MemoryMapIO
	}
	if err2 := db.loadDataFiles(ioType); err2 != nil {
		return nil, err2
	}

	// S4 构建内存索引
	// 如果存在hint文件，直接从hint文件中加载merge后的记录的索引
	// 从 hint 索引文件中加载索引
	if err2 := db.loadIndexFromHint(); err2 != nil {
		return nil, err2
	}
	if err2 := db.loadIndexFromDataFiles(); err2 != nil {
		return nil, err2
	}

	// 启动完需要把每个文件的ioManager重置回标准IO
	if ioType == fio.MemoryMapIO {
		err = db.resetIOTypeToStandardIO()
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

func (db *DB) Close() error {
	defer func() {
		// 释放文件锁
		if err := db.flock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
		// 关闭索引
		if err := db.index.Close(); err != nil {
			panic(fmt.Sprintf("failed to close index"))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//	关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化当前活跃文件（旧文件在变成旧文件的那一刻就自动sync了，无需手动sync）
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.SyncFile()
}

// Stat 统计数据库的各项信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dataFileNum := uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFileNum += 1
	}

	dirSize, err := utils.GetDirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get the size of data file directory: %v", err))
	}

	return &Stat{
		KeyNum:           uint(db.index.Size()),
		DataFileNum:      dataFileNum,
		ReclaimableSize:  db.invalidSize,
		OccupiedDiscSize: dirSize,
	}
}

// Backup 备份数据库
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// SetActiveFile 更新当前活跃文件（1.刚启动时调用；2.上一个活跃文件满了时调用）
// *************** 访问此方法前必须持有互斥锁 ******************
func (db *DB) SetActiveFile() error {
	// 数据库刚启动时没有活跃文件
	var ID uint32 = 1
	// 活跃文件满时，下一个活跃文件ID+1
	if db.activeFile != nil {
		ID = db.activeFile.Fid + 1
	}
	// 打开当前活跃文件
	currentActiveFile, err := data.OpenDataFile(db.options.DirPath, ID, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = currentActiveFile
	return nil
}

// Put 用户存入键值对, key不能为空！！！
// S1:写入磁盘文件。
// S2:更新内存索引。
// S1: Write to data file in disc.
// S2: Update the in-memory index.
func (db *DB) Put(key []byte, value []byte) error {
	// key为空（b tree中nil可以作key）
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// S1
	// 构造LogRecord结构体，暂存要存入数据文件的键值对
	logRecord := &data.LogRecord{Key: encodeKeyWithSeqNo(key, NonTransaction), Value: value, Type: data.LogRecordNormal}

	// 追加写入当前活跃文件，并且拿到数据位置的索引信息
	position, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	// S2
	if oldPos := db.index.Put(key, position); oldPos != nil {
		db.invalidSize += int64(oldPos.Size)
	}
	return nil
}

// Delete  logRecord by key  写入的记录只包含key和删除标志
// S1: Search if the key is in the index. If not, return directly, avoiding writing useless delete records.
// S2: construct delete record log (Type) and write it to active data file
// S3: delete the key in the index
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// S1
	if pos := db.index.Get(key); pos == nil {
		log.Print("the key is not in the database")
		return nil
	}

	// S2
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithSeqNo(key, NonTransaction),
		Type: data.LogRecordDeleted,
	}

	pos, err := db.appendLogRecordWithLock(logRecord) // 在这里才写入 db.appendLogRecordWithLock(logRecord)中加了写锁
	if err != nil {
		return err
	}
	db.invalidSize += int64(pos.Size)

	// S3
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.invalidSize += int64(oldPos.Size)
	}
	return nil
}

// Get 根据key读数据。
// S1:先去内存索引中查找，没有返回错误，有得到record的LogRecordPos信息
// S2:根据pos中的id查找文件，如果是当前活跃文件，直接使用活跃文件。否则去旧文件中查找。
// S3:根据pos中的offset，读取数据
// S1: search key in the in-memory index.
//
//	If not found, return an error.
//	If found, retrieve the LogRecordPos information of the record.
//
// S2: Using the ID from pos, search for the file.
//
//	If it is the current active file, directly use the active file. Otherwise, search in the old files.
//
// S3: Read the data based on the offset from pos.
func (db *DB) Get(key []byte) ([]byte, error) {
	// 加锁
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// S1
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
	// S2,3
	return db.getValueByPosition(pos)

}

// 根据位置信息得到value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {

	// S2 根据pos中的id查找文件，如果是当前活跃文件，直接使用活跃文件。否则去旧文件中查找。
	var dataFile *data.File
	if pos.Fid == db.activeFile.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// S3 根据pos中的offset，读取数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	// 因为是利用墓碑值来删除，所以可能找出了key对应的数据记录但是实际上它是已经被删除了的。
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// ListKeys 获取数据库中所有key的list
func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	defer iter.Close()
	keys := make([][]byte, db.index.Size())
	var i int
	for iter.Rewind(); iter.IsValid(); iter.Next() {
		keys[i] = iter.Key()
		i += 1
	}
	return keys
}

// appendLogRecordWithLock 向数据文件追加写入LogRecord，返回数据记录的索引信息，或者可能存在的error
// S1:判断当前是否有活跃数据文件,没有就set一个
// S2:判断写入后会不会超过文件大小阈值。超过就重新set活跃数据文件
// S3:是否需要进行持久化
// S1: Check if there is an active data file. If not, set one.
// S2: Check if writing will exceed the file size threshold. If it does, reset the active data file.
// S3: Determine if persistence is required.
func (db *DB) appendLogRecordWithoutLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前是否存在活跃文件
	// 若当前没有活跃文件，先进行初始化
	if db.activeFile == nil {
		err := db.SetActiveFile()
		if err != nil {
			return nil, err
		}
	}

	// 对记录进行编码，并追加写入
	encoded, size := data.EncodeLogRecord(record)
	// 写入前需要进行一个判断：如果当前活跃文件写入当前数据后的大小超过的阈值，需要进行更新操作。
	// 将当前活跃文件转变为old文件，并打开一个新的文件作为活跃文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 先对当前活跃文件持久化
		if err := db.activeFile.SyncFile(); err != nil {
			return nil, err
		}
		// 持久化后，转换为旧文件
		db.olderFiles[db.activeFile.Fid] = db.activeFile
		// 更新新的活跃文件
		err := db.SetActiveFile()
		if err != nil {
			return nil, err
		}
	}

	// 将数据写入当前活跃文件
	OffsetStart := db.activeFile.WriteOffset
	err := db.activeFile.Write(encoded)
	if err != nil {
		return nil, err
	}

	// 记录距离上一次持久化写入了多少字节
	db.bytesWrite += uint(size)

	// 持久化策略
	var needSync = db.options.SyncWrites
	// 1.用户没有设置每次写入都立即进行持久化，则没写n个字节自动进行一次持久化（默认为0，即不进行持久化）
	// 2.用户胡设置了每次写入后立即持久化
	if !needSync && db.options.SyncPerBytes > 0 && db.bytesWrite > db.options.SyncPerBytes {
		needSync = true
	}
	if needSync {
		err = db.activeFile.SyncFile()
		if err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// 构造内存的索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.Fid, Offset: OffsetStart, Size: uint32(size)}
	return pos, nil
}

func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecordWithoutLock(record)
}

// loadDataFiles 从磁盘中加载数据文件
// S1:找出所有数据文件
// S2:设置ID最大的数据文件为当前活跃文件。将其他数据文件添加进olderFiles map
// S1: Find all data files.
// S2: Set the data file with the highest ID as the current active file.
//
//	Add other data files to the "olderFiles" map.
func (db *DB) loadDataFiles(ioType fio.FileIOType) error {
	dir, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	// S1
	// 遍历该目录下的所有文件，找到所有 *.data 文件
	var fids []int // uint32 ??
	for _, file := range dir {
		if strings.HasSuffix(file.Name(), data.FileSuffix) {
			fid, err1 := strconv.Atoi(strings.Split(file.Name(), ".")[0])
			if err1 != nil {
				return ErrDataFileDirectoryCorrupted
			}
			fids = append(fids, fid)
		}
	}
	// 从小到大排序
	sort.Ints(fids)
	db.loadedFileIds = fids // 便于后面加载内存索引时复用

	// S2
	// 遍历每个文件，打开
	for i, id := range fids {
		dataFile, err1 := data.OpenDataFile(db.options.DirPath, uint32(id), ioType)
		if err1 != nil {
			return err1
		}

		// 最后一个数据文件就是活跃文件,其他的都是旧文件
		if i == len(fids)-1 {
			// 活跃文件
			db.activeFile = dataFile
		} else {
			// 旧文件
			db.olderFiles[uint32(id)] = dataFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 根据数据文件中构建内存索引
// 遍历各个文件中的所有记录，构建内存索引。（后续可以实现hint文件）
// Iterate through all records in each file and build an in-memory index.
func (db *DB) loadIndexFromDataFiles() error {
	// 如果没有文件，说明是空的，直接返回
	if len(db.loadedFileIds) == 0 {
		return nil
	}

	// 如果发生过merge，则跳过merge过的文件（即，跳过fid小于firstNonMergeFid的文件）
	// 这里先记录下是否merge过，以及最早未merge的文件id。后面循环加载索引时再根据id来判断
	isMerged := false
	firstNonMergedFid := uint32(0)

	mergeFinishFile := filepath.Join(db.options.DirPath, data.MergeFinishedFile)
	// 如果存在mergeFinishFile，说明merge已完成。拿到First Non-Merged File id
	if _, err := os.Stat(mergeFinishFile); err == nil {
		//  os.Stat(path) 返回path的信息。如果path不存在，返回一个error。反之，如果没有返回error，代表目录已存在
		id, err1 := db.GetFirstNonMergedFid(db.options.DirPath)
		if err1 != nil {
			return err1
		}
		isMerged = true
		firstNonMergedFid = id
	}
	isActive := false

	// 定义了一个方法用于更新内存索引
	updateIndex := func(key []byte, t data.LogRecordType, pos *data.LogRecordPos) {
		// 是否已被删除
		var oldPos *data.LogRecordPos
		if t == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.invalidSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.invalidSize += int64(oldPos.Size)
		}
	}
	// 如果使用了事务，用于暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	currentSeqNo := NonTransaction

	// 加载每一个文件，前n-1个文件是旧文件，最后一个文件是活跃文件，需要维护它的WriteOffSet
	for _, fid := range db.loadedFileIds {
		var fileId = uint32(fid)

		// 如果file id 小于 firstNonMergedFid，说明已经通过hint文件加载进内存index，直接跳过
		if isMerged && fileId < firstNonMergedFid {
			continue
		}

		var dataFile *data.File
		if fileId == db.activeFile.Fid {
			// active file
			dataFile = db.activeFile
			isActive = true
		} else {
			// older file
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0

		// 循环读取文件中的记录，读到EOF时跳出循环
		for {
			// 注意：这里的err不能直接返回，因为如果读到文件末尾，也会返回EOF。
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					return err
				}
			}

			// 构造内存索引
			pos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			// 解码出实际的key（数据文件中的key是key+事务序列号）
			realKey, seqNo := decodeKeyWithSeqNo(logRecord.Key)

			// 非事务
			if seqNo == NonTransaction {
				updateIndex(realKey, logRecord.Type, pos)
			} else {
				// 通过write batch提交的事务
				if logRecord.Type == data.TransactionFinished {
					// 如果是事务提交完成的标识，则将带有该事务序列号的数据一起更新进内存索引
					for _, tRecord := range transactionRecords[seqNo] {
						updateIndex(tRecord.Record.Key, tRecord.Record.Type, tRecord.Position)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 还没有提交成功，先暂存起来
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record:   logRecord,
						Position: pos,
					})
				}
			}

			// 记录最大的事务序列号
			if currentSeqNo < seqNo {
				currentSeqNo = seqNo
			}

			offset += size

			// update WriteOffset of the active file
			if isActive {
				db.activeFile.WriteOffset = offset
			}
		}
	}

	// 将当前事务序列号记录进db
	db.seqNo = currentSeqNo

	return nil
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIOTypeToStandardIO() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
