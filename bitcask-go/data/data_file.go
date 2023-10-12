package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	FileSuffix        = ".data"
	HintFileName      = "hint"
	MergeFinishedFile = "merged-mark"
)

type File struct {
	Fid         uint32        // 文件id
	WriteOffset int64         // 文件写到了什么位置(主要用于活跃文件)
	IOManager   fio.IOManager // 数据读写操作的抽象接口
}

// GetDataFileName 得到dirPath目录下数据文件的文件名称（即fid）
func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileSuffix)
}

// OpenDataFile 从dirPath打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32, ioType fio.FileIOType) (*File, error) {
	// fileID : 9位字符，用0填充
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileSuffix)
	return NewFile(fileName, fileId, ioType)
}

// OpenHintFile merge前，从dirPath打开一个hint文件
func OpenHintFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return NewFile(fileName, 0, fio.StandardFIO)
}

// OpenMergeFinishedFile 从dirPath打开一个merge完成的标识文件
func OpenMergeFinishedFile(dirPath string) (*File, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFile)
	return NewFile(fileName, 0, fio.StandardFIO)
}

func NewFile(fileName string, fileId uint32, ioType fio.FileIOType) (*File, error) {
	// 初始化IO管理接口
	ioManager, err := fio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	return &File{
		Fid:         fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

func (f *File) Close() error {
	return f.IOManager.Close()
}

// ReadLogRecord 根据offset读取记录
func (f *File) ReadLogRecord(offset int64) (*LogRecord, int64, error) {

	// Go语言中，读取超过文件大小会返回EOF错误，而当记录被deleted时，记录的header长度会小于maxLogRecordHeaderSize，
	// 还按照maxLogRecordSize读会超过文件大小。直接读到文件末尾即可。
	fileSize, err := f.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 如果读取的最大 header 长度已经超过了文件的长度，则只需要读取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 读取记录的header部分
	headerBuf, err := f.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 如果从某个偏移量没读到header，说明文件已经读到头了
	if header == nil {
		return nil, 0, io.EOF
	}
	// ????这个也表示读到了末尾？？？？？
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 得到key、value的长度
	ks, vs := int64(header.keySize), int64(header.valueSize)

	recordSize := headerSize + ks + vs
	logRecord := &LogRecord{Type: header.recordType}

	// 读取实际存储的key和value
	if ks > 0 || vs > 0 {
		kvBuf, err2 := f.readNBytes(ks+vs, offset+headerSize)
		if err2 != nil {
			return nil, 0, err2
		}
		logRecord.Key = kvBuf[:ks]
		logRecord.Value = kvBuf[ks:]
	}

	// 校验数据有效性
	// 存数据时计算一次crc并保存（crc1），取出数据后再根据取出的数据计算一次crc（记为crc2），最后判断两个数字是否相等
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, errors.New("invalid CRC value, log record may be corrupted")
	}

	return logRecord, recordSize, nil
}

func (f *File) Write(buf []byte) error {
	writeSize, err := f.IOManager.Write(buf)
	if err != nil {
		return err
	}
	f.WriteOffset += int64(writeSize)
	return nil
}

// SyncFile 持久化数据文件
func (f *File) SyncFile() error {
	return f.IOManager.Sync()
}

func (f *File) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = f.IOManager.Read(b, offset)
	return // 可以自动决定返回什么？？
}

// WriteHintFile 将记录的索引信息写入hint文件
func (f *File) WriteHintFile(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos), // value 是对位置信息编码后的一条记录（fid， offset）
	}
	encodedRecord, _ := EncodeLogRecord(record)
	return f.Write(encodedRecord)
}

// SetIOManager 更改当前文件的io类型
func (f *File) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := f.IOManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, f.Fid), ioType)
	if err != nil {
		return err
	}
	f.IOManager = ioManager
	return nil
}
