package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordType LogRecord的墓碑值字段。枚举类型，正常和已删除
type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	TransactionFinished
)

// crc type ks vs    4+1+5+5    // 这里的ks是指keySize字段长度，而不是key字段的长度
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 1 + 4

// LogRecord 写入到数据文件的记录。由于是类似日志一样地追加写入的，所以叫做Log
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecord 的头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 的类型
	keySize    uint32        // key 的长度
	valueSize  uint32        // value 的长度
}

// LogRecordPos 数据内存索引，用于描述数据在内存上的位置position
type LogRecordPos struct {
	Fid    uint32 //表示数据被存放在了内存中的哪个文件里
	Offset int64  //表示数据存放在文件的哪个位置
	Size   uint32 //标识数据在磁盘上的大小
}

// TransactionRecord 从数据文件加载数据到内存时，用于暂存事务中记录的结构体
type TransactionRecord struct {
	Record   *LogRecord
	Position *LogRecordPos
}

// EncodeLogRecord 对数据记录LogRecord进行编码，返回编码后的字节数组和数组的长度
// CRC		Type		KeySize		ValueSize		Keys		Value
//
//	4         1           <=5          <=5           变长      变长
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	kLen := len(record.Key)
	vLen := len(record.Value)

	// 初始化一个header部分的编码
	header := make([]byte, maxLogRecordHeaderSize)

	// 第5个字节：Type
	header[4] = record.Type

	// 第6个字节开始：key/value长度
	var index = 5
	index += binary.PutVarint(header[index:], int64(kLen))
	index += binary.PutVarint(header[index:], int64(vLen))

	// 整条LogRecord的长度
	var size = index + kLen + vLen

	// 初始化最终要返回的编码后的字节数组
	encodedBytes := make([]byte, size)
	copy(encodedBytes[:index], header[:index])

	// 第index个字节开始：写入key/value的实际值(直接copy过来)
	copy(encodedBytes[index:], record.Key)
	copy(encodedBytes[index+kLen:], record.Value)

	// CRC 校验
	crc := crc32.ChecksumIEEE(encodedBytes[4:])
	binary.LittleEndian.PutUint32(encodedBytes[:4], crc)

	/*fmt.Printf("header:%x\n", encodedBytes[:index])
	fmt.Printf("key and value: %x\n", encodedBytes[index:])*/

	return encodedBytes, int64(size)
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// 计算取出数据记录的crc值
func getLogRecordCRC(r *LogRecord, header []byte) uint32 {
	if r == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, r.Key)
	crc = crc32.Update(crc, crc32.IEEETable, r.Value)

	return crc
}

// EncodeLogRecordPos 对位置信息结构体进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	record := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	index := 0
	index += binary.PutVarint(record[index:], int64(pos.Fid))
	index += binary.PutVarint(record[index:], pos.Offset)
	index += binary.PutVarint(record[index:], int64(pos.Size))
	return record[:index]
}

// DecodeLogRecordPos 对hint文件中的位置信息记录解码
func DecodeLogRecordPos(record []byte) *LogRecordPos {
	index := 0
	fid, n := binary.Varint(record[index:])
	index += n
	offset, n := binary.Varint(record[index:])
	index += n
	size, _ := binary.Varint(record[index:])
	return &LogRecordPos{
		Fid:    uint32(int32(fid)),
		Offset: offset,
		Size:   uint32(size),
	}
}
