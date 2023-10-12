package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const NonTransaction uint64 = 0

var transactionFinishKey = []byte("fin")

// WriteBatch 原子批量写数据，保证原子性。
// 加入了事务序列号的概念，不直接写入，而是暂存在内存，之后批量写入索引，批量写入数据文件。
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opts,
		db:            db,
		mu:            new(sync.Mutex),
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// PendingPut 批量写数据。
// 用一个无序的map暂存写入信息。这里不需要在意写入顺序是因为，只需要保证每个key对应的value是最新的值即可，不同key之间顺序不同不影响
func (wb *WriteBatch) PendingPut(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 与db中不同。不会直接写，而是先暂存起来
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) PendingDelete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在,直接返回(需要判断index和原子写暂存的数据)
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务，将暂存的事务保存到磁盘，并且更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 没有暂存的数据，或者暂存的数据超过了配置项中的配置
	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 写入数据。 先写入数据文件，全部写完之后，再将位置信息存入内存索引
	posTmp := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecordWithoutLock( // 上面已经给db上过锁了
			&data.LogRecord{
				Key:   encodeKeyWithSeqNo(record.Key, seqNo),
				Value: record.Value,
				Type:  record.Type,
			},
		)
		if err != nil {
			return err
		}
		posTmp[string(record.Key)] = logRecordPos
	}

	// 写入一条标识事务完成的数据
	fin := &data.LogRecord{
		Key:  encodeKeyWithSeqNo(transactionFinishKey, seqNo),
		Type: data.TransactionFinished,
	}
	_, err := wb.db.appendLogRecordWithoutLock(fin)
	if err != nil {
		return err
	}

	// 根据配置决定是否进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		err2 := wb.db.activeFile.SyncFile()
		if err2 != nil {
			return err2
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := posTmp[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
			wb.db.invalidSize += int64(pos.Size) // ???
		}
		if oldPos != nil {
			wb.db.invalidSize += int64(oldPos.Size)
		}
	}

	// 最后清空暂存数据结构，方便下一次commit
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 对key重新进行编码，加上当前事务的序列号
// 变成 SeqNo + Keys
func encodeKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	// 序列号是变长的
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

func decodeKeyWithSeqNo(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
