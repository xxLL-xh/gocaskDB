package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// IteratorUI 面向用户的迭代器接口
type IteratorUI struct {
	indexIter index.Iterator // 内存索引迭代器，用于取出key和logRecord记录的位置信息
	db        *DB            // 拿到位置信息pos后，需要从db中找出实际的值来返回给用户
	options   IteratorOptions
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorOptions) *IteratorUI {
	indexIter := db.index.Iterator(opts.Reverse)
	return &IteratorUI{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *IteratorUI) Rewind() {
	it.indexIter.Rewind()
	it.nextPrefix()
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (it *IteratorUI) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.nextPrefix()
}

// Next 跳转到下一个 key
func (it *IteratorUI) Next() {
	it.indexIter.Next()
	it.nextPrefix()
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (it *IteratorUI) Valid() bool {
	return it.indexIter.IsValid()
}

// Key 当前遍历位置的 Key 数据
func (it *IteratorUI) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (it *IteratorUI) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (it *IteratorUI) Close() {
	it.indexIter.Close()
}

// 如果用户配置了查找的key的prefix，则跳过不包含该prefix的key
func (it *IteratorUI) nextPrefix() {
	preLen := len(it.options.Prefix)
	if preLen == 0 {
		return
	}

	for ; it.indexIter.IsValid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if preLen <= len(key) && bytes.Compare(it.options.Prefix, key[:preLen]) == 0 {
			break
		}
	}
}
