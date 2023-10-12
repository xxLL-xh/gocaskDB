package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer KeyDir abstract interface
type Indexer interface {
	// Put Stores the key into the index along with information about the position of the data.
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get Query the corresponding index position information according to the key
	Get(key []byte) *data.LogRecordPos
	// Delete according to the key, and return the old value after successful deletion.
	Delete(key []byte) (*data.LogRecordPos, bool)
	// Iterator Returns the indexed iterator
	Iterator(reverse bool) Iterator
	// Size Amount of data in the index
	Size() int
	// Close 关闭
	Close() error
}

// IndexType 索引类型枚举变量
type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// Hash 哈希表
	Hash

	// ART 自适应基数树索引
	ART

	// SkipList B+ 树索引
	SkipList
)

// NewIndexer 根据类型，初始化索引
func NewIndexer(t IndexType) Indexer {
	switch t {
	case Btree:
		return NewBTree()

	case Hash:
		return NewSafeHashTable()

	case ART:
		return NewART()

	case SkipList:
		return NewSkipList()

	default:
		panic("unsupported index type")

	}
}

// Iterator 索引迭代器(内部使用，不面向用户)
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于，根据key的排列顺序）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// IsValid  是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	IsValid() bool

	// Key 当前位置的 Key
	Key() []byte

	// Value 当前位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 清理掉临时数组values
	Close()
}

// Item item需实现Less方法，用于比较不同item的key，用于btree排序
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool { // 为什么两个是不同类型？
	return bytes.Compare(ai.key, bi.(*Item).key) == -1 // ******************************??????
}

/*func (ai *Item) Less(bi *Item) bool { // 为什么两个是不同类型？
	return bytes.Compare(ai.key, bi.key) == -1    // ??????
}*/
