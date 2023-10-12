package bitcask_go

type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 每次写数据是否持久化
	SyncWrites bool

	// 每写多少字节自动持久化
	SyncPerBytes uint

	// 索引类型
	IndexType IndexerType

	// 是否在启动时使用内存映射MemoryMap加速加载
	MMapAtStartupNeeded bool

	// merge阈值
	MergeRatioThreshold float32
	// hash table 的初始容量？

}

// IteratorOptions 索引迭代器配置参数
type IteratorOptions struct {
	// 遍历的key的前缀。可以只遍历key中含有指定前缀的items
	Prefix []byte
	// 是否反向遍历
	Reverse bool
}

type WriteBatchOptions struct {
	// 一个批次中的最大数据量
	MaxBatchNum uint
	// 提交后是否直接进行sync持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1
	// Hash 哈希表
	Hash
	// ART 自适应基数树索引
	ART
	// SkipList B+ 树索引
	SkipList
)

var DefaultOptions = Options{
	DirPath:             "/tmp/kv/DB",     // 更换一个路径
	DataFileSize:        32 * 1024 * 1024, // 32MB
	SyncWrites:          false,
	SyncPerBytes:        0,
	IndexType:           BTree,
	MMapAtStartupNeeded: true,
	MergeRatioThreshold: 0.6,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
