package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTreeIndex 内存数据索引，封装google的btree库。包含btree和同步锁    https://github.com/google/btree
type BTreeIndex struct {
	// btree.New()返回的是一个b树的指针，所以这里是”tree *btree.BTree“，用的地址
	tree *btree.BTree  // ********并发写是不安全的（要加锁），读安全*******
	lock *sync.RWMutex // 上层用户可能多线程地并发访问内存，所以需要加锁
}

func (bt *BTreeIndex) Size() int {
	return bt.tree.Len()
}

// NewBTree 初始化BTree索引结构
func NewBTree() *BTreeIndex {
	return &BTreeIndex{
		tree: btree.New(32), // degree：分支因子***************可以提供一个参数供用户选择
		lock: new(sync.RWMutex),
	}
}

// Put 存数据，存之前需要加锁
func (bt *BTreeIndex) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	// 加锁
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

// Get 取数据, 取数据需要加锁吗？——————> 不需要！因为google的btree库读是安全的！！！*****
func (bt *BTreeIndex) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	// bt.lock.Lock()
	btreeItem := bt.tree.Get(it)
	// bt.lock.Unlock()
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

// Delete 在内存中删除索引
func (bt *BTreeIndex) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTreeIndex) Close() error {
	bt.tree = nil
	bt.lock = nil
	return nil
}

// Iterator 建立内存索引迭代器
func (bt *BTreeIndex) Iterator(reverse bool) Iterator {
	if bt == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTreeIndex 索引迭代器（不面向用户）
// 问题：要将items都保存在内存中的values数组里，可能导致内存不够用！！！！
// 但是没办法，因为google btree自带的迭代器不能满足需求。
type btreeIterator struct {
	currIndex int     // 当前位置指针：当前遍历到values的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // 从内存取出的 key+位置索引 信息
}

func newBTreeIterator(t *btree.BTree, reverse bool) *btreeIterator {
	var i int
	values := make([]*Item, t.Len())

	// 定义一个方法，用于在遍历中保存b树上的所有item
	saveItems := func(bi btree.Item) bool {
		values[i] = bi.(*Item)
		i++
		return true // 如果返回FALSE就会终止BTree的遍历
	}

	if reverse {
		// 反向遍历b树，接受一个遍历时的处理方法，将b树中的items保存下来
		t.Descend(saveItems)
	} else {
		// 正向遍历b树，接受一个遍历时的处理方法
		t.Ascend(saveItems)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}

}

func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0 // currIndex设置为0，即回到起点
}

func (bti *btreeIterator) Seek(key []byte) {
	// values本身就是有序的
	// 二分查找
	if bti.reverse {
		// currIndex设置为找到第一个小于等于key的item的下标
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0 // 找到第一个小于等于key的
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0 // 找到第一个大于等于key的
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

func (bti *btreeIterator) IsValid() bool {
	return bti.currIndex < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

func (bti *btreeIterator) Close() {
	// 清理掉临时数组
	bti.values = nil
}
