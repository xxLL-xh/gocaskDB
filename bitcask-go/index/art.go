package index

import (
	"bitcask-go/data"
	"bytes"
	goART "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 自适应基数树
// 封装了https://github.com/plar/go-adaptive-radix-tree库
type AdaptiveRadixTree struct {
	// 与b树相比，goART.New() 返回的是一个artTree而不是地址，所以这里直接是’tree goART.Tree‘结构体
	tree goART.Tree
	lock *sync.RWMutex
}

// NewART 初始化自适应基数树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goART.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	// Search()返回的是value类型，需要通过pos.(*data.LogRecordPos)强转成我们自己的类型
	pos, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return pos.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, isDeleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), isDeleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	art.tree = nil
	art.lock = nil
	return nil
}

// artIterator 索引迭代器（不面向用户）
// 问题：要将items都保存在内存中的values数组里，可能导致内存不够用！！！！
type artIterator struct {
	currIndex int     // 当前位置指针：当前遍历到values的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // 从内存取出的 key+位置索引 信息
}

func newArtIterator(t goART.Tree, reverse bool) *artIterator {
	var i int
	if reverse {
		i = t.Size() - 1
	}
	values := make([]*Item, t.Size())

	// Callback function type for tree traversal. if the callback function returns false then iteration is terminated.

	saveValues := func(node goART.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[i] = item
		if reverse {
			i--
		} else {
			i++
		}
		return true
	}

	t.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}

}

func (arti *artIterator) Rewind() {
	arti.currIndex = 0 // currIndex设置为0，即回到起点
}

func (arti *artIterator) Seek(key []byte) {
	// values本身就是有序的
	// 二分查找
	if arti.reverse {
		// currIndex设置为找到第一个小于等于key的item的下标
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0 // 找到第一个小于等于key的
		})
	} else {
		arti.currIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0 // 找到第一个大于等于key的
		})
	}
}

func (arti *artIterator) Next() {
	arti.currIndex += 1
}

func (arti *artIterator) IsValid() bool {
	return arti.currIndex < len(arti.values)
}

func (arti *artIterator) Key() []byte {
	return arti.values[arti.currIndex].key
}

func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currIndex].pos
}

func (arti *artIterator) Close() {
	// 清理掉临时数组
	arti.values = nil
}
