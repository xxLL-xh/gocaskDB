package tmp

import (
	"bitcask-go/data"
	"bytes"
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	maxLevel    int     = 18
	probability float64 = 1
)

type Node struct {
	next []*Element // 跳表中，每个节点可能指向n个其他节点
}

// Element element stored inSkipList
type Element struct {
	Node         // 指向下一个节点的指针（有n层，每层都可能指向不同的节点）
	key   []byte // 实际保存的数据
	value interface{}
}

// MySkipList 虚拟头结点，记录了跳表的一些数据信息
type MySkipList struct {
	Node

	Len      int // 可以访问跳表的长度
	maxLevel int // 最大层高

	randSource rand.Source // not safe for concurrent use by multiple goroutines

	probability float64
	probTable   []float64

	prevNodesCache []*Node // 在查找时暂存 要插入的节点 在每一层索引上，上一个节点的指针
}

// 1 1/2 1/4 1/8 1/16 ...
func (sl *MySkipList) initProbTable() {
	for i := 0; i < sl.maxLevel; i++ {
		sl.probTable[i] = math.Pow(0.5, float64(i))
	}
}

// 1/8
// 按照概率生成随机层数
func (sl *MySkipList) randomLevel() (level int) {
	r := float64(sl.randSource.Int63()) / (1 << 63) // ⽣成⼀个[0, 1)的概率值

	level = 1

	for level < sl.maxLevel && r < sl.probTable[level] { // 找到第⼀个prob⼩于 r 的层数
		level++
	}

	return
}

// 找到key对应节点在每一层索引的前一个节点
func (sl *MySkipList) backNodes(key []byte) []*Node {
	var pre = &sl.Node
	var cur *Element

	prevNodes := sl.prevNodesCache

	for i := sl.maxLevel - 1; i >= 0; i-- { // 从最⾼层索引开始遍历
		cur = pre.next[i] // 当前节点 为 第i层索引上的下⼀个节点

		for cur != nil && bytes.Compare(key, cur.key) > 0 { // 如果待查询的key⽐cur节点的key⼤
			pre = &cur.Node   // 当前节点变成pre
			cur = cur.next[i] // 通过当前层的索引跳到下⼀个位置
		} // 循环跳出后，key节点应位于pre和cur之间

		prevNodes[i] = pre // 将当前的pre节点缓存到跳表中的对应层上
	} // 到下一层继续寻找
	return prevNodes
}

// Get 根据 key 查找对应的 Element 元素
// 未找到则返回nil
func (sl *MySkipList) Get(key []byte) *Element {
	var prev = &sl.Node
	var next *Element

	for i := sl.maxLevel - 1; i >= 0; i-- {
		next = prev.next[i]

		for next != nil && bytes.Compare(key, next.key) > 0 {
			prev = &next.Node
			next = next.next[i]
		}
	}

	if next != nil && bytes.Compare(next.key, key) <= 0 {
		return next
	}
	return nil
}

// Put 存储⼀个元素⾄跳表中，如果key已经存在，则会更新其对应的value
// 因此此跳表的实现暂不⽀持相同的key
func (sl *MySkipList) Put(key []byte, value interface{}) *Element {
	var element *Element
	prev := sl.backNodes(key) // 找出key节点在每⼀层索引应该放的位置的前⼀个节点

	if element = prev[0].next[0]; element != nil && bytes.Compare(element.key, key) <= 0 {
		element.value = value // 如果key和prev的下⼀个节点的key相等，说明该key已存在，更新value返回 即可
		return element
	}

	element = &Element{
		Node: Node{
			next: make([]*Element, sl.randomLevel()), // 初始化ele的next索引层
		},
		key:   key,
		value: value,
	}
	// 当前key应该插⼊的位置已经确定，就在prev的下⼀个位置
	for i := range element.next { // 遍历ele的所有索引层，建⽴节点前后联系
		element.next[i] = prev[i].next[i]
		prev[i].next[i] = element
	}

	sl.Len++
	return element
}

type SafeSkipList struct {
	skipList *MySkipList
	lock     *sync.RWMutex
}

func (sl *SafeSkipList) Size() int {
	return sl.skipList.Len
}

func NewSafeSkipList() *SafeSkipList {
	sl := &MySkipList{
		maxLevel:    maxLevel,
		randSource:  rand.NewSource(time.Now().UnixNano()),
		probability: probability,
	}
	sl.initProbTable()

	return &SafeSkipList{
		skipList: sl,
		lock:     new(sync.RWMutex),
	}
}

// Put 存数据，存之前需要加锁
func (sl *SafeSkipList) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	// 加锁
	sl.lock.Lock()
	oldItem := sl.skipList.Put(key, pos)
	sl.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.value.(*data.LogRecordPos)
}

// Get 取数据, 取数据需要加锁吗？——————> 不需要！因为google的btree库读是安全的！！！*****
func (sl *SafeSkipList) Get(key []byte) *data.LogRecordPos {
	sl.lock.RLock()
	old := sl.skipList.Get(key)
	sl.lock.RUnlock()
	if old == nil {
		return nil
	}
	return old.value.(*data.LogRecordPos)
}

/*// Delete 在内存中删除索引
func (sl *SafeSkipList) Delete(key []byte) (*data.LogRecordPos, bool) {

	sl.lock.Lock()
	oldItem := sl.skipList.Delete(key)
	sl.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (sl *SafeSkipList) Close() error {
	bt.tree = nil
	bt.lock = nil
	return nil
}

// Iterator 建立内存索引迭代器
func (sl *SafeSkipList) Iterator(reverse bool) Iterator {
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
*/
