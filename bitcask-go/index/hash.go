package index

import (
	"bitcask-go/data"
	"sync"
)

type SafeHashTable struct {
	hash map[string]*data.LogRecordPos
	lock *sync.RWMutex
}

func NewSafeHashTable() *SafeHashTable {
	return &SafeHashTable{
		hash: make(map[string]*data.LogRecordPos, 100000), // 一开始放入多少条数据比较好？？？
		lock: new(sync.RWMutex),
	}
}

func (h *SafeHashTable) name() {

}

func (h *SafeHashTable) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	h.lock.Lock()
	defer h.lock.Unlock()
	oldPos := h.hash[string(key)]
	h.hash[string(key)] = pos
	if oldPos == nil {
		return nil
	}

	return oldPos
}

func (h *SafeHashTable) Get(key []byte) *data.LogRecordPos {
	h.lock.RLock()
	defer h.lock.RUnlock()
	pos, found := h.hash[string(key)]
	if !found {
		return nil
	}
	return pos
}

func (h *SafeHashTable) Delete(key []byte) (*data.LogRecordPos, bool) {
	h.lock.Lock()
	defer h.lock.Unlock()
	oldPos := h.hash[string(key)]
	if oldPos == nil {
		return nil, false
	}
	delete(h.hash, string(key))
	return oldPos, true
}

func (h *SafeHashTable) Size() int {
	h.lock.RLock()
	defer h.lock.RUnlock()
	size := len(h.hash)
	return size
}

func (h *SafeHashTable) Close() error {
	h.lock = nil
	h.hash = nil
	return nil
}

func (h *SafeHashTable) Iterator(reverse bool) Iterator {
	//TODO implement me
	panic("not implemented")
}

type hashIterator struct {
	currIndex int     // 当前位置指针：当前遍历到values的下标位置
	reverse   bool    // 是否是反向遍历
	values    []*Item // 从内存取出的 key+位置索引 信息
}

func (h hashIterator) Rewind() {
	//TODO implement me
	panic("not implemented")
}

func (h hashIterator) Seek(key []byte) {
	//TODO implement me
	panic("not implemented")
}

func (h hashIterator) Next() {
	//TODO implement me
	panic("not implemented")
}

func (h hashIterator) IsValid() bool {
	//TODO implement me
	panic("not implemented")
}

func (h hashIterator) Key() []byte {
	//TODO implement me
	panic("not implemented")
}

func (h hashIterator) Value() *data.LogRecordPos {
	//TODO implement me
	panic("not implemented")
}

func (h hashIterator) Close() {
	//TODO implement me
	panic("not implemented")
}
