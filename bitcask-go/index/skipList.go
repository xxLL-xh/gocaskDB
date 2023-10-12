package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/chen3feng/stl4go"
	"sort"
	"sync"
)

// MySkipList based on https://github.com/chen3feng/stl4go
type MySkipList struct {
	list *stl4go.SkipList[[]byte, *data.LogRecordPos]
	lock *sync.RWMutex
}

func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// NewSkipList Initialize the SkipList index
func NewSkipList() *MySkipList {
	return &MySkipList{
		list: stl4go.NewSkipListFunc[[]byte, *data.LogRecordPos](Compare),
		lock: new(sync.RWMutex),
	}
}

// Put Inserts a key-value pair into the SkipList index
func (sl *MySkipList) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	old := sl.list.Find(key)
	sl.list.Insert(key, pos)
	if old == nil {
		return nil
	}
	return *old
}

// Get Gets the value corresponding to the key from the SkipList index
func (sl *MySkipList) Get(key []byte) *data.LogRecordPos {
	sl.lock.RLock()
	defer sl.lock.RUnlock()

	res := sl.list.Find(key)
	if res != nil {
		return *res
	}
	return nil
}

// Delete Deletes the key-value pair corresponding to the key from the SkipList index
func (sl *MySkipList) Delete(key []byte) (*data.LogRecordPos, bool) {
	sl.lock.Lock()
	defer sl.lock.Unlock()

	old := sl.list.Find(key)
	if old == nil {
		return nil, false
	}

	return *old, sl.list.Remove(key)
}

// Size Gets the number of key-value pairs in the SkipList index
func (sl *MySkipList) Size() int {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return sl.list.Len()
}

func (sl *MySkipList) Close() error {
	sl.lock = nil
	sl.list = nil
	return nil
}

// Iterator Gets the iterator of the SkipList index
// If the reverse is true, the iterator is traversed in reverse order,
// otherwise it is traversed in order
func (sl *MySkipList) Iterator(reverse bool) Iterator {
	sl.lock.RLock()
	defer sl.lock.RUnlock()
	return NewSkipListIterator(sl, reverse)
}

type SkipListIterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

// NewSkipListIterator Initializes the SkipList index iterator
func NewSkipListIterator(sl *MySkipList, reverse bool) *SkipListIterator {
	// Estimate the expected slice capacity based on skip list size
	expectedSize := sl.Size()

	// Initialize with empty slice and expected capacity
	values := make([]*Item, 0, expectedSize)

	// for each operation
	saveToValues := func(K []byte, V *data.LogRecordPos) {
		item := &Item{
			key: K,
			pos: V,
		}
		values = append(values, item)
	}
	sl.list.ForEach(saveToValues)

	// Reverse the values slice if reverse is true
	if reverse {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
		}
	}

	return &SkipListIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind Resets the iterator to the beginning
func (sl *SkipListIterator) Rewind() {
	sl.currIndex = 0
}

// Seek Positions the iterator to the first key
// that is greater or equal to the specified key
func (sl *SkipListIterator) Seek(key []byte) {
	// binary search
	if sl.reverse {
		sl.currIndex = sort.Search(len(sl.values), func(i int) bool {
			return bytes.Compare(sl.values[i].key, key) <= 0
		})
	} else {
		sl.currIndex = sort.Search(len(sl.values), func(i int) bool {
			return bytes.Compare(sl.values[i].key, key) >= 0
		})
	}
}

// Next Positions the iterator to the next key
// If the iterator is positioned at the last key,
// the iterator is positioned to the start of the iterator
func (sl *SkipListIterator) Next() {
	sl.currIndex += 1
}

// Valid Determines whether the iterator is positioned at a valid key
func (sl *SkipListIterator) IsValid() bool {
	return sl.currIndex < len(sl.values)
}

// Key Gets the key at the current iterator position
func (sl *SkipListIterator) Key() []byte {
	return sl.values[sl.currIndex].key
}

// Value Gets the value at the current iterator position
func (sl *SkipListIterator) Value() *data.LogRecordPos {
	return sl.values[sl.currIndex].pos
}

// Close Closes the iterator
func (sl *SkipListIterator) Close() {
	sl.values = nil
}
