package redis

import (
	bitcask "bitcask-go"
	"encoding/binary"
)

/*
hash (两部分)

meta:
	key --> type | expire | version | size

data:
	key | version | field --> value
*/

// key | version | field --> value
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	// key
	index := 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// field
	copy(buf[index:], hk.field)

	return buf
}

// ********************************************************************************************************************

func (rds *Redis) HSet(key, field, value []byte) (bool, error) {
	// 先查找元数据
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash 数据部分的 key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.PendingPut(key, meta.encode())
	}
	_ = wb.PendingPut(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *Redis) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *Redis) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist == true {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.PendingPut(key, meta.encode())
		_ = wb.PendingDelete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}
