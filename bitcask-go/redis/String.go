package redis

import (
	"encoding/binary"
	"time"
)

/*
String

key --> type | expireTime | payload

*/

// Set 调用Put实现Set    key + ttl + value
func (rds *Redis) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码 ： key --> type + expire + payload(value)
	buf := make([]byte, binary.MaxVarintLen64+1) // type + expire
	buf[0] = String

	i := 1
	expireTime := int64(0)
	if ttl != 0 {
		expireTime = time.Now().Add(ttl).UnixNano()
	}
	i += binary.PutVarint(buf[i:], expireTime)

	encodedValue := make([]byte, i+len(value))
	copy(encodedValue[:i], buf[:i])
	copy(encodedValue[i:], value)

	// 调用goCask接口
	return rds.db.Put(key, encodedValue)
}

func (rds *Redis) Get(key []byte) ([]byte, error) {
	// 取出编码过的value
	encodedValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	index := 0

	dataType := encodedValue[0]
	index++

	if dataType != String {
		return nil, ErrWrongOperation
	}

	expire, n := binary.Varint(encodedValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encodedValue[index:], nil
}
