package redis

import "errors"

// Del 删除key
// 注意！ 只有String是删除了键值对。而其他类型是删除了【元数据】！
func (rds *Redis) Del(key []byte) error {
	return rds.db.Delete(key)
}

// Type 得到key的类型
func (rds *Redis) Type(key []byte) (RedisDataType, error) {
	encodedValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encodedValue) == 0 {
		return 0, errors.New("value is empty")
	}

	return encodedValue[0], nil
}
