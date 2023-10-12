package redis

import (
	goCask "bitcask-go"
	"errors"
)

// RedisDataType 支持的数据类型
type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

// ErrWrongOperation 错误
var ErrWrongOperation = errors.New("the data type of the key don't support this operation")

// Redis 服务结构体，提供redis服务
type Redis struct {
	db *goCask.DB // 将redis类型存储在gocaskDB中
}

// NewRedis 初始化Redis服务
func NewRedis(opt goCask.Options) (*Redis, error) {
	db, err := goCask.Open(opt)
	if err != nil {
		return nil, err
	}
	return &Redis{
		db: db,
	}, nil
}

// Close 关闭服务
func (rds *Redis) Close() error {
	return rds.db.Close()
}
