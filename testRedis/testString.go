package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
)

var client *redis.Client

// 获得客户端，初始化函数自动执行
func init() {
	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "redis6379", // no password set
		DB:       0,           // use default DB
	})
}

// TestGetAndSet 测试get和set方法
func TestGetAndSet() {
	err := client.Set(ctx, "name", "golang-teck-stack.com", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := client.Get(ctx, "name").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("name", val)
	// 运行结果： name golang-teck-stack.com
}

// TestMSetAndMGet 测试mget和mset方法
func TestMSetAndMGet() {
	statusCmd := client.MSet(ctx, "name", "golang技术栈", "url", "golang-teck-stack.com", "author", "老郭")
	sliceCmd := client.MGet(ctx, "name", "url", "author")
	fmt.Printf("statusCmd: %v\n", statusCmd)
	fmt.Printf("sliceCmd: %v\n", sliceCmd)
	// 运行结果：sc: mget name url author: [golang技术栈 golang-teck-stack.com 老郭]
}

// TestIncrAndDecr 测试incr和decr
func TestIncrAndDecr() {
	const KEY = "score"
	client.Set(ctx, KEY, "100", 0)
	client.Incr(ctx, KEY)
	client.Incr(ctx, KEY)
	sc := client.Get(ctx, KEY)
	fmt.Printf("sc: %v\n", sc)

	client.Decr(ctx, KEY)

	sc = client.Get(ctx, KEY)
	fmt.Printf("sc: %v\n", sc)

	// 运行结果：
	/*
		sc: get score: 102
		sc: get score: 101
	*/
}
