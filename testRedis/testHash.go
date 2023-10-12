package main

import (
	"fmt"
)

// TestHSetAndHGet 测试hset和hget
func TestHSetAndHGet() {
	client.HSet(ctx, "site", "name", "golang-tech-stack.com")
	sc := client.HGet(ctx, "site", "name")
	fmt.Printf("get site-name sc:%v", sc)
	// 输出结果：golang-tech-stack.com <nil>
}

// TestHMSetAndHMGet 测试hmset和hmget
func TestHMSetAndHMGet() {
	client.HMSet(ctx, "site", "name", "golang技术栈", "url", "golang-tech-stack.com", "author", "老郭")
	sc := client.HMGet(ctx, "site", "name", "url", "author")
	fmt.Println(sc.Result())
	// 输出结果：[golang技术栈 golang-tech-stack.com 老郭] <nil>
}

// TestHKeysAndHVals 测试hkeys和hvals
func TestHKeysAndHVals() {
	client.HMSet(ctx, "site", "name", "golang技术栈", "url", "golang-tech-stack.com", "author", "老郭")
	ssc := client.HKeys(ctx, "site")
	fmt.Println(ssc.Result())

	ssc2 := client.HVals(ctx, "site")
	fmt.Println(ssc2.Result())
	// 输出结果：
	// [name url author] <nil>
	// [golang技术栈 golang-tech-stack.com 老郭] <nil>
}
