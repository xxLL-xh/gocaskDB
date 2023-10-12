package main

import (
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

// 测试pipeline实例
func testPipeLine() {
	pipe := client.Pipeline()

	incr := pipe.Incr(ctx, "pipeline_counter")
	pipe.Expire(ctx, "pipeline_counter", time.Hour)

	_, err := pipe.Exec(ctx) // 调用Exec方法执行pipeline中的全部命令
	if err != nil {
		panic(err)
	}

	// 调用Exec后获得值
	fmt.Println(incr.Val())
}

// 测试Pipelined方法
func testPipelinedFunc() {
	var incr *redis.IntCmd

	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error { // 使用pipelined函数执行pipeline中的命令
		incr = pipe.Incr(ctx, "pipelined_counter")
		pipe.Expire(ctx, "pipelined_counter", time.Hour)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// 获得值
	fmt.Println(incr.Val())
}

func testPipelineSetGet() {
	// 用pipeline set 100个键值对
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < 100; i++ {
			pipe.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("key%d", i), 0)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// get
	cmds, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < 100; i++ {
			pipe.Get(ctx, fmt.Sprintf("key%d", i))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	// 遍历返回的命令
	for _, cmd := range cmds {
		fmt.Println(cmd.(*redis.StringCmd).Val())
	}

}
