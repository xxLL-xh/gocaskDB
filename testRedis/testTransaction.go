package main

import (
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"sync"
)



// 所有操作都成功才提交，有一个失败全部回滚
func testTransaction() {
	_, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < 100; i++ {
			pipe.Get(ctx, fmt.Sprintf("key%d", i))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

// 注意！！！！！！ 如果设置了”protect-mode yes“ 最多只允许4个并行进程

// TestTransactionWithWatch 乐观锁
// 乐观锁的核心思想在于不直接进行锁定，而是在执行操作之前先进行观察，确认操作是否可以安全执行。
// 如果发现其他并发操作修改了关键数据，则放弃当前操作并进行重试。
// 这种方式适用于并发读写较少的情况，避免了显式的锁定和解锁操作，提高了并发性能。
func TestTransactionWithWatch() {
	/*
		实验：多个进程同时向redis服务器提交事务
		redis需要保证在执行依次一个事务内的命令时，不被来自其他进程的命令插入

		使用watch，如果事务提交之前，某个key变化了（说明被其他事务更改了），就让事务提交失败
	*/

	// 最大重试次数
	const maxRetries = 1000

	increment := func(key string) error {
		// Transactional function.
		txFunc := func(tx *redis.Tx) error {
			//Get current value or zero.
			n, err := tx.Get(ctx, key).Int()
			if err != nil && err != redis.Nil {
				return err
			}

			println(n)

			// Actual operation (local in optimistic lock).
			n++

			//Operation is committed only if the watched keys remain unchanged.
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				_, err = pipe.Set(ctx, key, n, 0).Result()
				return err
			})
			return err
		}

		for i := 0; i < maxRetries; i++ {
			// Watch 与 Exec类似，可以看做如果txFunc方法执行期间，key没被其他事务修改，就执行事务方法txFunc
			// 即，如果事务执行期间，key未被其他事务修改，则事务才成功提交。否则，事务失败，并重新尝试
			err := client.Watch(ctx, txFunc, key)

			// 如果一个请求在执行事务函数期间，发现被观察的key已经被其他请求修改了（可能是另一个并发的递增操作），
			// 那么这个事务函数执行会失败，Redis会返回一个错误redis.TxFailedErr。
			// 当出现这个错误时，代码会继续进行下一次循环（即重试机制），再次尝试执行递增操作。
			if err == nil {
				log.Print("success")
				// Success.
				return nil
			}
			if err == redis.TxFailedErr {
				// 重试
				println("retry")
				continue
			}
			// 返回错误
			return err
		}
		return errors.New("到达最大次数")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {    // 注意：如果设置了”protect-mode yes“ 最多只允许4个并行进程
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := increment("score"); err != nil {
				fmt.Println("increment error:", err)
			}
		}()
	}
	wg.Wait()
	n, err := client.Get(ctx, "score").Int()
	fmt.Println("ended with", n, err)
}

func main() {
	TestTransactionWithWatch()
}
