package main

import (
	bitcask "bitcask-go"
	"bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6377"

type BitcaskServer struct {
	dbs    map[int]*redis.Redis
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	opt := bitcask.DefaultOptions
	opt.DirPath = "/tmp/kv/redisServer"
	redisDataStructure, err := redis.NewRedis(opt)
	if err != nil {
		panic(err)
	}

	// 初始化 BitcaskServer
	Server := &BitcaskServer{
		dbs: make(map[int]*redis.Redis),
	}
	Server.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务端
	Server.server = redcon.NewServer(addr, execClientCommand, Server.accept, Server.close)
	Server.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
