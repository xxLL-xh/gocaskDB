package main

import (
	bitcask "bitcask-go"
	"fmt"
	"github.com/gin-gonic/gin"
)

var c *Controller

func init() {
	// 初始化 DB 实例
	options := bitcask.DefaultOptions
	options.DirPath = "/tmp/DB-http"
	db, err := bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
	c = NewDBController(db)
}

func main() {
	g := gin.Default()

	g.PUT("/kv/putKVPairs", c.PutHandler)
	g.PUT("/kv/delete", c.DeleteHandler)
	g.POST("/kv/get", c.GetHandler)
	g.GET("/kv/listKeys", c.ListKeyHandler)
	g.GET("/kv/showStat", c.StatHandler)

	err := g.Run(":5000")
	if err != nil {
		return
	}
}
