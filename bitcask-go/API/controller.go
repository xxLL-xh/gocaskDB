package main

import (
	bitcaskGo "bitcask-go"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"log"
	"net/http"
)
import "github.com/gin-gonic/gin"

type Controller struct {
	db *bitcaskGo.DB
}

func NewDBController(db *bitcaskGo.DB) *Controller {
	return &Controller{
		db: db,
	}

}

// CreateDB 创建数据库
func (c *Controller) CreateDB(g *gin.Context) {
	var req CreateDBRequest
	if err := g.ShouldBind(&req); err != nil {
		utils.HandleError(g, errors.New("check your request body"))
		return
	}
	// TODO

}

// PutHandler 处理put请求
func (c *Controller) PutHandler(g *gin.Context) {
	var req PutKVPairsRequest
	if err := g.ShouldBind(&req); err != nil {
		utils.HandleError(g, errors.New("check your request body"))
		return
	}
	count := 0
	for key, value := range req.KVPairs {
		if err := c.db.Put([]byte(key), []byte(value)); err != nil {
			utils.HandleError(g, err)
			log.Printf("failed to put kv in db: %v\n", err)
			return
		}
		count++
	}
	g.JSON(http.StatusOK, ResponseMessage{200, fmt.Sprintf("%d pairs are successfully put", count)})
}

func (c *Controller) GetHandler(g *gin.Context) {
	var req GetOneValueRequest
	if err := g.ShouldBind(&req); err != nil {
		utils.HandleError(g, errors.New("check your request body"))
		return
	}
	value, err := c.db.Get([]byte(req.Key))
	if err != nil {
		utils.HandleError(g, err)
		log.Printf("failed to get the value: %v\n", err)
		return
	}

	g.JSON(http.StatusOK, GetOneValueResponse{200, string(value)})
}

func (c *Controller) DeleteHandler(g *gin.Context) {
	/*var req DeleteDataResponse
	if err := g.ShouldBind(&req); err != nil {
		utils.HandleError(g, errors.New("check your request body"))
		return
	}

	for _, key := range req.Keys {
		err := c.db.Delete([]byte(key))
		if err != nil {
			utils.HandleError(g, err)
			log.Printf("failed to delete the data of key %v: %v\n", key, err)
			return
		}
	}
	g.JSON(http.StatusOK, GetOneValueResponse{200, "successfully deleted"})*/
	var req DeleteDataRequest
	if err := g.ShouldBind(&req); err != nil {
		utils.HandleError(g, errors.New("check your request body"))
		return
	}

	err := c.db.Delete([]byte(req.Key))
	if err != nil {
		utils.HandleError(g, err)
		log.Printf("failed to delete the data of key %v: %v\n", req.Key, err)
		return
	}

	g.JSON(http.StatusOK, GetOneValueResponse{200, "successfully deleted"})
}

func (c *Controller) StatHandler(g *gin.Context) {
	stat := c.db.Stat()
	g.JSON(http.StatusOK, StatResponse{Code: 200, Stat: stat})
}

func (c *Controller) ListKeyHandler(g *gin.Context) {
	keys := c.db.ListKeys()
	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}

	g.JSON(http.StatusOK, ListKeyResponse{200, result})
}
