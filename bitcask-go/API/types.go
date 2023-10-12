package main

import bitcask_go "bitcask-go"

type ResponseMessage struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type CreateDBRequest struct {
	// 数据文件的大小
	DataFileSize int64 `json:"data_file_size"`

	// 每次写数据是否持久化
	SyncWrites bool `json:"sync_writes"`

	// 每写多少字节自动持久化
	SyncPerBytes uint `json:"sync_per_bytes"`

	// 索引类型
	IndexType bitcask_go.IndexerType `json:"index_type"`

	// 是否在启动时使用内存映射MemoryMap加速加载
	MMapAtStartupNeeded bool `json:"m_map_at_startup_needed"`

	// merge阈值
	MergeRatioThreshold float32 `json:"merge_ratio_threshold"`
}

type PutKVPairsRequest struct {
	KVPairs map[string]string `json:"kv_pairs"`
}

type PutKVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type GetOneValueRequest struct {
	Key string `json:"key"`
}

type GetOneValueResponse struct {
	Code  int    `json:"code"`
	Value string `json:"value"`
}

type DeleteDataRequest struct {
	Key string `json:"key"`
}

type StatResponse struct {
	Code int              `json:"code"`
	Stat *bitcask_go.Stat `json:"stat"`
}

type ListKeyResponse struct {
	Code int      `json:"code"`
	Keys []string `json:"keys"`
}
