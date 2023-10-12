package benchmark

import (
	goCaskDB "bitcask-go"
	"github.com/boltdb/bolt"
	"github.com/cockroachdb/pebble"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
)

func getGoCaskDB() *goCaskDB.DB {
	opt := goCaskDB.DefaultOptions
	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Cask")
	opt.DirPath = dir
	opt.SyncWrites = false
	opt.IndexType = goCaskDB.SkipList

	db, err := goCaskDB.Open(opt)
	if err != nil {
		panic(err)
	}

	return db
}

func getBoltDB() *bolt.DB {
	dir := "/tmp/bench_tmp/bolt_db.bolt"
	opts := bolt.DefaultOptions

	opts.NoGrowSync = true

	boltdb, err := bolt.Open(dir, 0777, opts)
	if err != nil {
		panic(err)
	}

	return boltdb
}

func getLevelDB() *leveldb.DB {
	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Level")

	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		panic(err)
	}

	return db
}

func getPebbleDB() *pebble.DB {
	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "pebble")

	opt := &pebble.Options{
		BytesPerSync: 0,
		MemTableSize: 4 * 1024 * 1024,
	}

	db, err := pebble.Open(dir, opt)
	if err != nil {
		panic(err)
	}

	return db
}
