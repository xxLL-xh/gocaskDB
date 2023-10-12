package benchmark

import (
	goCaskDB "bitcask-go"
	"bitcask-go/utils"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/rand"
	"os"
	"testing"
	"time"
)

/*
（一） b.N原理
	b.N 从 1 开始，如果基准测试函数在1秒内就完成 (默认值)，则 b.N 增加，并再次运行基准测试函数。
	b.N 在近似这样的序列中不断增加；1, 2, 3, 5, 10, 20, 30, 50, 100, 1000, 20000 等等。
	基准框架试图变得聪明，如果它看到当b.N较小而且测试很快就完成的时候，它将让序列增加地更快

（二） 测试方向
功能测试
	（1）Put时，setActiveFile
	（2）Merge
	（3）http、redis 协议
性能测试
	（1）mmap 加速
	（2）内存keyDir
		随机读写
		listKeys
基准测试
	（3）随机读写（3种）
		写主导		sync
					无sync

	（4）zipf分布
		写主导
		读主导
		读写五五开

*/

const (
	existedNum   = 100000
	valueLen     = 1024
	p            = 0.5
	missRate     = float64(6) / float64(5) // get时，键值对不在数据库中的概率
	segmentSize  = 32 * 1024 * 1024
	syncPerBytes = 512 * 1024
)

func Benchmark_goCaskDBRandom(b *testing.B) {
	// 以p的概率 随机读写
	// 加上b.StopTimer()
	// b.StartTimer()
	db := getGoCaskDB()

	// 初始环境：存在existedNum个键值对
	for i := 0; i < existedNum; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(valueLen))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()
	num := existedNum
	//println(num)
	//println(b.N)

	for i := 0; i < b.N; i++ {
		// 随机选择读写操作
		//b.StopTimer()
		prob := rand.Float64()
		//b.StartTimer()

		if prob < p {
			key := utils.GetTestKey(rand.Intn(int(float64(num) * missRate)))
			_, err := db.Get(key)

			if err != nil && err != goCaskDB.ErrKeyNotFound {
				b.Fatal(err)
			}
		} else {
			//b.StopTimer()
			key := utils.GetTestKey(num)
			value := utils.RandomValue(valueLen)
			//b.StartTimer()

			err := db.Put(key, value)
			num += 1
			assert.Nil(b, err)
		}
	}
	//println(num)
}

func Benchmark_goLevelDBRandom(b *testing.B) {
	// 以p的概率 随机读写
	// 加上b.StopTimer()
	// b.StartTimer()
	db := getLevelDB()

	// 初始环境：存在existedNum个键值对
	for i := 0; i < existedNum; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(valueLen), nil)
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())

	// SYNC 选项
	wo := &opt.WriteOptions{Sync: false}

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum

	for i := 0; i < b.N; i++ {
		// 随机选择读写操作
		//b.StopTimer()
		prob := rand.Float64()
		//b.StartTimer()

		if prob < p {
			key := utils.GetTestKey(rand.Intn(int(float64(num) * missRate)))
			_, err := db.Get(key, nil)

			if err != nil && err != leveldb.ErrNotFound {
				b.Fatal(err)
			}
		} else {
			//b.StopTimer()
			key := utils.GetTestKey(num)
			value := utils.RandomValue(valueLen)
			//b.StartTimer()

			err := db.Put(key, value, wo)
			num += 1
			assert.Nil(b, err)
		}
	}
}

func Benchmark_PebbleRandom(b *testing.B) {
	// 以p的概率 随机读写
	// 加上b.StopTimer()
	// b.StartTimer()
	db := getPebbleDB()
	opt := &pebble.WriteOptions{Sync: true}

	// 初始环境：存在existedNum个键值对
	for i := 0; i < existedNum; i++ {
		err := db.Set(utils.GetTestKey(i), utils.RandomValue(valueLen), opt)
		assert.Nil(b, err)
	}

	opt.Sync = true
	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum

	for i := 0; i < b.N; i++ {
		// 随机选择读写操作
		//b.StopTimer()
		prob := rand.Float64()
		//b.StartTimer()

		if prob < p {
			key := utils.GetTestKey(rand.Intn(int(float64(num) * missRate)))
			_, _, err := db.Get(key)

			if err != nil && err != pebble.ErrNotFound {
				b.Fatal(err)
			}
		} else {
			//b.StopTimer()
			key := utils.GetTestKey(num)
			value := utils.RandomValue(valueLen)
			//b.StartTimer()

			err := db.Set(key, value, opt)
			num += 1
			assert.Nil(b, err)
		}
	}
}

// boltDB
func Benchmark_BoltDBRandom(b *testing.B) {
	db := getBoltDB()
	defer func(db *bolt.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("benchmark"))
		if err != nil {
			return err
		}
		for i := 0; i < existedNum; i++ {
			key := utils.GetTestKey(i)
			value := utils.RandomValue(valueLen)
			err = bucket.Put(key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
	assert.Nil(b, err)

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum

	/*	// 每次都sync
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			err = db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("benchmark"))
				if bucket == nil {
					return fmt.Errorf("bucket not found")
				}
				b.StartTimer()
				// Randomly select read or write operation
				if rand.Float64() < p {
					key := utils.GetTestKey(rand.Intn(int(float64(num) * missRate)))
					_ = bucket.Get(key)
				} else {
					key := utils.GetTestKey(num)
					value := utils.RandomValue(valueLen)
					err = bucket.Put(key, value)
					num += 1
					if err != nil {
						return err
					}
				}
				return nil
			})
			assert.Nil(b, err)

		}*/

	// 不每次都sync
	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("benchmark"))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		for i := 0; i < b.N; i++ {
			// Randomly select read or write operation
			if rand.Float64() < p {
				key := utils.GetTestKey(rand.Intn(int(float64(num) * missRate)))
				_ = bucket.Get(key)
			} else {
				key := utils.GetTestKey(num)
				value := utils.RandomValue(valueLen)

				err = bucket.Put(key, value)
				num += 1
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	assert.Nil(b, err)
}

func Benchmark_goCaskZipF(b *testing.B) {
	db := getGoCaskDB()

	// Zipfian 随机读写
	for i := 0; i < existedNum; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(valueLen))
		assert.Nil(b, err)
	}

	all := existedNum + b.N
	keys := make([][]byte, all)
	for i := 0; i < all; i++ {
		keys[i] = utils.GetTestKey(i)
	}

	zipfianDistribution := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.1, 1, uint64(all))

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum // 目前的键值对数量

	for i := 0; i < b.N; i++ {
		keyIndex := zipfianDistribution.Uint64() % uint64(float64(num)*missRate)
		key := keys[keyIndex]

		prob := rand.Float64()

		if prob < p {
			_, err := db.Get(key)
			if err != nil && err != goCaskDB.ErrKeyNotFound {
				b.Fatal(err)
			}
		} else {
			key = keys[zipfianDistribution.Uint64()%uint64(num)]
			value := utils.RandomValue(valueLen)

			err := db.Put(key, value)
			num++
			assert.Nil(b, err)
		}
	}
}

func Benchmark_LevelDBZipF(b *testing.B) {
	db := getLevelDB()

	// Zipfian 随机读写
	for i := 0; i < existedNum; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(valueLen), nil)
		assert.Nil(b, err)
	}

	all := existedNum + b.N
	keys := make([][]byte, all)
	for i := 0; i < all; i++ {
		keys[i] = utils.GetTestKey(i)
	}

	zipfianDistribution := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.1, 1, uint64(all))

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum // 目前的键值对数量

	for i := 0; i < b.N; i++ {

		keyIndex := zipfianDistribution.Uint64() % uint64(float64(num)*missRate)
		key := keys[keyIndex]

		prob := rand.Float64()

		if prob < p {
			_, err := db.Get(key, nil)
			if err != nil && err != leveldb.ErrNotFound {
				b.Fatal(err)
			}
		} else {
			key = keys[zipfianDistribution.Uint64()%uint64(num)]
			value := utils.RandomValue(valueLen)

			err := db.Put(key, value, nil)
			num++
			assert.Nil(b, err)
		}
	}
}

func Benchmark_PebbleZipF(b *testing.B) {
	db := getPebbleDB()
	opt := &pebble.WriteOptions{Sync: false}

	// Zipfian 随机读写
	for i := 0; i < existedNum; i++ {
		err := db.Set(utils.GetTestKey(i), utils.RandomValue(valueLen), opt)
		assert.Nil(b, err)
	}

	all := existedNum + b.N
	keys := make([][]byte, all)
	for i := 0; i < all; i++ {
		keys[i] = utils.GetTestKey(i)
	}

	zipfianDistribution := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.1, 1, uint64(all))

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum // 目前的键值对数量

	for i := 0; i < b.N; i++ {

		keyIndex := zipfianDistribution.Uint64() % uint64(float64(num)*missRate)
		key := keys[keyIndex]

		prob := rand.Float64()

		if prob < p {
			_, _, err := db.Get(key)
			if err != nil && err != pebble.ErrNotFound {
				b.Fatal(err)
			}
		} else {
			key = keys[zipfianDistribution.Uint64()%uint64(num)]
			value := utils.RandomValue(valueLen)

			err := db.Set(key, value, opt)
			num++
			assert.Nil(b, err)
		}
	}
}

func Benchmark_BoltDBZipF(b *testing.B) {
	db := getBoltDB()
	defer func(db *bolt.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)

	err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("benchmark"))
		if err != nil {
			return err
		}
		for i := 0; i < existedNum; i++ {
			key := utils.GetTestKey(i)
			value := utils.RandomValue(valueLen)
			err = bucket.Put(key, value)
			if err != nil {
				return err
			}
		}
		return nil
	})
	assert.Nil(b, err)

	all := existedNum + b.N
	keys := make([][]byte, all)
	for i := 0; i < all; i++ {
		keys[i] = utils.GetTestKey(i)
	}

	zipfianDistribution := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.1, 1, uint64(all))

	rand.Seed(time.Now().UnixNano())

	b.ResetTimer()
	b.ReportAllocs()

	num := existedNum

	err = db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("benchmark"))
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		for i := 0; i < b.N; i++ {
			// Randomly select read or write operation

			if rand.Float64() < p {
				keyIndex := zipfianDistribution.Uint64() % uint64(float64(num)*missRate)
				key := keys[keyIndex]

				_ = bucket.Get(key)
			} else {

				key := keys[zipfianDistribution.Uint64()%uint64(num)]
				value := utils.RandomValue(valueLen)

				err = bucket.Put(key, value)
				num += 1
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	assert.Nil(b, err)
}

func Benchmark_Hash(b *testing.B) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.Hash

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < existedNum; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(valueLen))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err = db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != goCaskDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Btree(b *testing.B) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.BTree

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err = db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != goCaskDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func TestDB_BtreeListKey(t *testing.T) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.BTree

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	start := time.Now()

	key := db.ListKeys()
	assert.NotNil(t, key)

	t.Log(time.Since(start))
}

func Benchmark_ART(b *testing.B) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.ART

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err = db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != goCaskDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func TestDB_ARTListKey(t *testing.T) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.ART

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	start := time.Now()

	key := db.ListKeys()
	assert.NotNil(t, key)

	t.Log(time.Since(start))
}

func Benchmark_SkipList(b *testing.B) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.SkipList

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err = db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != goCaskDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func TestDB_SkipListListKey(t *testing.T) {
	options := goCaskDB.DefaultOptions

	options.IndexType = goCaskDB.SkipList

	dir, _ := os.MkdirTemp("/tmp/bench_tmp", "Index")
	options.DirPath = dir

	db, err := goCaskDB.Open(options)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	start := time.Now()

	key := db.ListKeys()
	assert.NotNil(t, key)

	t.Log(time.Since(start))
}
