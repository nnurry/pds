package metadata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/nnurry/pds/db"
	"github.com/redis/go-redis/v9"
)

// ---------------- CARDINAL ----------------

type Cardinal interface {
	Add([]byte) bool
	Cardinality() uint64
	Type() string
	Key() string
	Serialize() []byte
	Deserialize([]byte) error
}

// -------- Std hyperloglog --------

type stdHLLMeta struct {
	cardinalType string
	key          string
}

type StdHLL struct {
	core *hyperloglog.Sketch
	meta *stdHLLMeta
}

func NewStdHLL(precision int, sparse bool, key string) *StdHLL {
	hll := &StdHLL{}
	hll.setStdHLLCore(14, false)
	hll.setStdHLLMeta(key)
	return hll
}

func (hll *StdHLL) setStdHLLCore(precision int, sparse bool) {
	if precision == 14 && sparse {
		hll.core = hyperloglog.New14()
	} else if precision == 14 && !sparse {
		hll.core = hyperloglog.NewNoSparse()
	} else if precision == 16 && sparse {
		hll.core = hyperloglog.New16()
	} else if precision == 16 && !sparse {
		hll.core = hyperloglog.New16NoSparse()
	}
}

func (hll *StdHLL) setStdHLLMeta(key string) {
	hll.meta = &stdHLLMeta{
		cardinalType: "STD_HLL",
		key:          key,
	}
}

func (hll *StdHLL) Add(value []byte) bool {
	inserted := hll.core.Insert(value)
	return inserted
}

func (hll *StdHLL) Cardinality() uint64 {
	cardinality := hll.core.Estimate()
	return cardinality
}

func (hll *StdHLL) Type() string {
	return hll.meta.cardinalType
}

func (hll *StdHLL) Key() string {
	return hll.meta.key
}

func (hll *StdHLL) Serialize() []byte {
	blob, err := hll.core.MarshalBinary()
	if err != nil {
		return nil
	}
	return blob
}

func (hll *StdHLL) Deserialize(data []byte) error {
	return hll.core.UnmarshalBinary(data)
}

// -------- Redis hyperloglog --------

type redisHLLMeta struct {
	cardinalType string
	key          string
}

type RedisHLL struct {
	core *redis.Client
	meta *redisHLLMeta
}

func NewRedisHLL(key string) *RedisHLL {
	hll := &RedisHLL{}
	hll.setRedisHLLCore(db.RedisClient())
	hll.setRedisHLLMeta(key)
	return hll
}

func (hll *RedisHLL) setRedisHLLCore(client *redis.Client) {
	hll.core = client
}

func (hll *RedisHLL) setRedisHLLMeta(key string) {
	hll.meta = &redisHLLMeta{
		cardinalType: "STD_HLL",
		key:          key,
	}
}

func (hll *RedisHLL) Add(value []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inserted, err := hll.core.PFAdd(ctx, hll.meta.key, value).Result()
	if err != nil {
		return false
	}
	return inserted > 0
}

func (hll *RedisHLL) Cardinality() uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cardinality, err := hll.core.PFCount(ctx, hll.meta.key).Result()
	if err != nil {
		return 0
	}
	return uint64(cardinality)
}

func (hll *RedisHLL) Type() string {
	return hll.meta.cardinalType
}

func (hll *RedisHLL) getKey() string {
	return fmt.Sprintf("hll:key=%s", hll.meta.key)
}

func (hll *RedisHLL) Key() string {
	return hll.getKey()
}

func (hll *RedisHLL) Serialize() []byte {
	return []byte{}
}

func (hll *RedisHLL) Deserialize(data []byte) error {
	if len(data) > 0 {
		return errors.New("invalid data: must be empty byte slice")
	}
	// NOTE: pass the redis client object here
	hll.core = &redis.Client{}
	return nil
}

// ---------------- FILTER ----------------

type Filter interface {
	Add([]byte) bool
	Exists([]byte) bool
	Type() string
	Key() string
	MaxCard() uint
	MaxFp() float64
	HashFuncNum() uint
	HashFuncType() string
	Serialize() []byte
	Deserialize([]byte) error
}

// -------- Std bloom filter --------

type stdBloomMeta struct {
	key          string
	filterType   string
	maxCard      uint
	maxFp        float64
	hashFuncNum  uint
	hashFuncType string
}

type StdBloom struct {
	core *bloom.BloomFilter
	meta *stdBloomMeta
}

func NewStdBloom(key string, filterType string, maxCard uint, maxFp float64) *StdBloom {
	bf := &StdBloom{}
	bf.setStdBloomCore(maxCard, maxFp)
	bf.setStdBloomMeta(key, filterType, maxCard, maxFp)
	return bf
}

func (bf *StdBloom) setStdBloomCore(maxCard uint, maxFp float64) {
	bf.core = bloom.NewWithEstimates(maxCard, maxFp)
}

func (bf *StdBloom) setStdBloomMeta(key string, filterType string, maxCard uint, maxFp float64) {
	_, hashFuncNum := bloom.EstimateParameters(maxCard, maxFp)
	bf.meta = &stdBloomMeta{
		key:          key,
		filterType:   filterType,
		maxCard:      maxCard,
		maxFp:        maxFp,
		hashFuncNum:  hashFuncNum,
		hashFuncType: "murmur128",
	}
}

func (bf *StdBloom) Add(value []byte) bool {
	bf.core = bf.core.Add(value)
	return true
}

func (bf *StdBloom) Exists(value []byte) bool {
	return bf.core.Test(value)
}

func (bf *StdBloom) Type() string {
	return bf.meta.filterType
}

func (bf *StdBloom) Key() string {
	return bf.meta.key
}

func (bf *StdBloom) MaxCard() uint {
	return bf.meta.maxCard
}

func (bf *StdBloom) MaxFp() float64 {
	return bf.meta.maxFp
}

func (bf *StdBloom) HashFuncNum() uint {
	return bf.meta.hashFuncNum
}

func (bf *StdBloom) HashFuncType() string {
	return bf.meta.hashFuncType
}

func (bf *StdBloom) Serialize() []byte {
	data, err := bf.core.MarshalBinary()
	if err != nil {
		return nil
	}
	return data
}

func (bf *StdBloom) Deserialize(data []byte) error {
	err := bf.core.UnmarshalBinary(data)
	if err != nil {
		return err
	}
	return nil
}

// -------- Redis bloom filter --------

type redisBloomMeta struct {
	filterType      string
	maxCard         uint
	maxFp           float64
	hashFuncNum     uint
	hashFuncType    string
	key             string
	expansionFactor uint
	nonScaling      bool
}

type RedisBloom struct {
	core *redis.Client
	meta *redisBloomMeta
}

func NewRedisBloom(key string, filterType string, maxCard uint, maxFp float64) *RedisBloom {
	bf := &RedisBloom{}
	bf.setRedisBloomCore()
	bf.setRedisBloomMeta(key, filterType, maxCard, maxFp)
	return bf
}

func (bf *RedisBloom) setRedisBloomCore() {
	bf.core = db.RedisClient()
}

func (bf *RedisBloom) setRedisBloomMeta(key string, filterType string, maxCard uint, maxFp float64) {
	hashFuncNum := uint(math.Ceil(-(math.Log(maxFp) / math.Log(2))))
	bf.meta = &redisBloomMeta{
		key:          key,
		filterType:   filterType,
		maxCard:      maxCard,
		maxFp:        maxFp,
		hashFuncNum:  hashFuncNum,
		hashFuncType: "murmur64",
	}
}

func (bf *RedisBloom) Add(value []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	added, err := bf.core.BFAdd(ctx, bf.meta.key, value).Result()
	if err != nil {
		return false
	}
	return added
}

func (bf *RedisBloom) Exists(value []byte) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exist, err := bf.core.BFExists(ctx, bf.meta.key, value).Result()
	if err != nil {
		return false
	}
	return exist
}

func (bf *RedisBloom) Type() string {
	return bf.meta.filterType
}

func (bf *RedisBloom) getKey() string {
	return fmt.Sprintf(
		"bloom:key=%s:capacity=%d:error_rate=%f:expansion=%d:scaling=%t",
		bf.meta.key,
		bf.meta.maxCard,
		bf.meta.maxFp,
		bf.meta.expansionFactor,
		bf.meta.nonScaling,
	)
}

func (bf *RedisBloom) Key() string {
	return bf.getKey()
}

func (bf *RedisBloom) MaxCard() uint {
	return bf.meta.maxCard
}

func (bf *RedisBloom) MaxFp() float64 {
	return bf.meta.maxFp
}

func (bf *RedisBloom) HashFuncNum() uint {
	return bf.meta.hashFuncNum
}

func (bf *RedisBloom) HashFuncType() string {
	return bf.meta.hashFuncType
}

func (bf *RedisBloom) Serialize() []byte {
	return []byte{}
}

func (bf *RedisBloom) Deserialize(data []byte) error {
	if len(data) > 0 {
		return errors.New("invalid data: must be empty byte slice")
	}
	// NOTE: pass the redis client object here
	bf.core = &redis.Client{}
	return nil
}
