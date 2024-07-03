package db

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	redisAddr   string = "127.0.0.1:6379"
	redisClient *redis.Client
)

func RedisClient() *redis.Client {
	if redisClient != nil {
		return redisClient
	}
	var err error
	opts := &redis.Options{Addr: redisAddr}
	redisClient = redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = redisClient.Ping(ctx).Err()
	if err != nil {
		log.Fatalln("can't ping redis db:", err)
	}

	return redisClient
}
