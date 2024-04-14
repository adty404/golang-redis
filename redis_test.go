package belajar_golang_redis

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)

	// err := client.Close()
	// assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Aditya Prasetyo", 3*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Aditya Prasetyo", result)

	time.Sleep(4 * time.Second)

	_, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "names", "Aditya")
	client.RPush(ctx, "names", "Prasetyo")

	assert.Equal(t, "Aditya", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Prasetyo", client.LPop(ctx, "names").Val())

	client.Del(ctx, "names")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "students", "Aditya")
	client.SAdd(ctx, "students", "Aditya")
	client.SAdd(ctx, "students", "Prasetyo")
	client.SAdd(ctx, "students", "Prasetyo")
	client.SAdd(ctx, "students", "Prasetyo")

	assert.Equal(t, int64(2), client.SCard(ctx, "students").Val())
	assert.Equal(t, []string{"Aditya", "Prasetyo"}, client.SMembers(ctx, "students").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score: 90, Member: "Aditya"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 80, Member: "Prasetyo"})

	assert.Equal(t, []string{"Prasetyo", "Aditya"}, client.ZRange(ctx, "scores", 0, -1).Val())
	assert.Equal(t, "Aditya", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Prasetyo", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", 1)
	client.HSet(ctx, "user:1", "name", "Aditya")
	client.HSet(ctx, "user:1", "age", 24)
	client.HSet(ctx, "user:1", "job", "Programmer")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, map[string]string{
		"id":   "1",
		"name": "Aditya",
		"age":  "24",
		"job":  "Programmer",
	}, user)

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "location", &redis.GeoLocation{
		Name:      "Aditya",
		Longitude: 107.619125,
		Latitude:  -6.917464,
	})

	client.GeoAdd(ctx, "location", &redis.GeoLocation{
		Name:      "Prasetyo",
		Longitude: 107.619125,
		Latitude:  -6.917464,
	})

	client.GeoDist(ctx, "location", "Aditya", "Prasetyo", "m").Val()
	assert.Equal(t, float64(0), client.GeoDist(ctx, "location", "Aditya", "Prasetyo", "m").Val())

	sellers := client.GeoSearch(ctx, "location", &redis.GeoSearchQuery{
		Longitude: 107.619125,
		Latitude:  -6.917464,
		Radius:    100,
	}).Val()

	assert.Equal(t, 2, len(sellers))
	assert.Equal(t, []string{"Aditya", "Prasetyo"}, sellers)

	result := client.GeoRadius(ctx, "location", 107.619125, -6.917464, &redis.GeoRadiusQuery{
		Radius:      100,
		Unit:        "m",
		WithCoord:   true,
		WithDist:    true,
		WithGeoHash: true,
		Count:       1,
		Sort:        "ASC",
	}).Val()

	assert.Len(t, result, 1)
	assert.Equal(t, "Aditya", result[0].Name)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "Aditya")
	client.PFAdd(ctx, "visitors", "Prasetyo")
	count := client.PFCount(ctx, "visitors").Val()
	assert.Equal(t, int64(2), count)

	assert.Equal(t, int64(2), client.PFCount(ctx, "visitors").Val())

	client.PFAdd(ctx, "visitors", "Aditya", "Prasetyo", "Aditya", "Prasetyo")
	count = client.PFCount(ctx, "visitors").Val()
	assert.Equal(t, int64(2), count)

	assert.Equal(t, int64(2), client.PFCount(ctx, "visitors").Val())

	client.Del(ctx, "visitors")
}

func TestPipeline(t *testing.T) {
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "name", "Aditya Prasetyo", 0)
		pipe.Set(ctx, "age", 24, 0)
		pipe.Set(ctx, "address", "Indonesia", 0)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, "Aditya Prasetyo", client.Get(ctx, "name").Val())
	assert.Equal(t, "24", client.Get(ctx, "age").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "name", "Aditya", 0)
		pipe.Set(ctx, "age", 24, 0)
		pipe.Set(ctx, "address", "Indonesia", 0)
		return nil
	})
	assert.Nil(t, err)

	assert.Equal(t, "Aditya", client.Get(ctx, "name").Val())
	assert.Equal(t, "24", client.Get(ctx, "age").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "Aditya",
				"age":     24,
				"address": "Indonesia",
			},
		}).Err()

		assert.Nil(t, err)
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	err := client.XGroupCreate(ctx, "members", "group-1", "0").Err()
	assert.Nil(t, err)

	err = client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1").Err()
	assert.Nil(t, err)
	err = client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2").Err()
	assert.Nil(t, err)
}

func TestConsumeStream(t *testing.T) {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    10,
		Block:    time.Second * 5,
	}).Val()

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	subscriber := client.Subscribe(ctx, "channel-1")
	defer subscriber.Close()
	for i := 0; i < 10; i++ {
		message, err := subscriber.ReceiveMessage(ctx)
		assert.Nil(t, err)
		fmt.Println(message.Payload)
	}
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.Publish(ctx, "channel-1", "Hello "+strconv.Itoa(i)).Err()
		assert.Nil(t, err)
	}
}
