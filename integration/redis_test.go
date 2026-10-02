package integration

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/udaykishore-resu/idem"
	"github.com/udaykishore-resu/idem/redisstore"
	"github.com/udaykishore-resu/idem/storetest"
)

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("IDEM_REDIS_ADDR")
	if addr == "" {
		t.Skip("IDEM_REDIS_ADDR is not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("cannot reach Redis at %s: %v", addr, err)
	}
	return rdb
}

// goRedis is the adapter shown in the redisstore documentation.
func goRedis(rdb *redis.Client) redisstore.Evaler {
	return redisstore.EvalFunc(func(ctx context.Context, script string, keys []string, args ...any) (any, error) {
		return rdb.Eval(ctx, script, keys, args...).Result()
	})
}

func TestRedisStoreWithGoRedis(t *testing.T) {
	rdb := redisClient(t)
	storetest.Run(t, func(*testing.T) idem.Store {
		return redisstore.New(goRedis(rdb), redisstore.WithPrefix("idem-integration:"))
	})
}

func TestRedisEndToEnd(t *testing.T) {
	ctx := context.Background()
	engine := idem.New(redisstore.New(goRedis(redisClient(t)), redisstore.WithPrefix("idem-integration:")))
	key := uniqueKey(t)
	calls := 0
	fn := func(context.Context) (map[string]int, error) {
		calls++
		return map[string]int{"amount": 500}, nil
	}

	if _, replayed, err := idem.Do(ctx, engine, key, fn); err != nil || replayed {
		t.Fatalf("first call: replayed = %v, err = %v", replayed, err)
	}
	v, replayed, err := idem.Do(ctx, engine, key, fn)
	if err != nil || !replayed || v["amount"] != 500 {
		t.Fatalf("second call: v = %v, replayed = %v, err = %v", v, replayed, err)
	}
	if calls != 1 {
		t.Errorf("fn ran %d times, want 1", calls)
	}
}
