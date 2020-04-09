package caches

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"reflect"
	"time"
	"unsafe"

	"github.com/go-redis/redis/v7"

	"xorm.io/xorm/log"
)

const (
	// DefaultRedisExpiration defaults expiration, default will not expiration
	DefaultRedisExpiration = time.Duration(0)
	foreverExpiration      = time.Duration(-1)
	loggerPrefix           = "[redis_cacher]"
)

var _ Cacher = &RedisCacher{}

// RedisCacher wraps the Redis client to meet the Cache interface.
type RedisCacher struct {
	client              *redis.Client
	defaultExpiration time.Duration
	logger log.Logger
}

// NewRedisCacher creates a Redis Cacher, host as IP endpoint, i.e., localhost:6379, provide empty string or nil if Redis server doesn't
// require AUTH command, defaultExpiration sets the expire duration for a key to live. Until redigo supports
// sharding/clustering, only one host will be in hostList
//
//     engine.SetDefaultCacher(caches.NewRedisCacher("localhost:6379", "", caches.DefaultRedisExpiration, engine.Logger()))
//
// or set MapCacher
//
//     engine.MapCacher(&user, caches.NewRedisCacher("localhost:6379", "", caches.DefaultRedisExpiration, engine.Logger()))
//
func NewRedisCacher(host string, password string, dbIdx int, defaultExpiration time.Duration, logger log.Logger) *RedisCacher {
	client := redis.NewClient(&redis.Options{
		Addr:     host,
		Password: password, // no password set
		DB:       dbIdx,  // use default DB
	})
	
	return &RedisCacher{
		client: client,
		defaultExpiration: defaultExpiration, 
		logger:logger,
	}
}

func (c *RedisCacher) logErrf(format string, contents ...interface{}) {
	if c.logger != nil {
		c.logger.Errorf(fmt.Sprintf("%s %s", loggerPrefix, format), contents...)
	}
}

func (c *RedisCacher) logDebugf(format string, contents ...interface{}) {
	if c.logger != nil {
		c.logger.Debugf(fmt.Sprintf("%s %s", loggerPrefix, format), contents...)
	}
}

func (c *RedisCacher) getBeanKey(tableName string, id string) string {
	return fmt.Sprintf("xorm:bean:%s:%s", tableName, id)
}

func (c *RedisCacher) getSQLKey(tableName string, sql string) string {
	// hash sql to minimize key length
	crc := crc32.ChecksumIEEE([]byte(sql))
	return fmt.Sprintf("xorm:sql:%s:%d", tableName, crc)
}

// Flush deletes all xorm cached objects
func (c *RedisCacher) Flush() error {
	// conn := c.pool.Get()
	// defer conn.Close()
	// _, err := conn.Do("FLUSHALL")
	// return err
	return c.delObject("xorm:*")
}

func (c *RedisCacher) getObject(key string) interface{} {
	bs, err := c.client.Get(key).Bytes()
	if err != nil {
		c.logErrf("redis.Bytes failed: %v", err)
		return nil
	}

	value, err := c.deserialize(bs)
	if err != nil {
		c.logErrf("deserialize: %v", err)
		return nil
	}

	return value
}

// GetIDs implemented Cacher
func (c *RedisCacher) GetIDs(tableName, sql string) interface{} {
	sqlKey := c.getSQLKey(tableName, sql)
	c.logDebugf(" GetIds|tableName:%s|sql:%s|key:%s", tableName, sql, sqlKey)
	return c.getObject(sqlKey)
}

// GetBean implemented Cacher
func (c *RedisCacher) GetBean(tableName string, id string) interface{} {
	beanKey := c.getBeanKey(tableName, id)
	c.logDebugf("[xorm/redis_cacher] GetBean|tableName:%s|id:%s|key:%s", tableName, id, beanKey)
	return c.getObject(beanKey)
}

func (c *RedisCacher) putObject(key string, value interface{}) {
	c.set(key, value, c.defaultExpiration)
}

// PutIDs implemented Cacher
func (c *RedisCacher) PutIDs(tableName, sql string, ids interface{}) {
	sqlKey := c.getSQLKey(tableName, sql)
	c.logDebugf("PutIds|tableName:%s|sql:%s|key:%s|obj:%s|type:%v", tableName, sql, sqlKey, ids, reflect.TypeOf(ids))
	c.putObject(sqlKey, ids)
}

// PutBean implemented Cacher
func (c *RedisCacher) PutBean(tableName string, id string, obj interface{}) {
	beanKey := c.getBeanKey(tableName, id)
	c.logDebugf("PutBean|tableName:%s|id:%s|key:%s|type:%v", tableName, id, beanKey, reflect.TypeOf(obj))
	c.putObject(beanKey, obj)
}

func (c *RedisCacher) delObject(key string) error {
	c.logDebugf("delObject key:[%s]", key)

	r, err := c.client.Do("EXISTS", key).Result()
	if err != nil {
		return err
	}
	if exist, ok := r.(bool); ok && !exist {
		c.logErrf("delObject key:[%s] err: %v", key, ErrCacheMiss)
		return ErrCacheMiss
	}

	_, err = c.client.Do("DEL", key).Result()
	return err
}

func (c *RedisCacher) delObjects(key string) error {
	c.logDebugf("delObjects key:[%s]", key)

	keys, err := c.client.Do("KEYS", key).Result()
	c.logDebugf("delObjects keys: %v", keys)
	if err != nil {
		return err
	}

	for _, key := range keys.([]interface{}) {
		_, err = c.client.Do("DEL", key).Result()
		if err != nil {
			c.logErrf("delObje")
		}
	}

	return nil
}

// DelIDs implemented Cacher
func (c *RedisCacher) DelIDs(tableName, sql string) {
	c.delObject(c.getSQLKey(tableName, sql))
}

// DelBean implemented Cacher
func (c *RedisCacher) DelBean(tableName string, id string) {
	c.delObject(c.getBeanKey(tableName, id))
}

// ClearIDs implemented Cacher
func (c *RedisCacher) ClearIDs(tableName string) {
	c.delObjects(fmt.Sprintf("xorm:sql:%s:*", tableName))
}

// ClearBeans implemented Cacher
func (c *RedisCacher) ClearBeans(tableName string) {
	c.delObjects(c.getBeanKey(tableName, "*"))
}

func (c *RedisCacher) set(key string, value interface{}, expires time.Duration) error {
	switch expires {
	case DefaultRedisExpiration:
		expires = c.defaultExpiration
	case foreverExpiration:
		expires = time.Duration(0)
	}

	b, err := c.serialize(value)
	if err != nil {
		return err
	}

	if expires > 0 {
		_, err = c.client.Do("SETEX", key, int32(expires/time.Second), b).Result()
		return err
	}
	_, err = c.client.Do("SET", key, b).Result()
	return err
}

func (c *RedisCacher) serialize(value interface{}) ([]byte, error) {
	err := c.registerGobConcreteType(value)
	if err != nil {
		return nil, err
	}

	if reflect.TypeOf(value).Kind() == reflect.Struct {
		return nil, fmt.Errorf("serialize func only take pointer of a struct")
	}

	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)

	c.logDebugf("serialize type:%v", reflect.TypeOf(value))
	err = encoder.Encode(&value)
	if err != nil {
		c.logErrf("gob encoding '%s' failed: %s|value:%v", value, err, value)
		return nil, err
	}
	return b.Bytes(), nil
}

func (c *RedisCacher) deserialize(byt []byte) (ptr interface{}, err error) {
	b := bytes.NewBuffer(byt)
	decoder := gob.NewDecoder(b)

	var p interface{}
	err = decoder.Decode(&p)
	if err != nil {
		c.logErrf("decode failed: %v", err)
		return
	}

	v := reflect.ValueOf(p)
	c.logDebugf("deserialize type:%v", v.Type())
	if v.Kind() == reflect.Struct {

		var pp interface{} = &p
		datas := reflect.ValueOf(pp).Elem().InterfaceData()

		sp := reflect.NewAt(v.Type(),
			unsafe.Pointer(datas[1])).Interface()
		ptr = sp
		vv := reflect.ValueOf(ptr)
		c.logDebugf("deserialize convert ptr type:%v | CanAddr:%t", vv.Type(), vv.CanAddr())
	} else {
		ptr = p
	}
	return
}

func (c *RedisCacher) registerGobConcreteType(value interface{}) error {
	t := reflect.TypeOf(value)

	c.logDebugf("registerGobConcreteType:%v", t)

	switch t.Kind() {
	case reflect.Ptr:
		v := reflect.ValueOf(value)
		i := v.Elem().Interface()
		gob.Register(&i)
	case reflect.Struct, reflect.Map, reflect.Slice:
		gob.Register(value)
	case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Bool, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		// do nothing since already registered known type
	default:
		return fmt.Errorf("unhandled type: %v", t)
	}
	return nil
}
