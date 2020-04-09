// Copyright 2019 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package caches

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"time"

	"xorm.io/xorm/schemas"
)

const (
	// CacheExpired is default cache expired time
	CacheExpired = 60 * time.Minute
	// CacheMaxMemory is not use now
	CacheMaxMemory = 256
	// CacheGcInterval represents interval time to clear all expired nodes
	CacheGcInterval = 10 * time.Minute
	// CacheGcMaxRemoved represents max nodes removed when gc
	CacheGcMaxRemoved = 20
)

// list all the errors
var (
	ErrCacheMiss = errors.New("xorm/cache: key not found")
	ErrNotStored = errors.New("xorm/cache: not stored")
	// ErrNotExist record does not exist error
	ErrNotExist = errors.New("Record does not exist")
)

// CacheStore is a interface to store cache
type CacheStore interface {
	// key is primary key or composite primary key
	// value is struct's pointer
	// key format : <tablename>-p-<pk1>-<pk2>...
	Put(key string, value interface{}) error
	Get(key string) (interface{}, error)
	Del(key string) error
}

// Cacher is an interface to provide cache
// id format : u-<pk1>-<pk2>...
type Cacher interface {
	GetIDs(tableName, sql string) interface{}
	GetBean(tableName string, id string) interface{}
	PutIDs(tableName, sql string, ids interface{})
	PutBean(tableName string, id string, obj interface{})
	DelIDs(tableName, sql string)
	DelBean(tableName string, id string)
	ClearIDs(tableName string)
	ClearBeans(tableName string)
}

func encodeIDs(ids []schemas.PK) (string, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(ids)

	return buf.String(), err
}

func decodeIDs(s string) ([]schemas.PK, error) {
	pks := make([]schemas.PK, 0)

	dec := gob.NewDecoder(strings.NewReader(s))
	err := dec.Decode(&pks)

	return pks, err
}

// GetCacheSQL returns cacher PKs via SQL
func GetCacheSQL(m Cacher, tableName, sql string, args interface{}) ([]schemas.PK, error) {
	bytes := m.GetIDs(tableName, GenSQLKey(sql, args))
	if bytes == nil {
		return nil, errors.New("Not Exist")
	}
	return decodeIDs(bytes.(string))
}

// PutCacheSQL puts cacher SQL and PKs
func PutCacheSQL(m Cacher, ids []schemas.PK, tableName, sql string, args interface{}) error {
	bytes, err := encodeIDs(ids)
	if err != nil {
		return err
	}
	m.PutIDs(tableName, GenSQLKey(sql, args), bytes)
	return nil
}

// GenSQLKey generates cache key
func GenSQLKey(sql string, args interface{}) string {
	return fmt.Sprintf("%v-%v", sql, args)
}
