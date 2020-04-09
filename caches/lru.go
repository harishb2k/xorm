// Copyright 2015 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package caches

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// LRUCacher implments cache object facilities
type LRUCacher struct {
	idList         *list.List
	sqlList        *list.List
	idIndex        map[string]map[string]*list.Element
	sqlIndex       map[string]map[string]*list.Element
	store          CacheStore
	mutex          sync.Mutex
	MaxElementSize int
	Expired        time.Duration
	GcInterval     time.Duration
}

// NewLRUCacher creates a cacher
func NewLRUCacher(store CacheStore, maxElementSize int) *LRUCacher {
	return NewLRUCacher2(store, 3600*time.Second, maxElementSize)
}

// NewLRUCacher2 creates a cache include different params
func NewLRUCacher2(store CacheStore, expired time.Duration, maxElementSize int) *LRUCacher {
	cacher := &LRUCacher{store: store, idList: list.New(),
		sqlList: list.New(), Expired: expired,
		GcInterval: CacheGcInterval, MaxElementSize: maxElementSize,
		sqlIndex: make(map[string]map[string]*list.Element),
		idIndex:  make(map[string]map[string]*list.Element),
	}
	cacher.RunGC()
	return cacher
}

// RunGC run once every m.GcInterval
func (m *LRUCacher) RunGC() {
	time.AfterFunc(m.GcInterval, func() {
		m.RunGC()
		m.GC()
	})
}

// GC check ids lit and sql list to remove all element expired
func (m *LRUCacher) GC() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	var removedNum int
	for e := m.idList.Front(); e != nil; {
		if removedNum <= CacheGcMaxRemoved &&
			time.Now().Sub(e.Value.(*idNode).lastVisit) > m.Expired {
			removedNum++
			next := e.Next()
			node := e.Value.(*idNode)
			m.delBean(node.tbName, node.id)
			e = next
		} else {
			break
		}
	}

	removedNum = 0
	for e := m.sqlList.Front(); e != nil; {
		if removedNum <= CacheGcMaxRemoved &&
			time.Now().Sub(e.Value.(*sqlNode).lastVisit) > m.Expired {
			removedNum++
			next := e.Next()
			node := e.Value.(*sqlNode)
			m.delIDs(node.tbName, node.sql)
			e = next
		} else {
			break
		}
	}
}

// GetIds returns all bean's ids according to sql and parameter from cache
func (m *LRUCacher) GetIDs(tableName, sql string) interface{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, ok := m.sqlIndex[tableName]; !ok {
		m.sqlIndex[tableName] = make(map[string]*list.Element)
	}
	if v, err := m.store.Get(sql); err == nil {
		if el, ok := m.sqlIndex[tableName][sql]; !ok {
			el = m.sqlList.PushBack(newSQLNode(tableName, sql))
			m.sqlIndex[tableName][sql] = el
		} else {
			lastTime := el.Value.(*sqlNode).lastVisit
			// if expired, remove the node and return nil
			if time.Now().Sub(lastTime) > m.Expired {
				m.delIDs(tableName, sql)
				return nil
			}
			m.sqlList.MoveToBack(el)
			el.Value.(*sqlNode).lastVisit = time.Now()
		}
		return v
	}

	m.delIDs(tableName, sql)
	return nil
}

// GetBean returns bean according tableName and id from cache
func (m *LRUCacher) GetBean(tableName string, id string) interface{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if _, ok := m.idIndex[tableName]; !ok {
		m.idIndex[tableName] = make(map[string]*list.Element)
	}
	tid := genID(tableName, id)
	if v, err := m.store.Get(tid); err == nil {
		if el, ok := m.idIndex[tableName][id]; ok {
			lastTime := el.Value.(*idNode).lastVisit
			// if expired, remove the node and return nil
			if time.Now().Sub(lastTime) > m.Expired {
				m.delBean(tableName, id)
				return nil
			}
			m.idList.MoveToBack(el)
			el.Value.(*idNode).lastVisit = time.Now()
		} else {
			el = m.idList.PushBack(newIDNode(tableName, id))
			m.idIndex[tableName][id] = el
		}
		return v
	}

	// store bean is not exist, then remove memory's index
	m.delBean(tableName, id)
	return nil
}

// clearIDs clears all sql-ids mapping on table tableName from cache
func (m *LRUCacher) clearIDs(tableName string) {
	if tis, ok := m.sqlIndex[tableName]; ok {
		for sql, v := range tis {
			m.sqlList.Remove(v)
			m.store.Del(sql)
		}
	}
	m.sqlIndex[tableName] = make(map[string]*list.Element)
}

// ClearIDs clears all sql-ids mapping on table tableName from cache
func (m *LRUCacher) ClearIDs(tableName string) {
	m.mutex.Lock()
	m.clearIDs(tableName)
	m.mutex.Unlock()
}

func (m *LRUCacher) clearBeans(tableName string) {
	if tis, ok := m.idIndex[tableName]; ok {
		for id, v := range tis {
			m.idList.Remove(v)
			tid := genID(tableName, id)
			m.store.Del(tid)
		}
	}
	m.idIndex[tableName] = make(map[string]*list.Element)
}

// ClearBeans clears all beans in some table
func (m *LRUCacher) ClearBeans(tableName string) {
	m.mutex.Lock()
	m.clearBeans(tableName)
	m.mutex.Unlock()
}

// PutIDs pus ids into table
func (m *LRUCacher) PutIDs(tableName, sql string, ids interface{}) {
	m.mutex.Lock()
	if _, ok := m.sqlIndex[tableName]; !ok {
		m.sqlIndex[tableName] = make(map[string]*list.Element)
	}
	if el, ok := m.sqlIndex[tableName][sql]; !ok {
		el = m.sqlList.PushBack(newSQLNode(tableName, sql))
		m.sqlIndex[tableName][sql] = el
	} else {
		el.Value.(*sqlNode).lastVisit = time.Now()
	}
	m.store.Put(sql, ids)
	if m.sqlList.Len() > m.MaxElementSize {
		e := m.sqlList.Front()
		node := e.Value.(*sqlNode)
		m.delIDs(node.tbName, node.sql)
	}
	m.mutex.Unlock()
}

// PutBean puts beans into table
func (m *LRUCacher) PutBean(tableName string, id string, obj interface{}) {
	m.mutex.Lock()
	var el *list.Element
	var ok bool

	if el, ok = m.idIndex[tableName][id]; !ok {
		el = m.idList.PushBack(newIDNode(tableName, id))
		m.idIndex[tableName][id] = el
	} else {
		el.Value.(*idNode).lastVisit = time.Now()
	}

	m.store.Put(genID(tableName, id), obj)
	if m.idList.Len() > m.MaxElementSize {
		e := m.idList.Front()
		node := e.Value.(*idNode)
		m.delBean(node.tbName, node.id)
	}
	m.mutex.Unlock()
}

func (m *LRUCacher) delIDs(tableName, sql string) {
	if _, ok := m.sqlIndex[tableName]; ok {
		if el, ok := m.sqlIndex[tableName][sql]; ok {
			delete(m.sqlIndex[tableName], sql)
			m.sqlList.Remove(el)
		}
	}
	m.store.Del(sql)
}

// DelIDs deletes ids
func (m *LRUCacher) DelIDs(tableName, sql string) {
	m.mutex.Lock()
	m.delIDs(tableName, sql)
	m.mutex.Unlock()
}

func (m *LRUCacher) delBean(tableName string, id string) {
	tid := genID(tableName, id)
	if el, ok := m.idIndex[tableName][id]; ok {
		delete(m.idIndex[tableName], id)
		m.idList.Remove(el)
		m.clearIDs(tableName)
	}
	m.store.Del(tid)
}

// DelBean deletes beans in some table
func (m *LRUCacher) DelBean(tableName string, id string) {
	m.mutex.Lock()
	m.delBean(tableName, id)
	m.mutex.Unlock()
}

type idNode struct {
	tbName    string
	id        string
	lastVisit time.Time
}

type sqlNode struct {
	tbName    string
	sql       string
	lastVisit time.Time
}

func genSQLKey(sql string, args interface{}) string {
	return fmt.Sprintf("%s-%v", sql, args)
}

func genID(prefix string, id string) string {
	return fmt.Sprintf("%s-%s", prefix, id)
}

func newIDNode(tbName string, id string) *idNode {
	return &idNode{tbName, id, time.Now()}
}

func newSQLNode(tbName, sql string) *sqlNode {
	return &sqlNode{tbName, sql, time.Now()}
}
