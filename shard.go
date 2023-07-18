package gocache

import (
	"hash/crc32"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func getPosition(key []byte, numberOfShards int) int {
	return int(crc32.ChecksumIEEE(key)) % numberOfShards
}

type Shard struct {
	data     map[string]*Item
	lock     *sync.RWMutex
	count    int64
	onRemove func(key []byte, i *Item)
}

func initShard(onRemove func(key []byte, i *Item)) *Shard {
	return &Shard{
		data:     make(map[string]*Item),
		lock:     new(sync.RWMutex),
		onRemove: onRemove,
	}
}

func (s *Shard) refresh() {
	newData := make(map[string]*Item)
	s.lock.Lock()
	for k, v := range s.data {
		newData[k] = v
	}
	s.data = newData
	s.lock.Unlock()
}

func (s *Shard) Count() int {
	return int(s.count)
}

func (s *Shard) Purge() {
	s.lock.Lock()
	defer func() {
		s.lock.Unlock()
	}()
	for key := range s.data {
		delete(s.data, key)
	}
	s.data = nil
}

func (s *Shard) Upsert(key string, val *Item, ttl time.Duration) bool {
	isInsert := false
	if !s.isExisted(key) {
		atomic.AddInt64(&s.count, 1)
		isInsert = true
	}
	val.ttlTime = time.Now().Unix() + int64(ttl.Seconds())
	s.lock.Lock()
	s.data[key] = val
	s.lock.Unlock()
	return isInsert
}

func (s *Shard) Delete(key string) {

	if s.isExisted(key) {
		atomic.AddInt64(&s.count, -1)
	}
	if s.onRemove != nil {
		s.onRemove([]byte(key), s.data[key])
	}
	s.lock.Lock()
	delete(s.data, key)
	s.lock.Unlock()
}

func (s *Shard) isExisted(key string) bool {
	s.lock.RLock()
	_, has := s.data[key]
	s.lock.RUnlock()
	return has
}

func (s *Shard) Get(key string) (*Item, bool) {
	s.lock.RLock()
	item, has := s.data[key]
	s.lock.RUnlock()
	if !has {
		return nil, false
	}
	if isExpired(item.ttlTime) {
		s.Delete(key)
		return nil, false
	}
	return item, has
}

func (s *Shard) iteratorExpire() {
	for key, dataItem := range s.data {
		if isExpired(dataItem.ttlTime) {
			s.Delete(key)
		}
	}
}

func (s *Shard) Info() {
	log.Printf("count %d, len(data) %d", s.count, len(s.data))
}
