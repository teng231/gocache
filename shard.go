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
	keyTTL   map[string]int64 // created + ttl
	lock     *sync.RWMutex
	lockTTL  *sync.RWMutex
	count    int64
	onRemove func(key []byte, i *Item)
}

func initShard(onRemove func(key []byte, i *Item)) *Shard {

	return &Shard{
		data:     make(map[string]*Item),
		lock:     new(sync.RWMutex),
		lockTTL:  new(sync.RWMutex),
		keyTTL:   make(map[string]int64),
		onRemove: onRemove,
	}
}

func (s *Shard) refresh() {
	newData := make(map[string]*Item)
	newKeyTTL := make(map[string]int64)
	s.lock.Lock()
	for k, v := range s.data {
		newData[k] = v
	}
	s.data = newData
	s.lock.Unlock()
	s.lockTTL.Lock()
	for k, v := range s.keyTTL {
		newKeyTTL[k] = v
	}
	s.keyTTL = newKeyTTL
	s.lockTTL.Unlock()
}

func (s *Shard) Count() int {
	return int(s.count)
}

func (s *Shard) Purge() {
	s.lock.Lock()
	s.lockTTL.Lock()
	defer func() {
		s.lock.Unlock()
		s.lockTTL.Unlock()
	}()
	for key := range s.data {
		delete(s.data, key)
	}
	for key := range s.keyTTL {
		delete(s.keyTTL, key)
	}
	s.data = nil
	s.keyTTL = nil
}

func (s *Shard) Upsert(key string, val *Item, ttl time.Duration) bool {
	isInsert := false
	if !s.isExisted(key) {
		atomic.AddInt64(&s.count, 1)
		isInsert = true
	}
	s.lockTTL.Lock()
	s.keyTTL[key] = time.Now().Unix() + int64(ttl.Seconds())
	s.lockTTL.Unlock()

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

	s.lockTTL.Lock()
	delete(s.keyTTL, key)
	s.lockTTL.Unlock()
}

func (s *Shard) isExisted(key string) bool {
	s.lock.RLock()
	_, has := s.data[key]
	s.lock.RUnlock()
	return has
}

func (s *Shard) Get(key string) (*Item, bool) {
	s.lockTTL.RLock()
	isEx := isExpired(s.keyTTL[key])
	s.lockTTL.RUnlock()
	if isEx {
		s.Delete(key)
		return nil, false
	}
	s.lock.RLock()
	item, has := s.data[key]
	s.lock.RUnlock()
	return item, has
}

func (s *Shard) Iterator(f func(key string, val *Item)) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for key, item := range s.data {
		f(key, item)
	}
}

func (s *Shard) iteratorExpire() {
	for key, expireTime := range s.keyTTL {
		if isExpired(expireTime) {
			s.Delete(key)
		}
	}
}

func (s *Shard) Info() {
	log.Printf("count %d, len(data) %d, len(keyTTL) %d", s.count, len(s.data), len(s.keyTTL))
}
