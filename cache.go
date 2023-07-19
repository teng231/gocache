package gocache

import (
	"errors"
	"log"
	"sync"
	"time"
)

type MetaData map[string]string
type Item struct {
	Value    []byte
	MetaData *MetaData
	ttlTime  int64
}

type IEngine interface {
	Len() int
	Set(key string, value []byte) error
	Keys() []string
	Get(key string) ([]byte, error)
	Pop() (string, []byte, error)
	Delete(key string) error
	Purge() error
}

type Engine struct {
	shardData   []*Shard
	lock        *sync.RWMutex
	numberShard int
	onRemove    func(key []byte, i *Item)
	ttl         time.Duration
	keyhub      *hub
}

func setCleanWindow(cleanWindow time.Duration, shardData []*Shard) {
	tick := time.NewTicker(cleanWindow)
	go func() {
		for {
			<-tick.C
			for _, shard := range shardData {
				shard.lock.RLock()
				shard.iteratorExpire()
				shard.lock.RUnlock()
			}

		}
	}()
}

func New(cf *Config) (*Engine, error) {
	if cf.CleanWindow <= 0 {
		return nil, errors.New(E_not_found_clean_window)
	}
	if cf.Shard <= 0 || cf.Shard > 256 {
		return nil, errors.New(E_shard_size_invalid)
	}
	if cf.TTL <= 0 {
		return nil, errors.New(E_invalid_ttl)
	}
	shardData := make([]*Shard, 0, cf.Shard)
	for i := 0; i < cf.Shard; i++ {
		shardData = append(shardData, initShard(cf.OnRemove))
	}
	setCleanWindow(cf.CleanWindow, shardData)

	engine := &Engine{
		shardData:   shardData,
		lock:        new(sync.RWMutex),
		numberShard: cf.Shard,
		onRemove:    cf.OnRemove,
		ttl:         cf.TTL,
		keyhub:      &hub{s: make([]*string, 0), lock: &sync.RWMutex{}},
	}
	return engine, nil
}

func (e *Engine) Len() int {
	count := 0
	for _, i := range e.shardData {
		count += i.Count()
	}
	return count
}

func (e *Engine) Set(key string, value []byte, metaData ...MetaData) error {
	pos := getPosition([]byte(key), e.numberShard)
	item := &Item{
		Value: value,
	}
	if len(metaData) > 0 {
		item.MetaData = &metaData[0]
	}
	e.lock.RLock()
	isInsert := e.shardData[pos].Upsert(string(key), item, e.ttl)
	e.lock.RUnlock()
	if isInsert {
		e.keyhub.push(key)
	}
	return nil
}

func (e *Engine) getWithMetaData(key string) (MetaData, []byte, error) {
	pos := getPosition([]byte(key), e.numberShard)
	e.lock.RLock()
	data, has := e.shardData[pos].Get(key)
	e.lock.RUnlock()
	if !has {
		return nil, nil, errors.New(E_not_found)
	}
	return *data.MetaData, data.Value, nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	pos := getPosition([]byte(key), e.numberShard)
	e.lock.RLock()
	data, has := e.shardData[pos].Get(key)
	e.lock.RUnlock()
	if !has {
		return nil, errors.New(E_not_found)
	}
	return data.Value, nil
}

func (e *Engine) Pop() (string, []byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	key := e.keyhub.pop()
	if key == "" {
		return "", nil, errors.New(E_queue_is_empty)
	}
	pos := getPosition([]byte(key), e.numberShard)
	data, has := e.shardData[pos].Get(key)
	if !has {
		return "", nil, errors.New(E_not_found)
	}
	e.shardData[pos].Delete(key)
	return key, data.Value, nil
}

func (e *Engine) Delete(key string) error {
	pos := getPosition([]byte(key), e.numberShard)
	e.keyhub.remove(key)
	e.lock.Lock()
	e.shardData[pos].Delete(key)
	e.lock.Unlock()
	return nil
}

func (e *Engine) Purge() error {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, shard := range e.shardData {
		shard.Purge()
	}
	return nil
}

func (e *Engine) Info() error {
	e.lock.Lock()
	defer e.lock.Unlock()
	log.Printf("%d %d", e.Len(), e.keyhub.len())
	return nil
}

func (e *Engine) Scan(handler func(key string, val []byte, metaData MetaData) bool) {
	sPtrs := e.keyhub.getS()
	for _, sPtr := range sPtrs {
		metaData, val, err := e.getWithMetaData(*sPtr)
		if err != nil {
			continue
		}
		isBreak := handler(*sPtr, val, metaData)
		if isBreak {
			break
		}
	}
}
