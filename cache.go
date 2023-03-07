package gocache

import (
	"errors"
	"log"
	"sync"
	"time"
)

type Item struct {
	Value []byte
	Key   []byte
}

type IEngine interface {
	Len() int
	Set(key []byte, value []byte) error
	Get(key []byte) error
	// OnRemove(func(i *Item))
	Delete(key []byte) error
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

func setRefresh(refreshDuration time.Duration, shardData []*Shard) {
	tick := time.NewTicker(refreshDuration)
	go func() {
		for {
			<-tick.C
			for _, shard := range shardData {
				shard.refresh()
			}
		}
	}()
}

func setCleanWindow(cleanWindow time.Duration, shardData []*Shard) {
	tick := time.NewTicker(cleanWindow)
	go func() {
		for {
			<-tick.C
			for _, shard := range shardData {
				shard.iteratorExpire()
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
	setRefresh(cf.CleanWindow, shardData)
	return &Engine{
		shardData:   shardData,
		lock:        new(sync.RWMutex),
		numberShard: cf.Shard,
		onRemove:    cf.OnRemove,
		ttl:         cf.TTL,
		keyhub:      &hub{s: make([][]byte, 0)},
	}, nil
}

func (e *Engine) Len() int {
	count := 0
	for _, i := range e.shardData {
		count += i.Count()
	}
	return count
}

func (e *Engine) set(key []byte, value []byte) error {
	pos := getPosition(key, e.numberShard)
	isInsert := e.shardData[pos].Upsert(string(key), &Item{
		Value: value,
		Key:   key,
	}, e.ttl)
	if isInsert {
		e.keyhub.push(key)
	}
	return nil
}

func (e *Engine) Set(key []byte, value []byte) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.set(key, value)
}

func (e *Engine) get(key []byte) ([]byte, error) {
	pos := getPosition(key, e.numberShard)
	data, has := e.shardData[pos].Get(string(key))
	if !has {
		return nil, errors.New(E_not_found)
	}
	return data.Value, nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.get(key)
}

func (e *Engine) Pop() ([]byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	key := e.keyhub.pop()
	if key == nil {
		return nil, errors.New(E_queue_is_empty)
	}
	data, err := e.get(key)
	if err != nil {
		return nil, err
	}
	e.delete(key)
	return data, nil
}
func (e *Engine) delete(key []byte) error {
	pos := getPosition(key, e.numberShard)
	if isDel := e.keyhub.remove(key); isDel {
		log.Print(isDel)
	}
	e.shardData[pos].Delete(string(key))
	return nil
}
func (e *Engine) Delete(key []byte) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.delete(key)
}

func (e *Engine) Purge() error {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, shard := range e.shardData {
		shard.Purge()
	}
	return nil
}
