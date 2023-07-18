package gocache

import (
	"errors"
	"log"
	"sync"
	"time"
)

type Item struct {
	Value   []byte
	ttlTime int64
}

type IEngine interface {
	Len() int
	Set(key string, value []byte) error
	Get(key string) ([]byte, error)
	Pop() ([]byte, error)
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

func (e *Engine) setRefresh(refreshDuration time.Duration, verbose bool) {
	tick := time.NewTicker(refreshDuration)
	go func() {
		for {
			<-tick.C
			e.lock.RLock()
			// e.Refresh(verbose)
			for _, shard := range e.shardData {
				shard.refresh()
			}
			e.lock.RUnlock()
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

	engine := &Engine{
		shardData:   shardData,
		lock:        new(sync.RWMutex),
		numberShard: cf.Shard,
		onRemove:    cf.OnRemove,
		ttl:         cf.TTL,
		keyhub:      &hub{s: make([]*string, 0)},
	}
	engine.setRefresh(cf.CleanWindow, cf.Verbose)
	return engine, nil
}

func (e *Engine) Len() int {
	count := 0
	for _, i := range e.shardData {
		count += i.Count()
	}
	return count
}

func (e *Engine) set(key string, value []byte) error {
	pos := getPosition([]byte(key), e.numberShard)
	isInsert := e.shardData[pos].Upsert(string(key), &Item{
		Value: value,
		// ttl:   time.Now().Unix() + int64(e.ttl.Seconds()),
	}, e.ttl)
	if isInsert {
		e.keyhub.push(key)
	}
	return nil
}

func (e *Engine) Set(key string, value []byte) error {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.set(key, value)
}

func (e *Engine) get(key string) ([]byte, error) {
	pos := getPosition([]byte(key), e.numberShard)
	data, has := e.shardData[pos].Get(key)
	if !has {
		return nil, errors.New(E_not_found)
	}
	return data.Value, nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.get(key)
}

func (e *Engine) Pop() ([]byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	key := e.keyhub.pop()
	if key == "" {
		return nil, errors.New(E_queue_is_empty)
	}
	pos := getPosition([]byte(key), e.numberShard)
	data, has := e.shardData[pos].Get(key)
	if !has {
		return nil, errors.New(E_not_found)
	}
	e.shardData[pos].Delete(key)
	return data.Value, nil
}

func (e *Engine) delete(key string) error {
	pos := getPosition([]byte(key), e.numberShard)
	if isDel := e.keyhub.remove(key); isDel {
		log.Print(isDel)
	}
	e.shardData[pos].Delete(key)
	return nil
}
func (e *Engine) Delete(key string) error {
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

func (e *Engine) Info() error {
	e.lock.Lock()
	defer e.lock.Unlock()
	log.Printf("%d %d", e.Len(), e.keyhub.len())
	return nil
}
