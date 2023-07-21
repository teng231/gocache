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
	// keyhub      *hub
	keylist []*string // key
}

func setCleanWindow(cleanWindow time.Duration, shardData []*Shard, deleteHook func([]string)) {
	tick := time.NewTicker(cleanWindow)
	go func() {
		for {
			<-tick.C
			for _, shard := range shardData {
				shard.lock.RLock()
				keys := shard.iteratorExpire()
				if len(keys) > 0 {
					deleteHook(keys)
				}
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

	engine := &Engine{
		shardData:   shardData,
		lock:        new(sync.RWMutex),
		numberShard: cf.Shard,
		onRemove:    cf.OnRemove,
		ttl:         cf.TTL,
		// keyhub:      &hub{s: make([]*string, 0), lock: &sync.RWMutex{}},
		keylist: make([]*string, 0),
	}
	setCleanWindow(cf.CleanWindow, shardData, func(keysDeleted []string) {
		engine.lock.Lock()
		for _, key := range keysDeleted {
			engine.keylist, _ = remove(engine.keylist, key)
		}
		defer engine.lock.Lock()
	})
	return engine, nil
}

func (e *Engine) Len() int {
	count := 0
	for _, i := range e.shardData {
		count += i.Count()
	}
	return count
}

func (e *Engine) set(key string, value []byte, metaData ...MetaData) error {
	pos := getPosition([]byte(key), e.numberShard)
	item := &Item{
		Value: value,
	}
	if len(metaData) > 0 {
		item.MetaData = &metaData[0]
	}
	isInsert := e.shardData[pos].Upsert(string(key), item, e.ttl)
	if isInsert {
		// e.keyhub.push(key)
		e.keylist = append(e.keylist, &key)
	}
	return nil
}

func (e *Engine) Set(key string, value []byte, metaData ...MetaData) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.set(key, value, metaData...)
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

func (e *Engine) getWithMetaData(key string) (MetaData, []byte, error) {
	pos := getPosition([]byte(key), e.numberShard)
	data, has := e.shardData[pos].Get(key)
	if !has {
		return nil, nil, errors.New(E_not_found)
	}
	return *data.MetaData, data.Value, nil
}

func (e *Engine) Pop() (string, []byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	var key *string
	e.keylist, key = pop(e.keylist)
	if key == nil {
		return "", nil, errors.New(E_queue_is_empty)
	}
	pos := getPosition([]byte(*key), e.numberShard)
	data, has := e.shardData[pos].GetThenDelete(*key)
	if !has {
		log.Print("not found ", *key)
		return "", nil, errors.New(E_not_found)
	}
	return *key, data.Value, nil
}

func (e *Engine) delete(key string) error {
	pos := getPosition([]byte(key), e.numberShard)
	e.keylist, _ = remove(e.keylist, key)
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
	e.keylist = make([]*string, 0)
	return nil
}

func (e *Engine) Info() error {
	e.lock.Lock()
	defer e.lock.Unlock()
	log.Printf("data count: %d listkeys count: %d", e.Len(), len(e.keylist))
	return nil
}

func (e *Engine) PopWithMetadata(key, matchVal string) (string, []byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, sPtr := range e.keylist {
		if sPtr == nil {
			log.Print("NILL")
			break
		}
		metaData, val, err := e.getWithMetaData(*sPtr)
		if err != nil {
			return *sPtr, val, err
		}

		if metaData[key] == matchVal {
			e.delete(*sPtr)
			return *sPtr, val, err
		}
	}
	log.Println("NOT FOUND ", len(e.keylist))
	return "", nil, errors.New(E_not_found)
}
