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

type Engine struct {
	dataItems   map[string]*Item
	lock        *sync.RWMutex
	onRemove    func(key string, i *Item)
	ttl         time.Duration
	keyPtrs     []*string
	metaDataMap map[string][]*string
}

func (e *Engine) CleanWindow(deleteHook func(key string, i *Item)) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for key, item := range e.dataItems {
		if !isExpired(item.ttlTime) {
			continue
		}
		e.delete(key)
		if deleteHook != nil {
			deleteHook(key, item)
		}
	}
	// log.Print("clear done")
}

func (e *Engine) Keys() []string {
	e.lock.RLock()
	defer e.lock.RUnlock()

	out := make([]string, 0)

	for _, key := range e.keyPtrs {
		out = append(out, *key)
	}
	return out
}

func (e *Engine) DataItems() map[string]int64 {
	e.lock.RLock()
	defer e.lock.RUnlock()

	out := make(map[string]int64)

	for key, d := range e.dataItems {
		out[key] = d.ttlTime
	}
	return out
}

func (e *Engine) setCleanWindow(cleanWindow time.Duration, deleteHook func(key string, i *Item)) {
	tick := time.NewTicker(cleanWindow)
	go func() {
		for {
			<-tick.C
			e.CleanWindow(deleteHook)
			e.Info()
		}
	}()
}

func New(cf *Config) (*Engine, error) {
	if cf.CleanWindow <= 0 {
		return nil, errors.New(E_not_found_clean_window)
	}
	if cf.TTL <= 0 {
		return nil, errors.New(E_invalid_ttl)
	}

	engine := &Engine{
		dataItems:   make(map[string]*Item),
		keyPtrs:     make([]*string, 0, 100),
		lock:        new(sync.RWMutex),
		onRemove:    cf.OnRemove,
		ttl:         cf.TTL,
		metaDataMap: make(map[string][]*string),
	}
	engine.setCleanWindow(cf.CleanWindow, engine.onRemove)
	return engine, nil
}

func (e *Engine) Len() int {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return len(e.dataItems)
}

func (e *Engine) set(key string, value []byte, metaData ...MetaData) error {
	item := &Item{
		Value:   value,
		ttlTime: time.Now().Unix() + int64(e.ttl.Seconds()),
	}
	if len(metaData) > 0 {
		item.MetaData = &metaData[0]
		for mkey, mval := range metaData[0] {
			keyBuilder := mkey + ":" + mval
			e.metaDataMap[keyBuilder] = append(e.metaDataMap[keyBuilder], &key)
		}
	}
	e.dataItems[key] = item
	// issue duplicate key
	e.keyPtrs, _ = removeV2(e.keyPtrs, key)
	e.keyPtrs = append(e.keyPtrs, &key)
	return nil
}

func (e *Engine) Set(key string, value []byte, metaData ...MetaData) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	return e.set(key, value, metaData...)
}

// getForDelete fix issue circle get/delete
// only get for delete do not action anything
func (e *Engine) getForDelete(key string) (MetaData, []byte, error) {
	data, has := e.dataItems[key]
	if !has {
		return nil, nil, errors.New(E_not_found)
	}
	if data.MetaData == nil {
		return nil, data.Value, nil
	}
	return *data.MetaData, data.Value, nil
}

func (e *Engine) get(key string) (MetaData, []byte, error) {
	data, has := e.dataItems[key]
	if !has {
		return nil, nil, errors.New(E_not_found)
	}
	if isExpired(data.ttlTime) {
		log.Print("EXPIRED")
		e.delete(key)
		return nil, nil, errors.New(E_not_found_expired)
	}
	if data.MetaData == nil {
		return nil, data.Value, nil
	}
	return *data.MetaData, data.Value, nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	_, data, err := e.get(key)
	return data, err
}

func (e *Engine) delete(key string) error {
	meta, _, err := e.getForDelete(key)
	if err != nil {
		return nil
	}
	if len(meta) > 0 {
		for mkey, mval := range meta {
			keyBuilder := mkey + ":" + mval
			removeV2(e.metaDataMap[keyBuilder], key)
		}
	}
	delete(e.dataItems, key)
	e.keyPtrs, _ = removeV2(e.keyPtrs, key)
	if e.onRemove != nil {
		e.onRemove(key, nil)
	}
	return nil
}

func (e *Engine) deleteData(key string) error {
	meta, _, err := e.getForDelete(key)
	if err != nil {
		return nil
	}
	if len(meta) > 0 {
		for mkey, mval := range meta {
			keyBuilder := mkey + ":" + mval
			removeV2(e.metaDataMap[keyBuilder], key)
		}
	}
	delete(e.dataItems, key)
	if e.onRemove != nil {
		e.onRemove(key, nil)
	}
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
	e.keyPtrs = make([]*string, 0)
	e.dataItems = make(map[string]*Item)
	e.metaDataMap = make(map[string][]*string)
	return nil
}

func (e *Engine) Info() map[string]any {
	return map[string]any{
		"len":         e.Len(),
		"keyPtrsLen":  len(e.keyPtrs),
		"metadataLen": len(e.metaDataMap),
	}
}

// Pop so fast : because not scan, just pop key and get then delete after
// pop not scare big map
// pop time about < 10 micro secs per request
func (e *Engine) Pop() (string, []byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	var key *string
	e.keyPtrs, key = popV2(e.keyPtrs, 0)
	if key == nil {
		return "", nil, errors.New(E_queue_is_empty)
	}
	_, data, err := e.get(*key)
	if err != nil && err.Error() == E_not_found_expired {
		log.Print("not found expire ", *key)
		return "", nil, err
	}
	if err != nil && err.Error() == E_not_found {
		log.Print("not found ", *key)
		return "", nil, err
	}

	e.deleteData(*key)
	return *key, data, nil
}

// PopWithMetadata so slow : because scan all key
// if number of keys so big > 50_000 can be slow
// synchorization function, pop is atomic item
// func (e *Engine) PopWithMetadata(filter func(metadata MetaData) bool) (string, []byte, error) {
// 	if filter == nil {
// 		return "", nil, errors.New("invalid input")
// 	}
// 	e.lock.Lock()
// 	defer e.lock.Unlock()
// 	for _, sPtr := range e.keyPtrs {
// 		if sPtr == nil {
// 			log.Print("NILL something wrong")
// 			continue
// 		}
// 		metaData, val, err := e.get(*sPtr)
// 		if err != nil && err.Error() == E_not_found_expired {
// 			continue
// 		}
// 		if err != nil && err.Error() == E_not_found {
// 			continue
// 		}
// 		if matched := filter(metaData); matched {
// 			// e.keyPtrs, _ = removeV2(e.keyPtrs, *sPtr)
// 			e.delete(*sPtr)
// 			return *sPtr, val, err
// 		}
// 	}
// 	return "", nil, errors.New(E_not_found)
// }

// PopWithMetadataV2 only search metaMap
// keyMaps like `<key>:<value>` ~ "worker:3" or "name:tom"
// condition is AND beween all keymap
func (e *Engine) PopWithMetadataV2(keyMaps ...string) (string, []byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()
	countMap := make(map[string]int)
	for _, keymap := range keyMaps {
		if len(e.metaDataMap[keymap]) == 0 {
			return "", nil, errors.New(E_not_found_keymap)
		}
		for _, sPtr := range e.metaDataMap[keymap] {
			if sPtr == nil {
				continue
			}
			countMap[*sPtr]++
		}
	}

	// Lặp qua map đếm để tìm giá trị chung đầu tiên
	for key, value := range countMap {
		if value == len(keyMaps) {
			_, val, err := e.get(key)
			// if err != nil {
			// 	return "", nil, errors.New(E_not_found)
			// }
			if err != nil && err.Error() == E_not_found_expired {
				continue
			}
			if err != nil && err.Error() == E_not_found {
				continue
			}
			e.delete(key)
			return key, val, nil
		}
	}
	return "", nil, errors.New(E_queue_is_empty)
}

func (e *Engine) LenWithMetadata(keyMaps ...string) int {
	e.lock.RLock()
	defer e.lock.RUnlock()
	countMap := make(map[string]int)
	for _, keymap := range keyMaps {
		if len(e.metaDataMap[keymap]) == 0 {
			return 0
		}
		for _, sPtr := range e.metaDataMap[keymap] {
			if sPtr == nil {
				continue
			}
			countMap[*sPtr]++
		}
	}
	total := 0
	// Lặp qua map đếm để tìm giá trị chung đầu tiên
	for key, value := range countMap {
		if value == len(keyMaps) {
			_, _, err := e.get(key)
			if err != nil {
				continue
			}
			total++
		}
	}
	return total
}
