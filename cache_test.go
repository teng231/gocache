package gocache

import (
	"log"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNormalCase(t *testing.T) {
	engine, err := New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	// for i := 0; i < 1000000; i++ {
	// 	engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
	// }
	now := time.Now()
	// log.Print(engine.Len(), " ")
	buf := make(chan []byte, 40000)

	wg := &sync.WaitGroup{}

	for i := 0; i < 200; i++ {
		go func(i int) {
			log.Print(i)
			for {
				key := <-buf
				data, err := engine.Get(key)
				log.Print(i, " ", string(data), err)
				wg.Done()
			}
		}(i)
	}
	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
		buf <- []byte("key" + strconv.Itoa(i))
	}

	wg.Wait()
	log.Print(time.Since(now))
}

func TestPopDuplicate(t *testing.T) {
	engine, err := New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	now := time.Now()
	// log.Print(engine.Len(), " ")
	buf := make(chan []byte, 40000)

	wg := &sync.WaitGroup{}
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	for i := 0; i < 200; i++ {
		go func(i int) {
			time.Sleep(time.Second)
			for {
				<-buf
				data, err := engine.Pop()
				log.Print(i, " ", string(data), err)
				mt.RLock()
				_, has := m[string(data)]
				if has {
					panic("existed " + string(data))
				}
				mt.RUnlock()
				mt.Lock()
				m[string(data)] = true
				mt.Unlock()
				wg.Done()
			}
		}(i)
	}
	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
		buf <- []byte("key" + strconv.Itoa(i))
	}

	wg.Wait()
	log.Print(time.Since(now))
}

func TestPopHybrid(t *testing.T) {
	engine, err := New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	now := time.Now()
	// log.Print(engine.Len(), " ")
	buf := make(chan []byte, 40000)

	wg := &sync.WaitGroup{}
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	for i := 0; i < 100; i++ {
		go func(i int) {
			time.Sleep(time.Second)
			for {
				<-buf
				data, err := engine.Pop()
				log.Print(i, " ", string(data), err)
				mt.RLock()
				_, has := m[string(data)]
				if has {
					panic("existed " + string(data))
				}
				mt.RUnlock()
				mt.Lock()
				m[string(data)] = true
				mt.Unlock()
				wg.Done()
			}
		}(i)
	}
	for i := 0; i < 1000000; i++ {
		if i > 800000 {
			time.Sleep(100 * time.Nanosecond)
		}
		wg.Add(1)
		engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
		buf <- []byte("key" + strconv.Itoa(i))
	}

	wg.Wait()
	log.Print(time.Since(now))
}

func TestPopHybridWithGC(t *testing.T) {
	engine, err := New(DefaultConfig())
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	now := time.Now()
	// log.Print(engine.Len(), " ")
	buf := make(chan []byte, 40000)

	wg := &sync.WaitGroup{}
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	for i := 0; i < 100; i++ {
		go func(i int) {
			time.Sleep(time.Second)
			for {
				<-buf
				data, err := engine.Pop()
				log.Print(i, " ", string(data), err)
				mt.RLock()
				_, has := m[string(data)]
				if has {
					panic("existed " + string(data))
				}
				mt.RUnlock()
				mt.Lock()
				m[string(data)] = true
				mt.Unlock()
				wg.Done()
			}
		}(i)
	}
	for i := 0; i < 5000000; i++ {
		wg.Add(1)
		engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
		buf <- []byte("key" + strconv.Itoa(i))
	}

	wg.Wait()
	log.Print(time.Since(now))
	log.Print("run gc")
	for i := 0; i < 3; i++ {
		runtime.GC()
		debug.FreeOSMemory()
		time.Sleep(2 * time.Second)
	}
	log.Print("done gc")
	time.Sleep(10 * time.Second)
}
