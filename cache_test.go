package gocache

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestExpired(t *testing.T) {
	engine, err := New(&Config{
		TTL:         2 * time.Second,
		Shard:       10,
		CleanWindow: 1 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	engine.Set([]byte("key"), []byte("halo"))
	time.Sleep(3 * time.Second)
	out, err := engine.Get([]byte("key"))
	log.Print(string(out), err)
}
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
	buf := make(chan []byte, 1000000)

	wg := &sync.WaitGroup{}
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	for i := 0; i < 300; i++ {
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
func printAlloc() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("%d Mb\n", m.Alloc/1024/1024)
}
func TestAlloc(t *testing.T) {
	n := 1_000_000
	m := make(map[int]*[128]byte)
	printAlloc()

	for i := 0; i < n; i++ { // Adds 1 million elements
		x := [128]byte{}
		m[i] = &x
	}
	printAlloc()

	for i := 0; i < n; i++ { // Deletes 1 million elements
		delete(m, i)
	}

	runtime.GC() // Triggers a manual GC
	printAlloc()
	runtime.KeepAlive(m) // Keeps a reference to m so that the map isn’t collected

}
