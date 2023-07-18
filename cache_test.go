package gocache

import (
	"log"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func printAlloc() {
	m := &runtime.MemStats{}
	runtime.ReadMemStats(m)
	log.Printf("Alloc: %d Mb, Sys: %d, HeapObject: %d HeapRelease: %d, HeapSys: %d", m.Alloc/1024/1024, m.Sys/1024/1024, m.HeapObjects, m.HeapReleased, m.HeapSys)
	m = nil
}

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
	engine.Set("key", []byte("halo"))
	time.Sleep(3 * time.Second)
	out, err := engine.Get("key")
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
	buf := make(chan string, 40000)

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
		engine.Set("key"+strconv.Itoa(i), []byte("value1"+strconv.Itoa(i)))
		buf <- "key" + strconv.Itoa(i)
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
	var a int64 = 0
	for i := 0; i < 300; i++ {
		go func(i int) {
			time.Sleep(time.Second)
			for {
				<-buf
				data, _ := engine.Pop()
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
				atomic.AddInt64(&a, 1)
			}
		}(i)
	}
	for i := 0; i < 1000000; i++ {
		wg.Add(1)
		engine.Set("key"+strconv.Itoa(i), []byte("value1"+strconv.Itoa(i)))
		buf <- []byte("key" + strconv.Itoa(i))
	}

	wg.Wait()
	log.Print(time.Since(now), a)
	log.Print(1111, engine.Info())
}

func TestAlloc(t *testing.T) {
	n := 1_000_00
	m := make(map[int]*[128]byte)

	for i := 0; i < n; i++ { // Adds 1 million elements
		x := [128]byte{}
		m[i] = &x
	}

	for i := 0; i < n; i++ { // Deletes 1 million elements
		delete(m, i)
	}

	runtime.GC() // Triggers a manual GC

	runtime.KeepAlive(m) // Keeps a reference to m so that the map isn’t collected

}

func TestAllocCache(t *testing.T) {
	engine, _ := New(&Config{
		Shard:       64,
		OnRemove:    nil,
		CleanWindow: time.Minute,
		TTL:         12 * time.Hour,
	})
	n := 1_000_000

	for i := 0; i < n; i++ {
		engine.Set(strconv.Itoa(i), []byte(`{
			"id": 496,
			"city": null,
			"jobTitle": "network engineer",
			"jobCategory": "engineer",
			"jobFocus": null,
			"level": null,
			"yearOfExperience": 1,
			"yearOfReceivedCompensation": "2023",
			"monthlyBaseSalary": 9,
			"annualExpectedBonus": 0,
			"signingBonus": 0,
			"bonusMemo": null,
			"otherBenefits": null,
			"createdAt": "2023-03-07T08:25:23.000Z",
			"totalCompensation": 108,
			"verified": false,
			"companyId": 245,
			"companyName": "CMC TSSG",
			"companySlug": "cmc-tssg"
		}`))
	}

	// log.Print(engine.keyhub.len())
	// log.Print(engine.Len())
	// time.Sleep(time.Second)
	now := time.Now()
	wg := &sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ { // Deletes 1 million elements
		go func() {
			defer wg.Done()
			for {
				data, err := engine.Pop()
				if err != nil && err.Error() == E_queue_is_empty {
					break
				}
				if (data == nil) && err == nil {
					panic("shit")
				}
				// log.Print("'", string(data), "'")
			}
		}()
	}
	wg.Wait()
	log.Print("--- ", time.Since(now))
	printAlloc()
	runtime.KeepAlive(engine) // Keeps a reference to m so that the map isn’t collected
}
