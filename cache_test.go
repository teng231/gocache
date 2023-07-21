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

func TestExpired2(t *testing.T) {
	engine, err := New(&Config{
		TTL:         2 * time.Second,
		Shard:       10,
		CleanWindow: 1 * time.Second,
	})

	if err != nil {
		panic(err)
	}
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
		// log.Print("done")
	}
	engine.Info()
	time.Sleep(2 * time.Second)
	engine.Info()
	// log.Print(string(out), err)
	time.Sleep(10 * time.Second)
	// log.Print(string(out), err)
}

// 1.970634083s 1_000_000
func TestRWConcurentGetSetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         5 * time.Second,
		Shard:       10,
		CleanWindow: 1 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	// for i := 0; i < 1000000; i++ {
	// 	engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
	// }
	now := time.Now()
	// log.Print(engine.Len(), " ")
	buf := make(chan string, 10000)

	wg := &sync.WaitGroup{}

	for i := 0; i < 200; i++ {
		go func(i int) {
			for {
				key := <-buf
				engine.Get(key)
				// log.Print(i, " ", string(data), err)
				wg.Done()
			}
		}(i)
	}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 1_00_000; j++ {
				wg.Add(1)
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
				buf <- "key" + strconv.Itoa(i) + "+" + strconv.Itoa(j)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	log.Print(time.Since(now))
}

// 11.292343625s 1000000
func TestPopDuplicate(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		Shard:       64,
		CleanWindow: 4 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	now := time.Now()
	// log.Print(engine.Len(), " ")
	// buf := make(chan string, 10000)

	wg := &sync.WaitGroup{}
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	var a int64 = 0

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 1_00_000; j++ {

				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
				// buf <- "key" + strconv.Itoa(i) + "+" + strconv.Itoa(j)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	engine.Info()
	// time.Sleep(2 * time.Second)
	now = time.Now()

	wg.Add(1_000_000)
	for i := 0; i < 200; i++ {
		go func(i int) {
			for {
				// <-buf
				key, data, err := engine.Pop()
				if err != nil {
					if a > 0 {
						break
					}
					continue
				}
				mt.RLock()
				_, has := m[key]
				if has {
					panic("existed " + string(data) + " " + key)
				}
				mt.RUnlock()
				mt.Lock()
				m[key] = true
				mt.Unlock()
				// log.Print(keyl, " ", a)
				engine.Info()
				wg.Done()
				atomic.AddInt64(&a, 1)
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	log.Print(time.Since(now), a)
	log.Print(1111, engine.Info())
	log.Println("len ", len(m))
}

// cache_test.go:257: --- 556.009125ms
// cache_test.go:16: Alloc: 673 Mb, Sys: 712, HeapObject: 4518831 HeapRelease: 4579328, HeapSys: 712572928
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
				_, data, err := engine.Pop()
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

// cache_test.go:290: --- 593.525709ms
// cache_test.go:314: --- 475.555625ms
// cache.go:177: 196657 0
// cache_test.go:16: Alloc: 735 Mb, Sys: 893, HeapObject: 4316154 HeapRelease: 2539520, HeapSys: 896204800
func TestAllocCacheHardcore(t *testing.T) {
	engine, _ := New(&Config{
		Shard:       64,
		OnRemove:    nil,
		CleanWindow: time.Second,
		TTL:         12 * time.Hour,
	})
	now := time.Now()
	n := 1_0_000
	wg := &sync.WaitGroup{}
	wg.Add(100)
	for j := 0; j < 100; j++ {
		go func(j int) {
			for i := 0; i < n; i++ {
				err := engine.Set(strconv.Itoa(j)+"+"+strconv.Itoa(i), []byte(`{
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
				if err != nil {
					log.Print(err)
				}
			}
			wg.Done()
		}(j)
	}
	wg.Wait()
	log.Print(engine.Len())
	log.Print("--- ", time.Since(now))

	now = time.Now()

	// time.Sleep(time.Second)
	// now := time.Now()
	// wg := &sync.WaitGroup{}
	wg.Add(100)
	for i := 0; i < 100; i++ { // Deletes 1 million elements
		go func() {
			defer wg.Done()
			for {
				_, data, err := engine.Pop()
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
	engine.Info()
	printAlloc()
	runtime.KeepAlive(engine) // Keeps a reference to m so that the map isn’t collected
}

// 1.769056708s 1_000_000
func TestRWConcurentPopGetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         10 * time.Second,
		Shard:       10,
		CleanWindow: 1 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	// for i := 0; i < 1000000; i++ {
	// 	engine.Set([]byte("key"+strconv.Itoa(i)), []byte("value1"+strconv.Itoa(i)))
	// }
	now := time.Now()
	// log.Print(engine.Len(), " ")
	// buf := make(chan string, 100000)
	var a int64 = 0

	wg := &sync.WaitGroup{}
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	for i := 0; i < 200; i++ {
		go func(i int) {
			for {
				// <-buf
				// val, err := engine.Pop()
				// if err != nil {
				// 	panic(err)
				// }
				// // log.Print(i, " ", string(val), err)

				key, data, err := engine.Pop()
				if err != nil && err.Error() == E_queue_is_empty {
					// panic(err)
					// log.Print(err)
					// continue
					break
				}
				mt.RLock()
				_, has := m[key]
				if has {
					panic("existed " + key)
				}
				mt.RUnlock()
				mt.Lock()
				m[string(data)] = true
				mt.Unlock()
				atomic.AddInt64(&a, 1)
				wg.Done()
			}
		}(i)
	}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 1_00_000; j++ {
				wg.Add(1)
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
				// buf <- "key" + strconv.Itoa(i) + "+" + strconv.Itoa(j)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	log.Print(time.Since(now), " ", a)
}

// cache_test.go:454: 2.506602792s
// func TestRWConcurentScanGetSetHashCore(t *testing.T) {
// 	engine, err := New(&Config{
// 		TTL:         10 * time.Second,
// 		Shard:       10,
// 		CleanWindow: 1 * time.Second,
// 	})
// 	if err != nil {
// 		panic(err)
// 	}
// 	log.Print("okeee")

// 	now := time.Now()
// 	// log.Print(engine.Len(), " ")
// 	buf := make(chan string, 100000)

// 	wg := &sync.WaitGroup{}
// 	for i := 0; i < 200; i++ {
// 		go func(i int) {
// 			for {
// 				item := <-buf
// 				engine.Get(item)
// 				wg.Done()
// 			}
// 		}(i)
// 	}
// 	wg.Add(10)
// 	for i := 0; i < 10; i++ {
// 		go func(i int) {
// 			for j := 0; j < 1_00_000; j++ {
// 				wg.Add(1)
// 				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)), MetaData{"id": strconv.Itoa(j), "worker": strconv.Itoa(i)})
// 				buf <- "key" + strconv.Itoa(i) + "+" + strconv.Itoa(j)
// 			}
// 			wg.Done()
// 		}(i)
// 	}
// 	for i := 0; i < 100; i++ {

// 		now2 := time.Now()
// 		// engine.Scan(func(key string, val []byte, metaData MetaData) bool {
// 		// 	if metaData["id"] == "4000" && metaData["worker"] == "8" {
// 		// 		// log.Print("founded")
// 		// 		return true
// 		// 	}
// 		// 	return false
// 		// })

// 		log.Print(time.Since(now2))
// 		time.Sleep(2 * time.Millisecond)
// 	}
// 	wg.Wait()
// 	log.Print(time.Since(now))

// }

// cache_test.go:454: 2.506602792s
func TestRWConcurentScanHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		Shard:       64,
		CleanWindow: 3 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")

	now := time.Now()
	// total := 0
	m := map[string]bool{}
	mt := &sync.RWMutex{}
	var a int64 = 0
	wg := &sync.WaitGroup{}
	// wg = &sync.WaitGroup{}
	now = time.Now()
	for i := 0; i < 20; i++ {
		go func() {
			for {
				key, _, err := engine.PopWithMetadata("worker", "3")
				// log.Print(key)
				if err != nil && err.Error() == E_not_found {
					continue
				}
				wg.Done()
				// log.Print(key)
				if err != nil {
					panic(err)
				}
				mt.Lock()
				_, has := m[key]
				if has {
					panic("existed " + key)
				}
				m[key] = true
				mt.Unlock()
				atomic.AddInt64(&a, 1)
			}
		}()
	}
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			for j := 0; j < 2_000; j++ {
				if i == 3 {
					wg.Add(1)
				}
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)),
					MetaData{"worker": strconv.Itoa(i)})
			}
			wg.Done()
		}(i)

	}

	wg.Wait()
	log.Print("-------- ", time.Since(now))
}
