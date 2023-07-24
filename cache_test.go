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

// 998.085208ms 1000_000
// 868.76675ms
func TestConcurrentSet(t *testing.T) {
	engine, err := New(&Config{
		TTL:         20 * time.Second,
		CleanWindow: 20 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	noww := time.Now()
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 1_00_000; j++ {
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
				// log.Print(strconv.Itoa(i) + "+" + strconv.Itoa(j))
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	log.Print(time.Since(noww))
	engine.Info()
}

func TestExpired(t *testing.T) {
	engine, err := New(&Config{
		TTL:         2 * time.Second,
		CleanWindow: 1 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	n := 1_000_00

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
	time.Sleep(3 * time.Second)
	engine.Info()
}

// 1.970634083s 1_000_000
// 1.5082555s
func TestRWConcurentGetSetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         20 * time.Second,
		CleanWindow: 100 * time.Second,
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
//
//	601.912042ms 1000000
func TestPopDuplicate(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		CleanWindow: 100 * time.Second,
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
	wg = &sync.WaitGroup{}
	wg.Add(1_000_000)
	for i := 0; i < 200; i++ {
		go func(i int) {
			for {
				// <-buf
				key, data, err := engine.Pop()
				if err != nil {
					log.Print("NILLL", err)
					if a > 0 {
						break
					}
					// continue
				}
				mt.RLock()
				_, has := m[key]
				if has {
					panic("existed " + string(data) + " " + key)
				}
				mt.RUnlock()
				mt.Lock()
				m[key] = true
				// log.Print(key, " ", len(m))
				mt.Unlock()
				atomic.AddInt64(&a, 1)

				// engine.Info()
				wg.Done()

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

// cache_test.go:270: --- 402.224792ms
// cache_test.go:16: Alloc: 651 Mb, Sys: 688, HeapObject: 3502564 HeapRelease: 2744320, HeapSys: 687374336
func TestAllocCache(t *testing.T) {
	engine, _ := New(&Config{
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
	engine.Info()
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

// cache_test.go:325: --- 649.720083ms
// cache_test.go:349: --- 270.669583ms
// cache.go:145: data count: 0 listkeys count: 0
// cache_test.go:16: Alloc: 720 Mb, Sys: 766, HeapObject: 3981982 HeapRelease: 1310720, HeapSys: 766902272

// cache_test.go:328: 1000000
// cache_test.go:329: --- 921.853709ms
// cache_test.go:353: --- 594.750458ms
// cache.go:171: data count: 0 listkeys count: 0
// cache_test.go:16: Alloc: 720 Mb, Sys: 770, HeapObject: 3981337 HeapRelease: 5070848, HeapSys: 771227648
func TestAllocCacheHardcore(t *testing.T) {
	engine, _ := New(&Config{
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
//
//	960.171084ms 1000000 - 1.030435584s 1000000
//
// 1.477163834s 1000000

// 1.442357375s 1000000
func TestRWConcurentPopGetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         15 * time.Second,
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
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			for j := 0; j < 50_000; j++ {
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

// 747.7435ms - w 200 p 200 t 92 ~ 4ms/req
// 439.345333ms- w 100 p200 t 92 ~ 2ms/req
// 439.345333ms- w 100 p200 t 92 ~ 2ms/req
// 5.037216333s - w 120 p 1000 t 92 ~ 5.037000 ms/req
// 3.435857s - w 120 p 500 t 92 ~ 6.870000 ms/req
// 152.986875ms - w 120 p 100 t 92 ~ 1.520000 ms/req

// 141.794791ms - w 120 p 100 t 40 ~ 1.410000 ms/req
func TestRWConcurentScanGetSetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		CleanWindow: 1 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")

	now := time.Now()
	// log.Print(engine.Len(), " ")
	wg := &sync.WaitGroup{}
	w := 120
	p := 100
	t1 := 40
	wg.Add(w)
	for i := 0; i < w; i++ {
		go func(i int) {
			for j := 0; j < p; j++ {
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)), MetaData{"worker": strconv.Itoa(i)})
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	engine.Info()
	log.Print(time.Since(now))
	now = time.Now()

	mt := &sync.RWMutex{}
	m := map[string]bool{}
	wg.Add(p)
	t2 := strconv.Itoa(t1)
	for i := 0; i < 20; i++ {
		go func() {
			for {
				key, _, err := engine.PopWithMetadata(func(metadata MetaData) bool {
					return metadata["worker"] == t2
				})
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
				log.Print(len(m))
				mt.Unlock()
			}
		}()
	}
	wg.Wait()
	log.Printf("%s - w %d p %d t %d ~ %f ms/req", time.Since(now).String(), w, p, t1, float32(time.Since(now).Milliseconds())/float32(p))

}

// 7.831917ms - w 120 p 100 t 92 ~ 0.070000 ms/req
// 5.887451166s - w 1200 p 1000 t 400 ~ 5.887000 ms/req
// 10.526208ms - w 120 p 100 t 40 ~ 0.100000 ms/req
func TestRWConcurentPopV2GetSetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		CleanWindow: 100 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")

	now := time.Now()
	// log.Print(engine.Len(), " ")
	wg := &sync.WaitGroup{}
	w := 120
	p := 100
	t1 := 40
	wg.Add(w)
	for i := 0; i < w; i++ {
		go func(i int) {
			for j := 0; j < p; j++ {
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)), MetaData{"worker": strconv.Itoa(i)})
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	engine.Info()
	log.Print(time.Since(now))
	now = time.Now()

	mt := &sync.RWMutex{}
	m := map[string]bool{}
	wg.Add(1)
	t2 := strconv.Itoa(t1)
	for i := 0; i < 20; i++ {
		go func() {
			for {
				key, _, err := engine.PopWithMetadataV2("worker:" + t2)
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
				log.Print(len(m))
				mt.Unlock()
			}
		}()
	}
	wg.Wait()
	log.Printf("%s - w %d p %d t %d ~ %f ms/req", time.Since(now).String(), w, p, t1, float32(time.Since(now).Milliseconds())/float32(p))

}
