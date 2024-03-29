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

func TestPopExpired(t *testing.T) {
	engine, err := New(&Config{
		TTL:         1 * time.Second,
		CleanWindow: 2 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")

	for j := 0; j < 350; j++ {
		engine.Set("key"+strconv.Itoa(j)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
	}
	time.Sleep(1500 * time.Millisecond)
	log.Print(engine.Keys())

	for j := 350; j < 700; j++ {
		engine.Set("key"+strconv.Itoa(j)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
	}
	time.Sleep(2900 * time.Millisecond)
	log.Print("-----------------")
	// engine.CleanWindow(nil)

	// for i := 0; i < 100; i++ {
	// 	key, data, err := engine.Pop()
	// 	log.Print(key, err, string(data))
	// 	engine.Info()
	// }
	log.Print(engine.Keys())
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
			for j := 0; j < 1_000; j++ {
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
			}
			wg.Done()
		}(i)
		for j := 0; j < 1_000; j++ {
			engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value1"+strconv.Itoa(j)))
		}
	}
	wg.Wait()
	log.Print(engine.Info())
	// time.Sleep(2 * time.Second)
	now = time.Now()
	wg = &sync.WaitGroup{}
	wg.Add(20_000)
	for i := 0; i < 200; i++ {
		go func(i int) {
			for {
				// <-buf
				key, data, err := engine.Pop()
				if err != nil {
					log.Print("NILLL", err)
					// if a > 0 {
					// 	break
					// }
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
		TTL:         2 * time.Second,
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

func TestPopExpired2(t *testing.T) {
	engine, err := New(&Config{
		TTL:         10 * time.Second,
		CleanWindow: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	item := map[string]bool{}
	mt := &sync.Mutex{}
	for i := 0; i < 4; i++ {
		go func() {
			for {
				key, _, err := engine.Pop()
				if err != nil {
					log.Print(err)
					time.Sleep(400 * time.Millisecond)
					continue
				}
				mt.Lock()
				if item[key] {
					log.Fatal("duplicated ", key)
				}
				log.Print("pop ", key)
				item[key] = true
				mt.Unlock()
				time.Sleep(200 * time.Millisecond)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	log.Print("Thêm vào 50 phần tử")
	for j := 0; j < 50; j++ {
		key := "key" + strconv.Itoa(j)
		if item[key] {
			continue
		}
		engine.Set(key, []byte("value1"+strconv.Itoa(j)))
	}
	log.Print("Thêm done 50 phần tử")
	time.Sleep(1 * time.Second)
	log.Print("Thêm vào 50 phần tử 2")
	for j := 10; j < 60; j++ {
		key := "key" + strconv.Itoa(j)
		if item[key] {
			continue
		}
		engine.Set(key, []byte("value1"+strconv.Itoa(j)))
	}
	log.Print("Thêm done 50 phần tử 2")

	log.Print(engine.Keys())
	time.Sleep(20 * time.Second)
}

func TestTime(t *testing.T) {
	log.Print(time.Second.Seconds())
	if time.Second != 1000*time.Millisecond {
		panic("shit")
	}
}

func TestPopLen(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		CleanWindow: 100 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	// log.Print(engine.Len(), " ")
	// buf := make(chan string, 10000)

	wg := &sync.WaitGroup{}

	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 20; j++ {

				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte("value"+strconv.Itoa(j)), MetaData{
					"worker": strconv.Itoa(i),
				})
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	log.Print(engine.Info())
	len := engine.LenWithMetadata("worker:4")
	log.Print("len ", len)
	// mt := &sync.Mutex{}
	// m := map[string]bool{}

	for i := 0; i < 3; i++ {
		key, val, err := engine.PopWithMetadataV2("worker:4")
		if err != nil {
			log.Print("NILLL", err)
			continue
		}
		log.Print(key, "---", string(val))
	}

	// wg.Add(10)
	// for i := 0; i < 10; i++ {
	// 	go func() {
	// 		for j := 0; j < 30; j++ {
	// 			key, val, err := engine.PopWithMetadataV2("worker:4")
	// 			if err != nil {
	// 				log.Print("NILLL", err)
	// 				continue
	// 			}
	// 			log.Print(key, "---", string(val))
	// 			// mt.Lock()
	// 			// _, has := m[key]
	// 			// if has {
	// 			// 	panic("existed " + string(data) + " " + key)
	// 			// }
	// 			// m[key] = true
	// 			// mt.Unlock()
	// 		}
	// 		wg.Done()
	// 	}()
	// }
	// wg.Wait()
	len = engine.LenWithMetadata("worker:4")
	log.Print("len ", len)
}

func TestPopExpire(t *testing.T) {
	engine, err := New(&Config{
		TTL:         2 * time.Second,
		CleanWindow: 1 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	log.Print("okeee")
	for i := 0; i < 2; i++ {
		for j := 0; j < 20; j++ {
			engine.Set("key"+strconv.Itoa(j), []byte("value"+strconv.Itoa(j)), MetaData{
				"worker": "x",
			})
		}
		for j := 0; j < 20; j++ {
			key, val, err := engine.PopWithMetadataV2("worker:x")
			if err != nil {
				log.Print("NILLL", err)
				continue
			}
			log.Print(key, "---", string(val))
		}
		time.Sleep(2 * time.Second)
	}

}
