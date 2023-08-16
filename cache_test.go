package gocache

import (
	"log"
	"runtime"
	"strconv"
	"sync"
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
// new: 3.108083ms - w 120 p 100 t 40 ~ 0.030000 ms/req
func TestRWConcurentPopV2GetSetHashCore(t *testing.T) {
	engine, err := New(&Config{
		TTL:         100 * time.Second,
		CleanWindow: 2 * time.Second,
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
	wg.Add(100)
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
	log.Print(len(m))
	log.Printf("%s - w %d p %d t %d ~ %f ms/req", time.Since(now).String(), w, p, t1, float32(time.Since(now).Milliseconds())/float32(p))

}

func TestGetSet(t *testing.T) {
	engine, err := New(&Config{
		TTL:         1 * time.Second,
		CleanWindow: 1 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	noww := time.Now()
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 10_000; j++ {
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte(`{
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
			wg.Done()
		}(i)
	}

	wg.Wait()
	log.Print(time.Since(noww))
	engine.Info()
	printAlloc()
	runtime.KeepAlive(engine) // Keeps a reference to m so that the map isn’t collected
	time.Sleep(3 * time.Second)
	printAlloc()
}

func TestSimplePopWithMetadata(t *testing.T) {
	engine, err := New(&Config{
		TTL:         1 * time.Second,
		CleanWindow: 1 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	wg := &sync.WaitGroup{}
	noww := time.Now()
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			for j := 0; j < 1000; j++ {
				engine.Set("key"+strconv.Itoa(i)+"+"+strconv.Itoa(j), []byte(`{
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
				}`), MetaData{"worker": strconv.Itoa(j)})
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	log.Print(time.Since(noww))
	engine.Info()
	for i := 0; i < 10; i++ {
		key, _, err := engine.PopWithMetadataV2("worker:3")
		if err != nil {
			panic(err)
		}
		log.Print(key, err)
	}
	for i := 0; i < 10; i++ {
		key, _, err := engine.PopWithMetadataV2("worker:3")
		if err != nil {
			panic(err)
		}
		log.Print(key, err)
	}

}
