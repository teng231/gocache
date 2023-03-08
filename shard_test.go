package gocache

import (
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestGetPosition(t *testing.T) {
	key := []byte("testkey")
	numberOfShards := 100
	start := time.Now()

	for i := 0; i < 10000; i++ {
		getPosition(key, numberOfShards)
	}

	elapsed := time.Since(start)
	fmt.Printf("TestGetPosition took %s\n", elapsed)
}
func TestWorkWithRefresh(t *testing.T) {
	shard := initShard(nil)
	go func() {
		for i := 0; i < 1_000_000; i++ {
			x := []byte(strconv.Itoa(i))
			y := []byte(strconv.Itoa(i + 1))

			shard.Upsert(strconv.Itoa(i), &Item{
				Key:   x,
				Value: y,
			}, 100*time.Second)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	shard.refresh()
	time.Sleep(3 * time.Second)
	shard.Info()
}
func TestGetPosition2(t *testing.T) {
	numberOfShards := 16
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
	}

	start := time.Now()
	for _, key := range keys {
		getPosition(key, numberOfShards)
	}
	elapsed := time.Since(start)

	t.Logf("Elapsed time: %v", elapsed)

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("Alloc: %v bytes", m.Alloc)
	t.Logf("TotalAlloc: %v bytes", m.TotalAlloc)
	t.Logf("Sys: %v bytes", m.Sys)
	t.Logf("NumGC: %v", m.NumGC)
}

func TestAllocShard(t *testing.T) {
	shard := initShard(nil)
	n := 1_000_000
	printAlloc()
	for i := 0; i < n; i++ {
		x := []byte(strconv.Itoa(i))
		y := []byte(`{
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
		}`)
		shard.Upsert(strconv.Itoa(i), &Item{
			Key:   x,
			Value: y,
		}, 100*time.Second)
	}
	shard.Info()
	printAlloc()
	for i := 0; i < n; i++ { // Deletes 1 million elements
		shard.Delete(strconv.Itoa(i))
	}
	shard.Info()
	runtime.GC() // Triggers a manual GC
	printAlloc()

	for i := 0; i < 6000; i++ {
		x := []byte(strconv.Itoa(i))
		y := []byte(strconv.Itoa(i + 1))
		shard.Upsert(strconv.Itoa(i), &Item{
			Key:   x,
			Value: y,
		}, 100*time.Second)
	}

	printAlloc()
	shard.refresh()
	runtime.GC() // Triggers a manual GC
	printAlloc()

	runtime.KeepAlive(shard) // Keeps a reference to m so that the map isn’t collected
}
