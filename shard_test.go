package gocache

import (
	"fmt"
	"runtime"
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
