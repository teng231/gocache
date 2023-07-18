package gocache

import (
	"runtime"
	"strconv"
	"testing"
)

func TestHub(t *testing.T) {
	h := &hub{}

	// Test empty pop
	if h.pop() != "" {
		t.Errorf("pop() on empty hub should return nil")
	}

	// Test push and pop
	h.push("foo", "foo3s", "bar", "baz")
	if len(h.s) != 4 {
		t.Errorf("hub length should be 3 after push, got %d", len(h.s))
	}
	if h.pop() == "foo" {
		t.Errorf("pop() should return 'foo'")
	}
	if h.pop() == "foo3s" {
		t.Errorf("pop() should return 'foo3s'")
	}
	if len(h.s) != 2 {
		t.Errorf("hub length should be 2 after pop, got %d", len(h.s))
	}

	// Test remove
	h.remove("bar")
	if len(h.s) != 1 {
		t.Errorf("hub length should be 1 after remove, got %d", len(h.s))
	}
	if *(h.s[0]) != "baz" {
		t.Errorf("remaining element should be 'baz'")
	}
}

func TestAllocKeyhub(t *testing.T) {
	h := &hub{s: make([]*string, 0)}
	n := 100_0000

	for i := 0; i < n; i++ {
		x := strconv.Itoa(i) + "data"
		h.push(x)
	}

	// log.Print(h.len())
	// wg := &sync.WaitGroup{}
	// wg.Add(10)
	// for i := 0; i < 10; i++ { // Deletes 1 million elements
	// 	go func() {
	// 		defer wg.Done()
	// 		for {
	// 			out := h.pop()
	// 			// log.Print(string(out))
	// 			if out == nil {
	// 				break
	// 			}
	// 		}
	// 	}()
	// }
	// wg.Wait()

	for i := 0; i < n; i++ { // Deletes 1 million elements
		h.pop()
		// out := h.pop()
		// log.Print(string(out))

		// x := strconv.Itoa(i) + "cau chuyen tinh yeu, 10000000")
		// h.remove(x)
	}

	runtime.GC() // Triggers a manual GC

	runtime.KeepAlive(h) // Keeps a reference to m so that the map isn’t collected
}
