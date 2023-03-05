package gocache

import (
	"bytes"
	"testing"
)

func TestHub(t *testing.T) {
	h := &hub{}

	// Test empty pop
	if h.pop() != nil {
		t.Errorf("pop() on empty hub should return nil")
	}

	// Test push and pop
	h.push([]byte("foo"), []byte("foo3s"), []byte("bar"), []byte("baz"))
	if len(h.s) != 4 {
		t.Errorf("hub length should be 3 after push, got %d", len(h.s))
	}
	if !bytes.Equal(h.pop(), []byte("foo")) {
		t.Errorf("pop() should return 'foo'")
	}
	if !bytes.Equal(h.pop(), []byte("foo3s")) {
		t.Errorf("pop() should return 'foo3s'")
	}
	if len(h.s) != 2 {
		t.Errorf("hub length should be 2 after pop, got %d", len(h.s))
	}

	// Test remove
	h.remove([]byte("bar"))
	if len(h.s) != 1 {
		t.Errorf("hub length should be 1 after remove, got %d", len(h.s))
	}
	if !bytes.Equal(h.s[0], []byte("baz")) {
		t.Errorf("remaining element should be 'baz'")
	}
}
